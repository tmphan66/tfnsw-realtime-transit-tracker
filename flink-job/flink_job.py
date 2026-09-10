import os
import math
import base64
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedCoProcessFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types
from google.transit import gtfs_realtime_pb2

JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink-sql-connector-kafka-3.1.0-1.18.jar")

# Two vehicles on the same route within this distance and time window are flagged as "bunching"
BUNCHING_DISTANCE_METERS = 500
BUNCHING_TIME_WINDOW_SECONDS = 120


def decode_vehicle_entity(raw_str: str) -> dict:
    raw_bytes = base64.b64decode(raw_str)
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(raw_bytes)

    if not entity.HasField("vehicle"):
        return None

    vehicle = entity.vehicle
    trip_id = vehicle.trip.trip_id
    if not trip_id:
        return None

    return {
        "vehicle_id": vehicle.vehicle.id,
        "route_id": vehicle.trip.route_id,
        "trip_id": trip_id,
        "latitude": vehicle.position.latitude,
        "longitude": vehicle.position.longitude,
        "timestamp": vehicle.timestamp,
    }


def decode_trip_update_entity(raw_str: str):
    raw_bytes = base64.b64decode(raw_str)
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(raw_bytes)

    if not entity.HasField("trip_update"):
        return None

    tu = entity.trip_update
    trip_id = tu.trip.trip_id
    if not trip_id:
        return None

    # Only look at the next stop's update (index 0)
    delay_seconds = None
    if len(tu.stop_time_update) > 0:
        stu = tu.stop_time_update[0]
        if stu.HasField("arrival") and stu.arrival.HasField("delay"):
            delay_seconds = stu.arrival.delay
        elif stu.HasField("departure") and stu.departure.HasField("delay"):
            delay_seconds = stu.departure.delay

    if delay_seconds is None:
        return None

    return {"trip_id": trip_id, "delay_seconds": delay_seconds}


def haversine_distance_meters(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (given their latitude/longitude in decimal degrees)
    """
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (math.sin(delta_phi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


class AttachDelayFunction(KeyedCoProcessFunction):
    """
    Joins the vehicle-positions stream with the trip-updates stream,
    keyed by trip_id. Keeps the last known delay per trip in state and
    attaches it to every vehicle position for that trip.
    """
    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("last_known_delay", Types.LONG())
        self.delay_state = runtime_context.get_state(descriptor)

    def process_element1(self, value, ctx):
        # value = vehicle position dict
        delay = self.delay_state.value()
        result = dict(value)
        result["delay_seconds"] = delay if delay is not None else 0
        yield result

    def process_element2(self, value, ctx):
        # value = trip update dict; updates state, emits nothing
        self.delay_state.update(value["delay_seconds"])
        return
        yield  # unreachable, makes this a generator function


class BunchingDetectionFunction(KeyedProcessFunction):
    """
    Keyed by route_id. Tracks the last known position of every active
    vehicle on the route, and flags a vehicle as bunching if another
    vehicle on the same route was recently seen nearby.
    """
    def open(self, runtime_context: RuntimeContext):
        descriptor = MapStateDescriptor(
            "vehicle_last_position", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
        )
        self.vehicle_positions = runtime_context.get_map_state(descriptor)

    def process_element(self, value, ctx):
        vehicle_id = value["vehicle_id"]
        lat = value["latitude"]
        lon = value["longitude"]
        ts = value["timestamp"]

        is_bunching = False
        for other_vehicle_id in list(self.vehicle_positions.keys()):
            if other_vehicle_id == vehicle_id:
                continue
            other_lat, other_lon, other_ts = self.vehicle_positions.get(other_vehicle_id)
            if abs(ts - other_ts) > BUNCHING_TIME_WINDOW_SECONDS:
                continue
            distance = haversine_distance_meters(lat, lon, other_lat, other_lon)
            if distance <= BUNCHING_DISTANCE_METERS:
                is_bunching = True
                break

        self.vehicle_positions.put(vehicle_id, (lat, lon, ts))

        result = dict(value)
        result["is_bunching"] = is_bunching
        yield result


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file:///{JAR_PATH}")

    positions_consumer = FlinkKafkaConsumer(
        topics="vehicle-positions-raw",
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": "localhost:19092", "group.id": "flink-positions-group"},
    )
    positions_consumer.set_start_from_latest()

    trip_updates_consumer = FlinkKafkaConsumer(
        topics="trip-updates-raw",
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": "localhost:19092", "group.id": "flink-trip-updates-group"},
    )
    trip_updates_consumer.set_start_from_latest()

    positions_stream = (
        env.add_source(positions_consumer)
        .map(decode_vehicle_entity)
        .filter(lambda v: v is not None)
    )

    trip_updates_stream = (
        env.add_source(trip_updates_consumer)
        .map(decode_trip_update_entity)
        .filter(lambda v: v is not None)
    )

    keyed_positions = positions_stream.key_by(lambda v: v["trip_id"], key_type=Types.STRING())
    keyed_trip_updates = trip_updates_stream.key_by(lambda v: v["trip_id"], key_type=Types.STRING())

    enriched = keyed_positions.connect(keyed_trip_updates).process(AttachDelayFunction())

    keyed_by_route = enriched.key_by(lambda v: v["route_id"], key_type=Types.STRING())
    with_bunching = keyed_by_route.process(BunchingDetectionFunction())
    with_bunching.print()

    env.execute("attach_delay_job")


if __name__ == "__main__":
    run()