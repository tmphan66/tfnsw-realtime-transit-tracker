import os
import base64
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from google.transit import gtfs_realtime_pb2

JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink-sql-connector-kafka-3.1.0-1.18.jar")


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

class AttachDelayFunction(KeyedCoProcessFunction):
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
    enriched.print()

    env.execute("attach_delay_job")


if __name__ == "__main__":
    run()