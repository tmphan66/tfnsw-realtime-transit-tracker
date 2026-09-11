import os
import math
import base64
import boto3
from decimal import Decimal

import io
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedCoProcessFunction, KeyedProcessFunction, RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types
from google.transit import gtfs_realtime_pb2

# Set up
load_dotenv()

JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink-sql-connector-kafka-3.1.0-1.18.jar")

# Two vehicles on the same route within this distance and time window are flagged as "bunching"
BUNCHING_DISTANCE_METERS = 500
BUNCHING_TIME_WINDOW_SECONDS = 120

# Maximum difference in compass bearing (0-360) between two vehicles to be considered "bunching"
BUNCHING_MAX_BEARING_DIFF_DEGREES = 45 

# DynamoDB table for storing last known vehicle state
DYNAMODB_TABLE_NAME = "transit-tracker-vehicle-state"
AWS_REGION = "ap-southeast-2"
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

def split_route_id(route_id: str) -> tuple:
    parts = route_id.split("_", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return route_id, route_id

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

    route_id = vehicle.trip.route_id
    agency_id, route_short_name = split_route_id(route_id)

    return {
        "vehicle_id": vehicle.vehicle.id,
        "route_id": route_id,
        "agency_id": agency_id,
        "route_short_name": route_short_name,
        "trip_id": trip_id,
        "latitude": vehicle.position.latitude,
        "longitude": vehicle.position.longitude,
        "bearing": vehicle.position.bearing,
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


def bearing_difference_degrees(bearing1, bearing2):
    """
    Smallest angle between two compass bearings (0-360), accounting for wraparound 
    """
    diff = abs(bearing1 - bearing2) % 360
    return min(diff, 360 - diff)


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
        bearing = value["bearing"]
        ts = value["timestamp"]

        is_bunching = False
        for other_vehicle_id in list(self.vehicle_positions.keys()):
            if other_vehicle_id == vehicle_id:
                continue
            other_lat, other_lon, other_bearing, other_ts = self.vehicle_positions.get(other_vehicle_id)
            if abs(ts - other_ts) > BUNCHING_TIME_WINDOW_SECONDS:
                continue
            distance = haversine_distance_meters(lat, lon, other_lat, other_lon)
            if distance > BUNCHING_DISTANCE_METERS:
                continue
            if bearing_difference_degrees(bearing, other_bearing) > BUNCHING_MAX_BEARING_DIFF_DEGREES:
                continue
            is_bunching = True
            break

        self.vehicle_positions.put(vehicle_id, (lat, lon, bearing, ts))

        result = dict(value)
        result["is_bunching"] = is_bunching
        yield result


class DynamoDBSinkFunction(MapFunction):
    """
    Writes each enriched vehicle record to DynamoDB table.
    """

    def open(self, runtime_context: RuntimeContext):
        self.table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_TABLE_NAME)

    def map(self, value):
        self.table.put_item(Item={
            "vehicle_id": value["vehicle_id"],
            "route_id": value["route_id"],
            "agency_id": value["agency_id"],
            "route_short_name": value["route_short_name"],
            "trip_id": value["trip_id"],
            "latitude": Decimal(str(value["latitude"])),
            "longitude": Decimal(str(value["longitude"])),
            "timestamp": value["timestamp"],
            "delay_seconds": value["delay_seconds"],
            "is_bunching": value["is_bunching"],
        })
        return value



class S3ParquetSinkFunction(KeyedProcessFunction):
    """
    Buffers enriched vehicle records and flushes them to S3 as a
    Parquet file roughly once a minute.
    """

    FLUSH_INTERVAL_MS = 60000

    def open(self, runtime_context: RuntimeContext):
        self.buffer = []
        self.s3_client = boto3.client("s3", region_name=AWS_REGION)

    def process_element(self, value, ctx):
        self.buffer.append(value)
        if len(self.buffer) == 1:
            ctx.timer_service().register_processing_time_timer(
                ctx.timer_service().current_processing_time() + self.FLUSH_INTERVAL_MS
            )
        yield value

    def on_timer(self, timestamp, ctx):
        if self.buffer:
            self._flush_to_s3()
        return
        yield  # unreachable, makes this a generator function

    def _flush_to_s3(self):
        table = pa.Table.from_pylist(self.buffer)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        now = datetime.now(timezone.utc)
        key = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/part-{now.strftime('%H%M%S')}.parquet"
        self.s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=buf.getvalue())

        self.buffer = []


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

    # Key the two streams by trip_id and connect them to attach delay information to vehicle positions
    keyed_positions = positions_stream.key_by(lambda v: v["trip_id"], key_type=Types.STRING())
    keyed_trip_updates = trip_updates_stream.key_by(lambda v: v["trip_id"], key_type=Types.STRING())

    enriched = keyed_positions.connect(keyed_trip_updates).process(AttachDelayFunction())

    # Key the enriched stream by route_id and detect bunching
    keyed_by_route = enriched.key_by(lambda v: v["route_id"], key_type=Types.STRING())
    with_bunching = keyed_by_route.process(BunchingDetectionFunction())

    # Sink 1: live state, read by the dashboard
    with_bunching.map(DynamoDBSinkFunction()).print()

    # Sink 2: historical batches for KPI/trend analysis
    with_bunching.key_by(lambda v: "all", key_type=Types.STRING()) \
        .process(S3ParquetSinkFunction()).print()

    env.execute("attach_delay_job")


if __name__ == "__main__":
    run()