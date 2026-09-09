import os
import base64
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from google.transit import gtfs_realtime_pb2

JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink-sql-connector-kafka-3.1.0-1.18.jar")


def decode_entity(raw_str: str) -> dict:
    raw_bytes = base64.b64decode(raw_str)
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(raw_bytes)
    vehicle = entity.vehicle
    return {
        "vehicle_id": vehicle.vehicle.id,
        "route_id": vehicle.trip.route_id,
        "trip_id": vehicle.trip.trip_id,
        "latitude": vehicle.position.latitude,
        "longitude": vehicle.position.longitude,
        "timestamp": vehicle.timestamp,
    }


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file:///{JAR_PATH}")

    kafka_consumer = FlinkKafkaConsumer(
        topics="vehicle-positions-raw",
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": "localhost:19092",
            "group.id": "flink-decode-group",
        },
    )
    kafka_consumer.set_start_from_latest()

    stream = env.add_source(kafka_consumer)
    decoded = stream.map(decode_entity)
    decoded.print()

    env.execute("decode_vehicle_positions_job")


if __name__ == "__main__":
    run()