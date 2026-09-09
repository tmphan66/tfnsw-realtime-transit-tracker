import os
from pyflink.common import Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink-sql-connector-kafka-3.1.0-1.18.jar")


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file:///{JAR_PATH}")

    kafka_consumer = FlinkKafkaConsumer(
        topics="vehicle-positions-raw",
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": "localhost:19092",
            "group.id": "flink-poc-group",
        },
    )
    kafka_consumer.set_start_from_earliest()

    stream = env.add_source(kafka_consumer)
    stream.print()

    env.execute("minimal_kafka_consume_job")


if __name__ == "__main__":
    run()