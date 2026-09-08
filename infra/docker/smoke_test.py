from kafka import KafkaProducer, KafkaConsumer

TOPIC = "vehicle-positions-raw"
BROKER = "localhost:19092"


def test_produce_and_consume_message():
    producer = KafkaProducer(bootstrap_servers=BROKER)
    producer.send(TOPIC, b"smoke-test-message")
    producer.flush()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )

    received = [msg.value for msg in consumer]
    assert b"smoke-test-message" in received