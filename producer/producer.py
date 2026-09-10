import os
import time
import logging
import base64

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_KEY = os.environ["TFNSW_API_KEY"]
VEHICLE_POSITIONS_URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"
TRIP_UPDATES_URL = "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses"
VEHICLE_POSITIONS_TOPIC = "vehicle-positions-raw"
TRIP_UPDATES_TOPIC = "trip-updates-raw"
BROKER = os.environ.get("KAFKA_BROKER", "localhost:19092")
POLL_INTERVAL_SECONDS = 15


def fetch_raw_feed(url: str) -> bytes:
    response = requests.get(url, headers={"Authorization": f"apikey {API_KEY}"}, timeout=10)
    response.raise_for_status()
    return response.content


def decode_feed(raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)
    return feed


def publish_feed(producer: KafkaProducer, url: str, topic: str) -> int:
    raw = fetch_raw_feed(url)
    feed = decode_feed(raw)
    for entity in feed.entity:
        encoded = base64.b64encode(entity.SerializeToString())
        producer.send(topic, encoded)
    producer.flush()
    return len(feed.entity)


def run():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: v,
    )

    while True:
        try:
            position_count = publish_feed(producer, VEHICLE_POSITIONS_URL, VEHICLE_POSITIONS_TOPIC)
            logger.info("Published %d vehicle position updates", position_count)
        except requests.RequestException as e:
            logger.warning("Vehicle positions API request failed: %s", e)
        except Exception as e:
            logger.error("Unexpected error (vehicle positions): %s", e)

        try:
            trip_update_count = publish_feed(producer, TRIP_UPDATES_URL, TRIP_UPDATES_TOPIC)
            logger.info("Published %d trip updates", trip_update_count)
        except requests.RequestException as e:
            logger.warning("Trip updates API request failed: %s", e)
        except Exception as e:
            logger.error("Unexpected error (trip updates): %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()