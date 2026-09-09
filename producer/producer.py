import os
import time
import logging

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

import base64

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_KEY = os.environ["TFNSW_API_KEY"]
URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"
TOPIC = "vehicle-positions-raw"
BROKER = os.environ.get("KAFKA_BROKER", "localhost:19092")
POLL_INTERVAL_SECONDS = 15


def fetch_raw_feed() -> bytes:
    response = requests.get(URL, headers={"Authorization": f"apikey {API_KEY}"}, timeout=10)
    response.raise_for_status()
    return response.content


def decode_feed(raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)
    return feed


def run():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: v,
    )

    while True:
        try:
            raw = fetch_raw_feed()
            feed = decode_feed(raw)

            for entity in feed.entity:
                encoded = base64.b64encode(entity.SerializeToString())
                producer.send(TOPIC, encoded)

            producer.flush()
            logger.info("Published %d vehicle updates", len(feed.entity))

        except requests.RequestException as e:
            logger.warning("API request failed: %s", e)
        except Exception as e:
            logger.error("Unexpected error: %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()