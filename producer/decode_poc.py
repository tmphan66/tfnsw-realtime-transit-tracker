import os
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

load_dotenv()

API_KEY = os.environ["TFNSW_API_KEY"]
URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"


def fetch_raw_feed() -> bytes:
    response = requests.get(URL, headers={"Authorization": f"apikey {API_KEY}"})
    response.raise_for_status()
    return response.content


def decode_feed(raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)
    return feed


if __name__ == "__main__":
    raw = fetch_raw_feed()
    feed = decode_feed(raw)
    for entity in feed.entity[:5]:
        v = entity.vehicle
        print(v.vehicle.id, v.trip.route_id, v.position.latitude, v.position.longitude)