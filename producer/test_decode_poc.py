from google.transit import gtfs_realtime_pb2
from decode_poc import decode_feed


def test_decode_feed_parses_vehicle_position():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "1"
    entity.vehicle.vehicle.id = "bus-123"
    entity.vehicle.trip.route_id = "333"
    entity.vehicle.position.latitude = -33.87
    entity.vehicle.position.longitude = 151.21

    raw_bytes = feed.SerializeToString()
    decoded = decode_feed(raw_bytes)

    assert decoded.entity[0].vehicle.vehicle.id == "bus-123"
    assert decoded.entity[0].vehicle.trip.route_id == "333"