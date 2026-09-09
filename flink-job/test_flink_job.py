import base64
import pytest
from google.transit import gtfs_realtime_pb2
from flink_job import decode_entity


def test_decode_entity_parses_base64_encoded_vehicle():
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = "1"
    entity.vehicle.vehicle.id = "bus-789"
    entity.vehicle.trip.route_id = "333"
    entity.vehicle.trip.trip_id = "trip-1"
    entity.vehicle.position.latitude = -33.87
    entity.vehicle.position.longitude = 151.21
    entity.vehicle.timestamp = 1700000000

    encoded = base64.b64encode(entity.SerializeToString()).decode("ascii")
    result = decode_entity(encoded)

    assert result["vehicle_id"] == "bus-789"
    assert result["route_id"] == "333"
    assert result["latitude"] == pytest.approx(-33.87, abs=1e-4)
    assert result["longitude"] == pytest.approx(151.21, abs=1e-4)