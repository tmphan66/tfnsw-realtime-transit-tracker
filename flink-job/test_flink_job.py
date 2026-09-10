import base64
import pytest
from google.transit import gtfs_realtime_pb2
from flink_job import decode_vehicle_entity, decode_trip_update_entity


def test_decode_vehicle_entity_parses_base64_encoded_vehicle():
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = "1"
    entity.vehicle.vehicle.id = "bus-789"
    entity.vehicle.trip.route_id = "333"
    entity.vehicle.trip.trip_id = "trip-1"
    entity.vehicle.position.latitude = -33.87
    entity.vehicle.position.longitude = 151.21
    entity.vehicle.timestamp = 1700000000

    encoded = base64.b64encode(entity.SerializeToString()).decode("ascii")
    result = decode_vehicle_entity(encoded)


    assert result["vehicle_id"] == "bus-789"
    assert result["route_id"] == "333"
    assert result["latitude"] == pytest.approx(-33.87, abs=1e-4)
    assert result["longitude"] == pytest.approx(151.21, abs=1e-4)

def test_decode_trip_update_extracts_delay():
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = "1"
    entity.trip_update.trip.trip_id = "trip-42"
    stop_time_update = entity.trip_update.stop_time_update.add()
    stop_time_update.arrival.delay = 180

    encoded = base64.b64encode(entity.SerializeToString()).decode("ascii")
    result = decode_trip_update_entity(encoded)

    assert result["trip_id"] == "trip-42"
    assert result["delay_seconds"] == 180