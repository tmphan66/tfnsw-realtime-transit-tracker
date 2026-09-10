import base64
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock
from google.transit import gtfs_realtime_pb2
from flink_job import decode_vehicle_entity, decode_trip_update_entity, haversine_distance_meters
from flink_job import DynamoDBSinkFunction
from flink_job import S3ParquetSinkFunction, S3_BUCKET_NAME


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

def test_haversine_distance_meters_same_point_is_zero():
    assert haversine_distance_meters(-33.87, 151.21, -33.87, 151.21) == pytest.approx(0, abs=1e-6)


def test_haversine_distance_meters_known_distance():
    # roughly 111km per degree of latitude
    distance = haversine_distance_meters(-33.0, 151.0, -34.0, 151.0)
    assert distance == pytest.approx(111000, rel=0.02)

@patch("flink_job.boto3.resource")
def test_dynamodb_sink_writes_expected_item(mock_boto3_resource):
    mock_table = MagicMock()
    mock_boto3_resource.return_value.Table.return_value = mock_table

    sink = DynamoDBSinkFunction()
    sink.open(None)

    record = {
        "vehicle_id": "bus-1",
        "route_id": "333",
        "trip_id": "trip-1",
        "latitude": -33.87,
        "longitude": 151.21,
        "timestamp": 1700000000,
        "delay_seconds": 90,
        "is_bunching": False,
    }
    result = sink.map(record)

    assert result == record
    mock_table.put_item.assert_called_once()
    written_item = mock_table.put_item.call_args[1]["Item"]
    assert written_item["vehicle_id"] == "bus-1"
    assert written_item["latitude"] == Decimal("-33.87")

from flink_job import S3ParquetSinkFunction, S3_BUCKET_NAME

@patch("flink_job.boto3.client")
def test_s3_parquet_sink_flush_writes_expected_data(mock_boto3_client):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    sink = S3ParquetSinkFunction()
    sink.open(None)
    sink.buffer = [{
        "vehicle_id": "bus-1", "route_id": "333", "trip_id": "t1",
        "latitude": -33.87, "longitude": 151.21, "timestamp": 1700000000,
        "delay_seconds": 10, "is_bunching": False,
    }]

    sink._flush_to_s3()

    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == S3_BUCKET_NAME
    assert call_kwargs["Key"].endswith(".parquet")
    assert sink.buffer == []