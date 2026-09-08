from unittest.mock import patch, MagicMock
from google.transit import gtfs_realtime_pb2
from producer import decode_feed, fetch_raw_feed


def build_fake_feed_bytes():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "1"
    entity.vehicle.vehicle.id = "bus-456"
    entity.vehicle.trip.route_id = "333"
    return feed.SerializeToString()


def test_decode_feed_still_works():
    raw = build_fake_feed_bytes()
    decoded = decode_feed(raw)
    assert decoded.entity[0].vehicle.vehicle.id == "bus-456"


@patch("producer.requests.get")
def test_fetch_raw_feed_calls_correct_url(mock_get):
    mock_response = MagicMock()
    mock_response.content = b"fake-bytes"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    result = fetch_raw_feed()

    assert result == b"fake-bytes"
    called_url = mock_get.call_args[0][0]
    assert "vehiclepos/buses" in called_url