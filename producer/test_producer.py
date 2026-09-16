from unittest.mock import MagicMock, patch

from google.transit import gtfs_realtime_pb2

from producer import decode_feed, fetch_raw_feed, publish_feed


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

    result = fetch_raw_feed("https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses")

    assert result == b"fake-bytes"
    called_url = mock_get.call_args[0][0]
    assert "vehiclepos/buses" in called_url


@patch("producer.fetch_raw_feed")
def test_publish_feed_sends_one_message_per_entity(mock_fetch_raw_feed):
    mock_fetch_raw_feed.return_value = build_fake_feed_bytes()
    mock_producer = MagicMock()

    count = publish_feed(mock_producer, "https://fake-url", "some-topic")

    assert count == 1
    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args[0]
    assert call_args[0] == "some-topic"
    # the message body should be base64 text, not raw protobuf bytes
    import base64
    base64.b64decode(call_args[1])  # raises if not valid base64
    mock_producer.flush.assert_called_once()