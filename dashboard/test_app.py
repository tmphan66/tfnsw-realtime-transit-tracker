import pandas as pd
import pytest
from app import (
    filter_valid_coordinates,
    add_operator_name,
    add_delay_color,
    truncate_label,
    compute_view_state,
    build_recent_silver_paths,
)


def test_filter_valid_coordinates_removes_null_island():
    df = pd.DataFrame({
        "latitude": [0.0, -33.87, 0.0],
        "longitude": [0.0, 151.21, 0.0],
        "vehicle_id": ["a", "b", "c"],
    })
    result = filter_valid_coordinates(df)
    assert list(result["vehicle_id"]) == ["b"]


def test_add_operator_name_resolves_known_agency():
    df = pd.DataFrame({"agency_id": ["2503"]})
    lookup = {"2503": "Transit Systems NSW"}
    result = add_operator_name(df, lookup)
    assert result["operator_name"].iloc[0] == "Transit Systems NSW"


def test_add_operator_name_falls_back_for_unknown_agency():
    df = pd.DataFrame({"agency_id": ["999999"]})
    lookup = {"2503": "Transit Systems NSW"}
    result = add_operator_name(df, lookup)
    assert result["operator_name"].iloc[0] == "Unknown operator (999999)"


def test_add_delay_color_bunching_is_red():
    df = pd.DataFrame({"is_bunching": [True], "delay_seconds": [0]})
    result = add_delay_color(df)
    assert result["color"].iloc[0] == [255, 0, 0, 200]


def test_add_delay_color_significant_delay_is_amber():
    df = pd.DataFrame({"is_bunching": [False], "delay_seconds": [400]})
    result = add_delay_color(df)
    assert result["color"].iloc[0] == [255, 165, 0, 200]


def test_add_delay_color_on_time_is_green():
    df = pd.DataFrame({"is_bunching": [False], "delay_seconds": [30]})
    result = add_delay_color(df)
    assert result["color"].iloc[0] == [0, 200, 0, 200]


def test_truncate_label_leaves_short_names_unchanged():
    assert truncate_label("Busways R1") == "Busways R1"


def test_truncate_label_truncates_long_names():
    result = truncate_label("Keolis Downer Northern Beaches", max_len=18)
    assert len(result) == 18
    assert result.endswith("…")


def test_compute_view_state_empty_df_returns_default():
    view = compute_view_state(pd.DataFrame())
    assert view.latitude == pytest.approx(-33.87)
    assert view.longitude == pytest.approx(151.21)


def test_compute_view_state_centers_on_data():
    df = pd.DataFrame({"latitude": [-33.0, -34.0], "longitude": [151.0, 152.0]})
    view = compute_view_state(df)
    assert view.latitude == pytest.approx(-33.5)
    assert view.longitude == pytest.approx(151.5)


def test_build_recent_silver_paths_returns_correct_count():
    paths = build_recent_silver_paths("my-bucket", days_back=3)
    assert len(paths) == 3
    for p in paths:
        assert p.startswith("s3://my-bucket/silver/vehicle-events/year=")
        assert p.endswith("/*.parquet")