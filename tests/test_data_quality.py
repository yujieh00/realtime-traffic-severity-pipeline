"""Unit tests for the data-quality validation module."""

import pandas as pd
import pytest

from data_quality import validate_collisions


def _good_df():
    return pd.DataFrame({
        "collision_index": ["C1", "C2", "C3"],
        "longitude": [-0.1, -1.5, 0.5],
        "latitude": [51.5, 53.4, 52.1],
        "date": ["01/06/2026"] * 3,
        "time": ["08:15", "17:40", "23:05"],
        "speed_limit": [30, 60, 40],
        "light_conditions": [1, 4, 7],
        "weather_conditions": [1, 2, -1],
        "road_surface_conditions": [1, 2, 1],
        "urban_or_rural_area": [1, 2, 1],
        "area": ["London", "North West", "South East"],
        "severity_score": [3, 8, 5],
    })


def test_valid_data_passes():
    report = validate_collisions(_good_df())
    assert report.passed, str(report)


def test_empty_data_fails():
    report = validate_collisions(_good_df().iloc[0:0])
    assert not report.passed
    assert any(c.name == "non_empty" and not c.passed for c in report.checks)


def test_missing_column_fails():
    df = _good_df().drop(columns=["severity_score"])
    report = validate_collisions(df)
    assert not report.passed
    assert any(c.name == "required_columns" and not c.passed for c in report.checks)


def test_duplicate_ids_fail():
    df = _good_df()
    df.loc[2, "collision_index"] = "C1"
    report = validate_collisions(df)
    assert not report.passed
    assert any(c.name == "unique_collision_index" and not c.passed for c in report.checks)


@pytest.mark.parametrize("col,bad_value", [
    ("severity_score", 42),        # outside 1-10
    ("speed_limit", 200),          # outside 10-70
    ("latitude", 10.0),            # outside UK bounds
    ("urban_or_rural_area", 9),    # outside 1-3
])
def test_out_of_range_values_fail(col, bad_value):
    df = _good_df()
    df.loc[0, col] = bad_value
    report = validate_collisions(df)
    assert not report.passed
    assert any(c.name == f"range[{col}]" and not c.passed for c in report.checks)


def test_high_null_rate_fails():
    df = _good_df()
    df["severity_score"] = [None, None, 5]  # 67% null > 20% tolerance
    report = validate_collisions(df)
    assert not report.passed
