"""Unit tests for the synthetic sample-data generator."""

from data_quality import validate_collisions
from generate_sample_data import REGIONS, generate


def test_generate_shapes_and_schema():
    collisions, vehicles = generate(rows=500, seed=1)
    assert len(collisions) == 500
    # Every collision has at least one vehicle.
    assert len(vehicles) >= 500
    for col in ["collision_index", "severity_score", "area", "latitude", "longitude"]:
        assert col in collisions.columns
    for col in ["collision_index", "vehicle_reference", "vehicle_type"]:
        assert col in vehicles.columns


def test_severity_in_range():
    collisions, _ = generate(rows=1000, seed=2)
    assert collisions["severity_score"].between(1, 10).all()


def test_areas_are_known_regions():
    collisions, _ = generate(rows=300, seed=3)
    assert set(collisions["area"].unique()).issubset(set(REGIONS))


def test_collision_ids_unique():
    collisions, _ = generate(rows=800, seed=4)
    assert collisions["collision_index"].is_unique


def test_generated_data_passes_quality_checks():
    collisions, _ = generate(rows=1000, seed=5)
    report = validate_collisions(collisions)
    assert report.passed, str(report)


def test_reproducible_with_seed():
    a, _ = generate(rows=200, seed=42)
    b, _ = generate(rows=200, seed=42)
    assert a.equals(b)
