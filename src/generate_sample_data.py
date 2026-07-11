"""
Generate a small, fully-synthetic road-safety sample in the STATS19 schema.

Why this exists
---------------
The real UK STATS19 accident data is large (hundreds of MB) and is not
committed to this repo. To let anyone clone the project and run the whole
pipeline in minutes, this script fabricates a *small* dataset that has the
same column schema and realistic value ranges as STATS19, plus an engineered
continuous `severity_score` (1-10) used as the regression target.

The data is generated from random distributions - it contains no real
records and no third-party data. To run on the genuine STATS19 data instead,
see data/sample/README.md.

Usage
-----
    python src/generate_sample_data.py                 # default 6000 collisions
    python src/generate_sample_data.py --rows 20000    # bigger sample
"""

import argparse
import os

import numpy as np
import pandas as pd

from config import COLLISIONS_CSV, RANDOM_SEED, SAMPLE_DIR, VEHICLES_CSV

# Nine UK regions used as the "district"/area label on the dashboard map,
# each with an approximate lon/lat centre so the bubble map looks like the UK.
REGIONS = {
    "London":        (-0.13, 51.51),
    "South East":    (-0.75, 51.20),
    "South West":    (-3.53, 50.90),
    "East Midlands": (-1.15, 52.95),
    "West Midlands": (-1.90, 52.48),
    "North West":    (-2.60, 53.48),
    "North East":    (-1.61, 54.97),
    "Yorkshire":     (-1.55, 53.80),
    "Wales":         (-3.18, 51.48),
}


def generate(rows: int, seed: int = RANDOM_SEED):
    rng = np.random.default_rng(seed)

    region_names = list(REGIONS.keys())
    region_idx = rng.integers(0, len(region_names), rows)
    areas = np.array(region_names)[region_idx]

    # Scatter points around each region centre.
    lon = np.array([REGIONS[a][0] for a in areas]) + rng.normal(0, 0.25, rows)
    lat = np.array([REGIONS[a][1] for a in areas]) + rng.normal(0, 0.25, rows)

    # Event time across a single simulated day, kept in chronological order so
    # the producer can replay it like a real-time sensor feed.
    minutes = np.sort(rng.integers(0, 24 * 60, rows))
    times = [f"{m // 60:02d}:{m % 60:02d}" for m in minutes]
    dates = ["01/06/2026"] * rows

    road_type = rng.choice([1, 2, 3, 6, 7, 9], rows, p=[.05, .10, .10, .55, .15, .05])
    speed_limit = rng.choice([20, 30, 40, 50, 60, 70], rows, p=[.05, .45, .15, .10, .15, .10])
    junction_detail = rng.choice([0, 1, 2, 3, 5, 6, -1], rows,
                                 p=[.30, .05, .05, .25, .15, .15, .05])
    junction_control = rng.choice([0, 1, 2, 4, -1], rows, p=[.30, .10, .20, .30, .10])
    pedestrian_crossing = rng.choice([0, 1, 5, -1], rows, p=[.70, .10, .15, .05])
    light_conditions = rng.choice([1, 4, 5, 6, 7], rows, p=[.62, .06, .22, .04, .06])
    weather_conditions = rng.choice([1, 2, 3, 5, 6, 7, 8, -1], rows,
                                    p=[.72, .12, .04, .03, .02, .02, .03, .02])
    road_surface = rng.choice([1, 2, 3, 4, 9, -1], rows, p=[.66, .24, .03, .02, .03, .02])
    carriageway_hazards = rng.choice([0, 1, 2, 6, 7], rows, p=[.92, .02, .02, .02, .02])
    urban_or_rural = rng.choice([1, 2], rows, p=[.64, .36])

    num_vehicles = rng.choice([1, 2, 3, 4], rows, p=[.28, .55, .12, .05])
    num_casualties = rng.choice([1, 2, 3, 4, 5], rows, p=[.68, .20, .07, .03, .02])

    collision_index = np.array([f"C{2026_00000 + i:09d}" for i in range(rows)])

    # ------------------------------------------------------------------ #
    # Engineer the continuous severity_score (1-10).
    # Higher speed, darkness, rural roads, bad weather, more vehicles and
    # more casualties all push severity up. Gaussian noise keeps it a
    # genuine regression problem rather than a deterministic formula.
    # ------------------------------------------------------------------ #
    score = (
        1.8
        + 0.045 * speed_limit
        + 1.4 * (urban_or_rural == 2)
        + 1.2 * np.isin(light_conditions, [4, 5, 6, 7])
        + 0.6 * np.isin(weather_conditions, [2, 3, 5, 6])
        + 0.5 * np.isin(road_surface, [2, 3, 4])
        + 0.7 * (num_vehicles - 1)
        + 0.9 * (num_casualties - 1)
        + rng.normal(0, 1.1, rows)
    )
    severity_score = np.clip(np.round(score), 1, 10).astype(int)

    collisions = pd.DataFrame({
        "collision_index": collision_index,
        "longitude": np.round(lon, 5),
        "latitude": np.round(lat, 5),
        "date": dates,
        "time": times,
        "road_type": road_type,
        "speed_limit": speed_limit,
        "junction_detail": junction_detail,
        "junction_control": junction_control,
        "pedestrian_crossing": pedestrian_crossing,
        "light_conditions": light_conditions,
        "weather_conditions": weather_conditions,
        "road_surface_conditions": road_surface,
        "carriageway_hazards": carriageway_hazards,
        "urban_or_rural_area": urban_or_rural,
        "area": areas,
        "num_casualties": num_casualties,
        "severity_score": severity_score,
    })

    # ------------------------------------------------------------------ #
    # Vehicle table: one row per vehicle, several per collision.
    # ------------------------------------------------------------------ #
    veh_rows = []
    for cidx, nv in zip(collision_index, num_vehicles, strict=False):
        for vref in range(1, nv + 1):
            vtype = int(rng.choice([9, 3, 5, 11, 19, 1], p=[.55, .10, .08, .12, .10, .05]))
            age_driver = int(rng.choice([-1, *range(17, 85)],
                                        p=[.03, *([0.97 / 68] * 68)]))
            age_vehicle = int(rng.choice([-1, *range(0, 25)],
                                         p=[.06, *([0.94 / 25] * 25)]))
            veh_rows.append((cidx, vref, vtype, age_driver, age_vehicle))

    vehicles = pd.DataFrame(
        veh_rows,
        columns=["collision_index", "vehicle_reference", "vehicle_type",
                 "age_of_driver", "age_of_vehicle"],
    )

    return collisions, vehicles


def main():
    parser = argparse.ArgumentParser(description="Generate a STATS19-schema sample.")
    parser.add_argument("--rows", type=int, default=6000,
                        help="number of collisions to generate (default 6000)")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    args = parser.parse_args()

    os.makedirs(SAMPLE_DIR, exist_ok=True)
    collisions, vehicles = generate(args.rows, args.seed)

    collisions.to_csv(COLLISIONS_CSV, index=False)
    vehicles.to_csv(VEHICLES_CSV, index=False)

    print(f"Wrote {len(collisions):,} collisions -> {COLLISIONS_CSV}")
    print(f"Wrote {len(vehicles):,} vehicles   -> {VEHICLES_CSV}")
    print("Severity distribution:")
    print(collisions["severity_score"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
