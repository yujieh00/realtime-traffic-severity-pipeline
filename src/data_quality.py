"""
Data-quality validation for the collision feed.

Before any data reaches the model - whether the batch training set or the live
stream sample - it should pass a set of explicit quality checks. This module
runs schema, null-rate and value-range checks and returns a structured report
so failures are visible and actionable rather than silent.

It is deliberately pandas-based (no Spark) so it is fast, easy to unit-test, and
usable as a standalone gate:

    python src/data_quality.py                       # check the bundled sample
    python src/data_quality.py path/to/collisions.csv
"""

from dataclasses import dataclass, field
from typing import List

import pandas as pd

from config import COLLISIONS_CSV

# Columns that must exist, and the inclusive value range each must fall in.
# None means "no numeric range check" (presence/null check only).
REQUIRED_RANGES = {
    "collision_index": None,
    "longitude": (-8.7, 2.0),      # UK longitude bounds
    "latitude": (49.8, 61.0),      # UK latitude bounds
    "date": None,
    "time": None,
    "speed_limit": (10, 70),
    "light_conditions": (-1, 7),
    "weather_conditions": (-1, 9),
    "road_surface_conditions": (-1, 9),
    "urban_or_rural_area": (1, 3),
    "area": None,
    "severity_score": (1, 10),
}

# Columns where a high null rate should fail the check.
MAX_NULL_RATE = 0.20


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


@dataclass
class QualityReport:
    checks: List[Check] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str = ""):
        self.checks.append(Check(name, passed, detail))

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def __str__(self) -> str:
        lines = ["Data-quality report", "-" * 60]
        for c in self.checks:
            mark = "PASS" if c.passed else "FAIL"
            lines.append(f"[{mark}] {c.name}" + (f" — {c.detail}" if c.detail else ""))
        lines.append("-" * 60)
        lines.append("OVERALL: " + ("PASS ✅" if self.passed else "FAIL ❌"))
        return "\n".join(lines)


def validate_collisions(df: pd.DataFrame) -> QualityReport:
    """Run all quality checks on a collisions DataFrame and return a report."""
    report = QualityReport()

    # 1) Non-empty.
    report.add("non_empty", len(df) > 0, f"{len(df):,} rows")

    # 2) Required columns present.
    missing = [c for c in REQUIRED_RANGES if c not in df.columns]
    report.add("required_columns", not missing,
               "all present" if not missing else f"missing: {missing}")

    # 3) No duplicate collision IDs.
    if "collision_index" in df.columns:
        dupes = int(df["collision_index"].duplicated().sum())
        report.add("unique_collision_index", dupes == 0, f"{dupes} duplicates")

    # 4) Null rates within tolerance.
    for col in REQUIRED_RANGES:
        if col in df.columns:
            null_rate = float(df[col].isna().mean())
            report.add(f"null_rate[{col}]", null_rate <= MAX_NULL_RATE,
                       f"{null_rate:.1%} null")

    # 5) Numeric value ranges.
    for col, rng in REQUIRED_RANGES.items():
        if rng is None or col not in df.columns:
            continue
        lo, hi = rng
        numeric = pd.to_numeric(df[col], errors="coerce")
        out_of_range = int(((numeric < lo) | (numeric > hi)).sum())
        report.add(f"range[{col}]", out_of_range == 0,
                   f"{out_of_range} outside [{lo}, {hi}]")

    return report


def main():
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else COLLISIONS_CSV
    print(f"Validating: {path}\n")
    df = pd.read_csv(path)
    report = validate_collisions(df)
    print(report)
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
