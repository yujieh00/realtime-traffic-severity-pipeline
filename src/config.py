"""
Central configuration for the real-time traffic-severity pipeline.

Everything that more than one script needs to agree on lives here:
the Kafka broker address, topic names, file paths, and the column
schema of the UK STATS19-style road-safety data.

Keeping this in one place means the producer, the Spark streaming job,
the model trainer and the dashboard can never drift out of sync.
"""

import os

# --------------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------------- #
# When running inside docker-compose the broker is reachable as "kafka".
# When running everything on your laptop set KAFKA_BOOTSTRAP=localhost:9092.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# Topic the producer writes to and the Spark job subscribes to.
TOPIC_INCOMING = "accident_stream"

# Topics the Spark job publishes results to (the dashboard subscribes to these).
TOPIC_SEVERITY_COUNTS = "accident_severity_counts"
TOPIC_DISTRICT_COUNTS = "accident_district_counts"
TOPIC_PREDICTIONS_MAP = "accident_predictions"

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SAMPLE_DIR = os.path.join(DATA_DIR, "sample")

COLLISIONS_CSV = os.path.join(SAMPLE_DIR, "collisions_sample.csv")
VEHICLES_CSV = os.path.join(SAMPLE_DIR, "vehicles_sample.csv")

MODEL_DIR = os.path.join(PROJECT_ROOT, "models", "severity_pipeline")

CHECKPOINT_ROOT = os.path.join(PROJECT_ROOT, "checkpoint")
OUTPUT_ROOT = os.path.join(PROJECT_ROOT, "output")

# --------------------------------------------------------------------------- #
# Streaming behaviour
# --------------------------------------------------------------------------- #
WATERMARK_DELAY = "30 seconds"      # drop events that arrive more than 30s late
MIN_BATCH = 50                      # producer: min rows emitted per second
MAX_BATCH = 100                     # producer: max rows emitted per second
SEND_INTERVAL = 1                   # producer: seconds between batches
SEVERITY_ALERT_THRESHOLD = 7        # predicted severity above this = "high"

# --------------------------------------------------------------------------- #
# STATS19-style schema
# --------------------------------------------------------------------------- #
# Column names below follow the public UK STATS19 road-safety data published by
# the Department for Transport (DfT) on data.gov.uk. Coded fields use small
# integer codes; -1 is the STATS19 convention for "missing / not known".
#
# `severity_score` (1-10) is a CONTINUOUS severity index engineered for this
# project's regression demo. The raw STATS19 file only carries a 3-level
# `accident_severity`; see data/sample/README.md for how the index is derived.

# Coded categorical fields (small integer codes).
CODED_CATEGORICAL = [
    "road_type",
    "junction_detail",
    "junction_control",
    "pedestrian_crossing",
    "light_conditions",
    "weather_conditions",
    "road_surface_conditions",
    "carriageway_hazards",
    "urban_or_rural_area",
]

# Continuous / numeric fields used as model features.
# NOTE: num_casualties is deliberately NOT a feature. The severity index is
# partly derived from casualties, so using it would leak the label.
NUMERIC_FEATURES = [
    "speed_limit",
    "init_age_of_driver",
    "init_age_of_vehicle",
    "num_vehicles",
]

# Boolean 0/1 flags derived during feature engineering.
BOOLEAN_FLAGS = [
    "has_motorcycle",
    "has_hgv",
    "young_driver_involved",
    "is_dark_rural",
]

LABEL_COL = "severity_score"
RANDOM_SEED = 2026
