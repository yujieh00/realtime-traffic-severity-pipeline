"""
Spark Structured Streaming job: the heart of the pipeline.

It consumes the live collision stream from Kafka, rebuilds the exact feature
set the trained pipeline expects, scores each accident's severity in real time,
and publishes three rolling aggregations back to Kafka for the dashboard:

    accident_severity_counts  - count per predicted severity, 10s tumbling window
    accident_district_counts  - low/med/high counts per region, 30s window
    accident_predictions      - per-accident location + score for the map

Key streaming techniques on show:
    * JSON deserialisation from Kafka
    * event-time watermarking (drop events > 30s late)
    * stream-static join to attach vehicle features
    * loading a persisted Spark ML PipelineModel for inference
    * tumbling-window aggregations
    * Parquet sink + Kafka sink

Run (after the model exists and Kafka is up):
    python src/spark_streaming.py
"""

import os
import shutil

import pyspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import (
    CHECKPOINT_ROOT,
    KAFKA_BOOTSTRAP,
    MODEL_DIR,
    OUTPUT_ROOT,
    SEVERITY_ALERT_THRESHOLD,
    TOPIC_DISTRICT_COUNTS,
    TOPIC_INCOMING,
    TOPIC_PREDICTIONS_MAP,
    TOPIC_SEVERITY_COUNTS,
    VEHICLES_CSV,
    WATERMARK_DELAY,
)

MOTORCYCLE_CODES = [2, 3, 4, 5, 23, 97]
HGV_CODES = [19, 20, 21, 98]

# Every field arrives from Kafka as a JSON string except event_time (epoch long).
WIRE_FIELDS = [
    "collision_index", "longitude", "latitude", "date", "time", "road_type",
    "speed_limit", "junction_detail", "junction_control", "pedestrian_crossing",
    "light_conditions", "weather_conditions", "road_surface_conditions",
    "carriageway_hazards", "urban_or_rural_area", "area",
]
WIRE_SCHEMA = StructType(
    [StructField(f, StringType(), True) for f in WIRE_FIELDS]
    + [StructField("event_time", LongType(), True)]
)

INT_COLS = [
    "road_type", "speed_limit", "junction_detail", "junction_control",
    "pedestrian_crossing", "light_conditions", "weather_conditions",
    "road_surface_conditions", "carriageway_hazards", "urban_or_rural_area",
]


def build_spark():
    """Spark session with the Kafka connector matched to the runtime version."""
    version = pyspark.__version__.split("+")[0]
    scala = "2.13" if version.startswith("4.") else "2.12"
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages org.apache.spark:spark-sql-kafka-0-10_{scala}:{version} "
        "pyspark-shell"
    )
    return (
        SparkSession.builder
        .master("local[4]")
        .appName("RoadSafety-Severity-Streaming")
        .config("spark.sql.session.timeZone", "Europe/London")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def static_vehicle_features(spark):
    """Aggregate the (static) vehicle table into per-collision features."""
    vehicles = spark.read.option("header", True).csv(VEHICLES_CSV)
    for c in ["vehicle_reference", "vehicle_type", "age_of_driver", "age_of_vehicle"]:
        vehicles = vehicles.withColumn(c, F.col(c).cast("int"))

    init_vehicle = (
        vehicles.filter(F.col("vehicle_reference") == 1)
        .select(
            "collision_index",
            F.col("age_of_driver").alias("init_age_of_driver"),
            F.col("age_of_vehicle").alias("init_age_of_vehicle"),
        )
    )
    vehicle_agg = vehicles.groupBy("collision_index").agg(
        F.countDistinct("vehicle_reference").alias("num_vehicles"),
        F.max(F.col("vehicle_type").isin(MOTORCYCLE_CODES).cast("int")).alias("has_motorcycle"),
        F.max(F.col("vehicle_type").isin(HGV_CODES).cast("int")).alias("has_hgv"),
        F.max(F.col("age_of_driver").between(17, 24).cast("int")).alias("young_driver_involved"),
    )
    return init_vehicle, vehicle_agg


def parse_stream(spark):
    """Read Kafka, deserialise JSON, cast types, derive event_time timestamp."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_INCOMING)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), WIRE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("latitude", F.col("latitude").cast("double"))
    )
    for c in INT_COLS:
        parsed = parsed.withColumn(c, F.col(c).cast("int"))
    # Epoch seconds -> event-time timestamp, then watermark.
    parsed = parsed.withColumn("event_time", F.col("event_time").cast("timestamp"))
    return parsed.withWatermark("event_time", WATERMARK_DELAY)


def build_features(stream_df, init_vehicle, vehicle_agg):
    """Reproduce the training-time feature set on the live stream."""
    df = (
        stream_df
        .join(init_vehicle, on="collision_index", how="left")
        .join(vehicle_agg, on="collision_index", how="left")
        .withColumn("hour_of_day", F.split(F.col("time"), ":").getItem(0).cast("int"))
        .withColumn(
            "peak_traffic",
            F.when(
                F.col("hour_of_day").between(7, 9) | F.col("hour_of_day").between(16, 18),
                F.lit("Peak"),
            ).otherwise(F.lit("Off-peak")),
        )
        .withColumn(
            "is_dark_rural",
            ((F.col("urban_or_rural_area") == 2) &
             (F.col("light_conditions").isin(4, 5, 6, 7))).cast("int"),
        )
    )
    # Fill the same way training did: -1 for coded fields, 0 for flags.
    df = df.fillna(-1, subset=INT_COLS + ["init_age_of_driver", "init_age_of_vehicle"])
    df = df.fillna(0, subset=["has_motorcycle", "has_hgv", "young_driver_involved",
                              "num_vehicles"])
    return df


def to_kafka(df, topic, checkpoint, mode="append", trigger="10 seconds"):
    """Publish a result stream as JSON to a Kafka topic."""
    return (
        df.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint)
        .outputMode(mode)
        .trigger(processingTime=trigger)
        .start()
    )


def main():
    # Fresh streaming state each run keeps the demo reproducible.
    for path in (CHECKPOINT_ROOT, OUTPUT_ROOT):
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(MODEL_DIR)
    print("Loaded model:", [type(s).__name__ for s in model.stages])

    init_vehicle, vehicle_agg = static_vehicle_features(spark)
    stream = parse_stream(spark)
    features = build_features(stream, init_vehicle, vehicle_agg)

    # Score every incoming accident; clamp the rounded score to 1-10.
    predictions = model.transform(features).withColumn(
        "pred_severity",
        F.greatest(F.lit(1), F.least(F.lit(10), F.round("prediction").cast("int"))),
    )

    # 1) Per-accident location + score -> map topic (append).
    map_feed = predictions.select(
        "event_time", "collision_index", "area", "latitude", "longitude",
        F.round("prediction", 2).alias("prediction"), "pred_severity",
    )

    # 2) Count per predicted severity, 10s tumbling window.
    severity_counts = (
        predictions
        .groupBy(F.window("event_time", "10 seconds").alias("w"), "pred_severity")
        .agg(F.count("*").alias("num_accidents"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "pred_severity", "num_accidents",
        )
    )

    # 3) Low/med/high counts per region, 30s tumbling window.
    district_counts = (
        predictions
        .groupBy(F.window("event_time", "30 seconds").alias("w"), F.col("area").alias("district"))
        .agg(
            F.sum((F.col("pred_severity").between(1, 3)).cast("int")).alias("low_1_3"),
            F.sum((F.col("pred_severity").between(4, 6)).cast("int")).alias("medium_4_6"),
            F.sum((F.col("pred_severity").between(7, 10)).cast("int")).alias("high_7_10"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "district", "low_1_3", "medium_4_6", "high_7_10",
        )
    )

    # Console alert for high-severity accidents (append mode).
    alerts = predictions.filter(F.col("prediction") > SEVERITY_ALERT_THRESHOLD).select(
        "event_time", "collision_index", "area",
        F.round("prediction", 2).alias("pred_severity_raw"),
    )

    # Start every sink. We await on spark.streams below, so the individual
    # query handles are collected only to keep them alive / referenced.
    queries = [
        # Console alert stream.
        (alerts.writeStream.format("console")
         .outputMode("append").option("truncate", False)
         .trigger(processingTime="5 seconds").start()),

        # Durable Parquet copy of the severity aggregation.
        (severity_counts.writeStream.format("parquet")
         .option("path", f"{OUTPUT_ROOT}/severity_counts")
         .option("checkpointLocation", f"{CHECKPOINT_ROOT}/pq_sev")
         .outputMode("append").trigger(processingTime="10 seconds").start()),

        # Kafka sinks that feed the dashboard.
        to_kafka(map_feed, TOPIC_PREDICTIONS_MAP,
                 f"{CHECKPOINT_ROOT}/k_map", trigger="5 seconds"),
        to_kafka(severity_counts, TOPIC_SEVERITY_COUNTS,
                 f"{CHECKPOINT_ROOT}/k_sev", trigger="10 seconds"),
        to_kafka(district_counts, TOPIC_DISTRICT_COUNTS,
                 f"{CHECKPOINT_ROOT}/k_dist", trigger="30 seconds"),
    ]

    print(f"Streaming ({len(queries)} sinks active)... Ctrl-C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
