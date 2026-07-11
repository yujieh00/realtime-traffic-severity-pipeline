"""
Train a Spark MLlib pipeline that predicts a continuous road-accident
severity score (1-10) from STATS19-style collision and vehicle data.

Pipeline overview
-----------------
1. Load the collision and vehicle CSVs with explicit schemas.
2. Flatten to one row per collision (join collision-level + vehicle-derived
   features).
3. Impute missing coded values and median-impute driver age.
4. Feature engineering: peak-traffic bucket, dark-rural interaction,
   vehicle-mix flags.
5. Build two candidate pipelines (Random Forest and Gradient-Boosted Trees),
   train on a 70/30 split, and evaluate with RMSE / MAE / R2 / within-1
   accuracy.
6. Tune the better estimator with CrossValidator + ParamGridBuilder.
7. Persist the best pipeline model to models/ for the streaming job to load.

Run:
    python src/train_model.py
"""

import time

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config import (
    BOOLEAN_FLAGS,
    CODED_CATEGORICAL,
    COLLISIONS_CSV,
    LABEL_COL,
    MODEL_DIR,
    NUMERIC_FEATURES,
    RANDOM_SEED,
    VEHICLES_CSV,
)

# STATS19 vehicle_type code groupings.
MOTORCYCLE_CODES = [2, 3, 4, 5, 23, 97]
HGV_CODES = [19, 20, 21, 98]


# --------------------------------------------------------------------------- #
# Spark session
# --------------------------------------------------------------------------- #
def build_spark():
    return (
        SparkSession.builder
        .master("local[4]")
        .appName("RoadSafety-Severity-ML")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
COLLISION_SCHEMA = StructType([
    StructField("collision_index", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("road_type", IntegerType(), True),
    StructField("speed_limit", IntegerType(), True),
    StructField("junction_detail", IntegerType(), True),
    StructField("junction_control", IntegerType(), True),
    StructField("pedestrian_crossing", IntegerType(), True),
    StructField("light_conditions", IntegerType(), True),
    StructField("weather_conditions", IntegerType(), True),
    StructField("road_surface_conditions", IntegerType(), True),
    StructField("carriageway_hazards", IntegerType(), True),
    StructField("urban_or_rural_area", IntegerType(), True),
    StructField("area", StringType(), True),
    StructField("num_casualties", IntegerType(), True),
    StructField("severity_score", IntegerType(), True),
])

VEHICLE_SCHEMA = StructType([
    StructField("collision_index", StringType(), True),
    StructField("vehicle_reference", IntegerType(), True),
    StructField("vehicle_type", IntegerType(), True),
    StructField("age_of_driver", IntegerType(), True),
    StructField("age_of_vehicle", IntegerType(), True),
])


# --------------------------------------------------------------------------- #
# 1-2. Load and flatten
# --------------------------------------------------------------------------- #
def load_flat(spark):
    collisions = (
        spark.read.option("header", True).schema(COLLISION_SCHEMA).csv(COLLISIONS_CSV)
    )
    vehicles = (
        spark.read.option("header", True).schema(VEHICLE_SCHEMA).csv(VEHICLES_CSV)
    )

    # Attributes of the initiating vehicle (reference 1) - the most "knowable"
    # vehicle to a dispatcher at report time.
    init_vehicle = (
        vehicles.filter(F.col("vehicle_reference") == 1)
        .select(
            "collision_index",
            F.col("age_of_driver").alias("init_age_of_driver"),
            F.col("age_of_vehicle").alias("init_age_of_vehicle"),
        )
    )

    # Collision-level vehicle-mix flags.
    vehicle_agg = vehicles.groupBy("collision_index").agg(
        F.countDistinct("vehicle_reference").alias("num_vehicles"),
        F.max(F.col("vehicle_type").isin(MOTORCYCLE_CODES).cast("int")).alias("has_motorcycle"),
        F.max(F.col("vehicle_type").isin(HGV_CODES).cast("int")).alias("has_hgv"),
        F.max(F.col("age_of_driver").between(17, 24).cast("int")).alias("young_driver_involved"),
    )

    return (
        collisions
        .join(init_vehicle, on="collision_index", how="left")
        .join(vehicle_agg, on="collision_index", how="left")
    )


# --------------------------------------------------------------------------- #
# 3. Imputation
# --------------------------------------------------------------------------- #
def impute(df):
    # Sentinel codes -> null. STATS19 uses -1 for missing; road surface also
    # uses 9 ("unknown").
    df = (
        df
        .withColumn(
            "road_surface_conditions",
            F.when(F.col("road_surface_conditions").isin(-1, 9), None)
             .otherwise(F.col("road_surface_conditions")),
        )
        .withColumn(
            "init_age_of_driver",
            F.when(F.col("init_age_of_driver") == -1, None)
             .otherwise(F.col("init_age_of_driver")),
        )
    )

    # Rule-based fill: if road surface unknown but weather is fine, assume dry.
    df = df.withColumn(
        "road_surface_conditions",
        F.when(F.col("road_surface_conditions").isNull() &
               (F.col("weather_conditions") == 1), F.lit(1))
         .otherwise(F.col("road_surface_conditions")),
    )

    # Median-impute the remaining numeric ages with the MLlib Imputer.
    imputer = Imputer(
        strategy="median",
        inputCols=["init_age_of_driver", "init_age_of_vehicle"],
        outputCols=["init_age_of_driver", "init_age_of_vehicle"],
    )
    df = imputer.fit(df).transform(df)

    # Any coded field still null -> -1 sentinel; boolean flags -> 0.
    df = df.fillna(-1, subset=CODED_CATEGORICAL + ["road_surface_conditions"])
    df = df.fillna(0, subset=["has_motorcycle", "has_hgv", "young_driver_involved",
                              "num_vehicles"])
    return df


# --------------------------------------------------------------------------- #
# 4. Feature engineering
# --------------------------------------------------------------------------- #
def engineer(df):
    # Peak vs off-peak from the HH:MM string.
    df = (
        df
        .withColumn("hour_of_day", F.split(F.col("time"), ":").getItem(0).cast("int"))
        .withColumn(
            "peak_traffic",
            F.when(
                F.col("hour_of_day").between(7, 9) | F.col("hour_of_day").between(16, 18),
                F.lit("Peak"),
            ).otherwise(F.lit("Off-peak")),
        )
    )

    # Dark-rural interaction: rural area (2) during any darkness condition (4-7).
    df = df.withColumn(
        "is_dark_rural",
        ((F.col("urban_or_rural_area") == 2) &
         (F.col("light_conditions").isin(4, 5, 6, 7))).cast("int"),
    )
    return df


# --------------------------------------------------------------------------- #
# 5. Build pipelines
# --------------------------------------------------------------------------- #
def build_pipelines():
    peak_indexer = StringIndexer(
        inputCol="peak_traffic", outputCol="peak_traffic_idx", handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES + CODED_CATEGORICAL + BOOLEAN_FLAGS + ["peak_traffic_idx"],
        outputCol="features",
        handleInvalid="keep",
    )
    rf = RandomForestRegressor(labelCol=LABEL_COL, featuresCol="features",
                               numTrees=60, maxDepth=6, seed=RANDOM_SEED)
    gbt = GBTRegressor(labelCol=LABEL_COL, featuresCol="features",
                       maxIter=40, maxDepth=5, seed=RANDOM_SEED)

    rf_pipeline = Pipeline(stages=[peak_indexer, assembler, rf])
    gbt_pipeline = Pipeline(stages=[peak_indexer, assembler, gbt])
    return {
        "Random Forest": (rf_pipeline, rf),
        "Gradient-Boosted Trees": (gbt_pipeline, gbt),
    }


def param_grid_for(name, estimator):
    """A small, laptop-friendly hyper-parameter grid for the chosen estimator."""
    builder = ParamGridBuilder().addGrid(estimator.maxDepth, [4, 6])
    if name == "Random Forest":
        builder = builder.addGrid(estimator.numTrees, [40, 80])
    else:  # GBT
        builder = builder.addGrid(estimator.maxIter, [30, 50])
    return builder.build()


# --------------------------------------------------------------------------- #
# 6. Evaluation
# --------------------------------------------------------------------------- #
def within_one_accuracy(pred, label=LABEL_COL, pred_col="prediction"):
    """Fraction of rows where the rounded prediction is within +/-1 of the label."""
    return (
        pred.withColumn("_ok", (F.abs(F.round(pred_col) - F.col(label)) <= 1).cast("int"))
        .agg(F.avg("_ok").alias("acc")).first()["acc"]
    )


def evaluate(pred, name):
    metrics = {}
    for m in ["rmse", "mae", "r2"]:
        metrics[m] = RegressionEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName=m,
        ).evaluate(pred)
    metrics["within1"] = within_one_accuracy(pred)
    print(f"  {name:<26} RMSE={metrics['rmse']:.3f}  MAE={metrics['mae']:.3f}  "
          f"R2={metrics['r2']:.3f}  within-1={metrics['within1']:.1%}")
    return metrics


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    # Gate: validate the raw collision data before spinning up Spark.
    import pandas as pd

    from data_quality import validate_collisions
    print("Running data-quality checks ...")
    report = validate_collisions(pd.read_csv(COLLISIONS_CSV))
    print(report)
    if not report.passed:
        raise SystemExit("Data-quality checks failed; aborting training.")
    print()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading + flattening data ...")
    df = engineer(impute(load_flat(spark)))
    df = df.dropna(subset=[LABEL_COL]).cache()
    print(f"  {df.count():,} labelled collisions")

    train, test = df.randomSplit([0.7, 0.3], seed=RANDOM_SEED)
    train.cache()
    test.cache()
    print(f"  train={train.count():,}  test={test.count():,}")

    candidates = build_pipelines()

    print("\nTraining candidate models ...")
    t0 = time.time()
    results = {}
    for name, (pipeline, _est) in candidates.items():
        model = pipeline.fit(train)
        results[name] = evaluate(model.transform(test), name)
    print(f"  (trained in {time.time() - t0:.1f}s)")

    # Pick the better base model by lowest test RMSE, then tune THAT one.
    best_name = min(results, key=lambda n: results[n]["rmse"])
    best_pipeline_spec, best_estimator = candidates[best_name]
    print(f"\nBetter base model: {best_name}")

    print(f"\nTuning {best_name} with CrossValidator (3-fold) ...")
    grid = param_grid_for(best_name, best_estimator)
    cv = CrossValidator(
        estimator=best_pipeline_spec,
        estimatorParamMaps=grid,
        evaluator=RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction",
                                      metricName="rmse"),
        numFolds=3,
        parallelism=2,
        seed=RANDOM_SEED,
    )
    t0 = time.time()
    cv_model = cv.fit(train)
    best_pipeline = cv_model.bestModel
    print(f"  (tuned in {time.time() - t0:.1f}s)")
    evaluate(best_pipeline.transform(test), f"{best_name} (tuned)")

    tuned_est = best_pipeline.stages[-1]
    print(f"  best maxDepth = {tuned_est.getMaxDepth()}")

    # ---- Persist ---- #
    best_pipeline.write().overwrite().save(MODEL_DIR)
    print(f"\nSaved tuned pipeline model -> {MODEL_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
