"""Lake layer: ML duration prediction + traffic/taxi correlation."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.metrics import MetricsTracker
from src.common.storage import ensure_local_dir, medallion_exists, medallion_uri
from src.transform.silver_cleaner import read_silver_trips


def run_duration_model(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    lake_cfg = config.get("lake", {})
    sample_rows = lake_cfg.get("ml_sample_rows", 5000)
    trips = read_silver_trips(spark, config).filter(
        F.col("trip_duration_sec").isNotNull() & (F.col("trip_duration_sec") > 0)
    )

    if config.get("light_mode"):
        trips = trips.filter(F.col("vehicle_type") == "fhvhv").limit(sample_rows)
    else:
        trips = trips.sample(fraction=0.001, seed=42).limit(sample_rows)

    with metrics.track("lake", "ml_duration_prediction") as metric:
        metric.rows_read = trips.count()
        if metric.rows_read < 100:
            metric.extra["skipped"] = "insufficient_rows"
            return

        pdf = trips.select(
            "pickup_hour",
            "pickup_dow",
            "pu_location_id",
            "trip_distance",
            "trip_duration_sec",
        ).toPandas()

        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import mean_absolute_error, r2_score
        from sklearn.model_selection import train_test_split

        features = pdf[["pickup_hour", "pickup_dow", "pu_location_id", "trip_distance"]].fillna(0)
        target = pdf["trip_duration_sec"]
        x_train, x_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

        model = LinearRegression()
        model.fit(x_train, y_train)
        preds = model.predict(x_test)

        report = {
            "model": "LinearRegression",
            "target": "trip_duration_sec",
            "train_rows": len(x_train),
            "test_rows": len(x_test),
            "mae_seconds": round(float(mean_absolute_error(y_test, preds)), 2),
            "r2_score": round(float(r2_score(y_test, preds)), 4),
            "features": list(features.columns),
            "coefficients": dict(zip(features.columns, [round(float(c), 4) for c in model.coef_])),
        }

        out_dir = ensure_local_dir(config, "lake", "ml")
        (out_dir / "duration_model_metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        metric.rows_written = len(x_test)
        metric.extra.update(report)


def run_traffic_correlation(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    traffic_uri = medallion_uri(config, "bronze", "external", "traffic_collisions", "records")
    if not medallion_exists(spark, config, "bronze", "external", "traffic_collisions", "records"):
        return

    trips = read_silver_trips(spark, config)
    if config.get("light_mode"):
        trips = trips.filter(F.col("vehicle_type") == "green")

    with metrics.track("lake", "traffic_taxi_correlation") as metric:
        collisions = spark.read.parquet(traffic_uri)
        metric.rows_read = collisions.count()

        coll_by_borough = (
            collisions.filter(F.col("borough").isNotNull())
            .groupBy("borough", "crash_month")
            .agg(F.count("*").alias("collision_count"))
        )
        trips_by_borough = (
            trips.filter(F.col("pu_borough").isNotNull())
            .withColumn("trip_month", F.col("year_month"))
            .groupBy(F.upper(F.col("pu_borough")).alias("borough"), "trip_month")
            .agg(F.count("*").alias("taxi_trip_count"))
        )

        joined = coll_by_borough.join(
            trips_by_borough,
            (coll_by_borough.borough == trips_by_borough.borough)
            & (coll_by_borough.crash_month == trips_by_borough.trip_month),
            "inner",
        ).select(
            coll_by_borough.borough,
            coll_by_borough.crash_month,
            "collision_count",
            "taxi_trip_count",
        )

        joined.write.mode("overwrite").parquet(
            medallion_uri(config, "lake", "exploration", "traffic_taxi_correlation")
        )
        metric.rows_written = joined.count()
