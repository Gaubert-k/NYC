"""Lake layer: anomaly detection on Silver (schema-on-read analytics)."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.metrics import MetricsTracker
from src.common.storage import medallion_uri
from src.transform.silver_cleaner import read_silver_trips


def detect_anomalies(trips: DataFrame, z_threshold: float = 3.0) -> DataFrame:
    stats = trips.groupBy("vehicle_type").agg(
        F.avg("fare_amount").alias("mean_fare"),
        F.stddev("fare_amount").alias("std_fare"),
        F.avg("trip_distance").alias("mean_distance"),
        F.stddev("trip_distance").alias("std_distance"),
    )

    enriched = trips.join(stats, on="vehicle_type", how="left")
    fare_z = F.when(
        F.col("std_fare") > 0,
        F.abs((F.col("fare_amount") - F.col("mean_fare")) / F.col("std_fare")),
    ).otherwise(F.lit(0.0))
    dist_z = F.when(
        F.col("std_distance") > 0,
        F.abs((F.col("trip_distance") - F.col("mean_distance")) / F.col("std_distance")),
    ).otherwise(F.lit(0.0))

    flagged = (
        enriched.withColumn("fare_zscore", fare_z)
        .withColumn("distance_zscore", dist_z)
        .withColumn(
            "anomaly_reason",
            F.when(
                (F.col("fare_zscore") > z_threshold) | (F.col("distance_zscore") > z_threshold),
                F.concat_ws(
                    ",",
                    F.when(F.col("fare_zscore") > z_threshold, F.lit("fare_outlier")),
                    F.when(F.col("distance_zscore") > z_threshold, F.lit("distance_outlier")),
                    F.when(F.col("pickup_ts") > F.col("dropoff_ts"), F.lit("time_inversion")),
                    F.when(F.col("pu_location_id") == F.col("do_location_id"), F.lit("same_zone")),
                ),
            ).otherwise(F.lit(None)),
        )
        .filter(F.col("anomaly_reason").isNotNull())
    )

    return flagged.select(
        "vehicle_type",
        "pickup_ts",
        "dropoff_ts",
        "pu_location_id",
        "pu_zone_name",
        "do_location_id",
        "do_zone_name",
        "fare_amount",
        "trip_distance",
        "fare_zscore",
        "distance_zscore",
        "anomaly_reason",
        "source_file",
    )


def run_anomalies(
    spark: SparkSession,
    config: dict[str, Any],
    metrics: MetricsTracker,
    trips: DataFrame | None = None,
) -> None:
    lake_cfg = config.get("lake", {})
    threshold = lake_cfg.get("anomaly_zscore_threshold", 3.0)
    max_output = lake_cfg.get("anomaly_max_output_rows", 50_000)
    if trips is None:
        trips = read_silver_trips(spark, config)
        if config.get("light_mode"):
            trips = trips.filter(F.col("vehicle_type") == "green")

    with metrics.track("lake", "anomaly_detection") as metric:
        total_rows = 0 if trips.is_cached else trips.count()
        metric.rows_read = total_rows

        # Full run : echantillonner pour eviter shuffle 300M+ lignes / saturation disque
        sample_fraction = lake_cfg.get("anomaly_sample_fraction", 0.01)
        if total_rows > 5_000_000 and not config.get("sample_mode"):
            trips = trips.sample(fraction=sample_fraction, seed=42)
            metric.extra["sampled_fraction"] = sample_fraction

        stats = trips.groupBy("vehicle_type").agg(
            F.avg("fare_amount").alias("mean_fare"),
            F.stddev("fare_amount").alias("std_fare"),
            F.avg("trip_distance").alias("mean_distance"),
            F.stddev("trip_distance").alias("std_distance"),
        )
        from pyspark.sql.functions import broadcast

        enriched = trips.join(broadcast(stats), on="vehicle_type", how="left")
        fare_z = F.when(
            F.col("std_fare") > 0,
            F.abs((F.col("fare_amount") - F.col("mean_fare")) / F.col("std_fare")),
        ).otherwise(F.lit(0.0))
        dist_z = F.when(
            F.col("std_distance") > 0,
            F.abs((F.col("trip_distance") - F.col("mean_distance")) / F.col("std_distance")),
        ).otherwise(F.lit(0.0))

        anomalies = (
            enriched.withColumn("fare_zscore", fare_z)
            .withColumn("distance_zscore", dist_z)
            .withColumn(
                "anomaly_reason",
                F.when(
                    (F.col("fare_zscore") > threshold) | (F.col("distance_zscore") > threshold),
                    F.concat_ws(
                        ",",
                        F.when(F.col("fare_zscore") > threshold, F.lit("fare_outlier")),
                        F.when(F.col("distance_zscore") > threshold, F.lit("distance_outlier")),
                        F.when(F.col("pickup_ts") > F.col("dropoff_ts"), F.lit("time_inversion")),
                        F.when(F.col("pu_location_id") == F.col("do_location_id"), F.lit("same_zone")),
                    ),
                ).otherwise(F.lit(None)),
            )
            .filter(F.col("anomaly_reason").isNotNull())
            .select(
                "vehicle_type",
                "pickup_ts",
                "dropoff_ts",
                "pu_location_id",
                "pu_zone_name",
                "do_location_id",
                "do_zone_name",
                "fare_amount",
                "trip_distance",
                "fare_zscore",
                "distance_zscore",
                "anomaly_reason",
                "source_file",
            )
            .limit(max_output)
        )

        anomalies.coalesce(4).write.mode("overwrite").parquet(
            medallion_uri(config, "lake", "ml", "anomaly_trips")
        )
        metric.rows_written = anomalies.count()
        metric.extra["anomaly_rate"] = round(metric.rows_written / max(metric.rows_read, 1), 6)
