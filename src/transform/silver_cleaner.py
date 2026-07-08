"""Silver layer: validation, deduplication, zone enrichment."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import path
from src.common.metrics import MetricsTracker
from src.common.storage import local_path, medallion_exists, medallion_uri
from src.schemas import (
    DISTANCE_COLUMNS,
    DO_LOCATION_COLUMNS,
    DROPOFF_COLUMNS,
    PU_LOCATION_COLUMNS,
    PICKUP_COLUMNS,
)


def _normalize_trips(df: DataFrame, vehicle_type: str) -> DataFrame:
    pickup_col = PICKUP_COLUMNS[vehicle_type]
    dropoff_col = DROPOFF_COLUMNS[vehicle_type]
    pu_col = PU_LOCATION_COLUMNS[vehicle_type]
    do_col = DO_LOCATION_COLUMNS[vehicle_type]
    dist_col = DISTANCE_COLUMNS.get(vehicle_type)

    normalized = (
        df.withColumn("vehicle_type", F.lit(vehicle_type))
        .withColumn("pickup_ts", F.col(pickup_col).cast("timestamp"))
        .withColumn("dropoff_ts", F.col(dropoff_col).cast("timestamp"))
        .withColumn("pu_location_id", F.col(pu_col).cast("int"))
        .withColumn("do_location_id", F.col(do_col).cast("int"))
        .withColumn("ingestion_ts", F.col("_ingestion_ts"))
        .withColumn("source_file", F.col("_source_file"))
    )

    if dist_col and dist_col in df.columns:
        normalized = normalized.withColumn("trip_distance", F.col(dist_col).cast("double"))
    else:
        normalized = normalized.withColumn("trip_distance", F.lit(None).cast("double"))

    fare_exprs = []
    if "fare_amount" in df.columns:
        fare_exprs.append(F.col("fare_amount").cast("double"))
    if "base_passenger_fare" in df.columns:
        fare_exprs.append(F.col("base_passenger_fare").cast("double"))
    if fare_exprs:
        normalized = normalized.withColumn("fare_amount", F.coalesce(*fare_exprs))
    else:
        normalized = normalized.withColumn("fare_amount", F.lit(None).cast("double"))

    if "total_amount" in df.columns:
        normalized = normalized.withColumn("total_amount", F.col("total_amount").cast("double"))
    elif "base_passenger_fare" in df.columns:
        normalized = normalized.withColumn("total_amount", F.col("base_passenger_fare").cast("double"))
    else:
        normalized = normalized.withColumn("total_amount", F.lit(None).cast("double"))

    if "payment_type" in df.columns:
        normalized = normalized.withColumn("payment_type", F.col("payment_type").cast("int"))
    else:
        normalized = normalized.withColumn("payment_type", F.lit(None).cast("int"))

    if "passenger_count" in df.columns:
        normalized = normalized.withColumn("passenger_count", F.col("passenger_count").cast("double"))
    else:
        normalized = normalized.withColumn("passenger_count", F.lit(None).cast("double"))

    if "trip_time" in df.columns:
        normalized = normalized.withColumn("trip_duration_sec", F.col("trip_time").cast("int"))
    else:
        normalized = normalized.withColumn("trip_duration_sec", F.lit(None).cast("int"))

    if "shared_match_flag" in df.columns:
        normalized = normalized.withColumn("shared_match_flag", F.col("shared_match_flag"))
    else:
        normalized = normalized.withColumn("shared_match_flag", F.lit(None).cast("string"))

    return normalized.select(
        "vehicle_type",
        "pickup_ts",
        "dropoff_ts",
        "pu_location_id",
        "do_location_id",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "payment_type",
        "passenger_count",
        "trip_duration_sec",
        "shared_match_flag",
        "year_month",
        "ingestion_ts",
        "source_file",
    )


def _apply_quality_filters(df: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, int]:
    quality = config.get("quality", {})
    min_loc = quality.get("min_location_id", 1)
    max_loc = quality.get("max_location_id", 265)
    min_fare = quality.get("min_fare_amount", 0.0)
    max_fare = quality.get("max_fare_amount", 500.0)

    before = df.count()
    filtered = df.filter(
        F.col("pickup_ts").isNotNull()
        & F.col("pu_location_id").isNotNull()
        & F.col("do_location_id").isNotNull()
        & (F.col("pu_location_id") >= min_loc)
        & (F.col("pu_location_id") <= max_loc)
        & (F.col("do_location_id") >= min_loc)
        & (F.col("do_location_id") <= max_loc)
        & (
            F.col("fare_amount").isNull()
            | ((F.col("fare_amount") >= min_fare) & (F.col("fare_amount") <= max_fare))
        )
    )
    invalid = before - filtered.count()
    return filtered, invalid


def _deduplicate(df: DataFrame) -> tuple[DataFrame, int]:
    before = df.count()
    window = Window.partitionBy(
        "vehicle_type",
        "pickup_ts",
        "dropoff_ts",
        "pu_location_id",
        "do_location_id",
        "fare_amount",
    ).orderBy(F.col("ingestion_ts").desc())
    deduped = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    removed = before - deduped.count()
    return deduped, removed


def _enrich_zones(df: DataFrame, zones_df: DataFrame) -> DataFrame:
    pu_zones = zones_df.select(
        F.col("LocationID").alias("pu_location_id"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone_name"),
    )
    do_zones = zones_df.select(
        F.col("LocationID").alias("do_location_id"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone_name"),
    )
    return (
        df.join(pu_zones, on="pu_location_id", how="left")
        .join(do_zones, on="do_location_id", how="left")
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("pickup_dow", F.dayofweek("pickup_ts"))
        .withColumn("trip_date", F.to_date("pickup_ts"))
    )


def transform_silver(
    spark: SparkSession,
    config: dict[str, Any],
    metrics: MetricsTracker,
    vehicle_type: str | None = None,
) -> None:
    silver_dir = medallion_uri(config, "silver", "trips_unified")
    quality_report_path = local_path(config, "silver", "quality_report.json")

    types = [vehicle_type] if vehicle_type else config.get("vehicle_types", [])
    zones_df = spark.read.parquet(medallion_uri(config, "bronze", "reference", "taxi_zones"))

    all_frames: list[DataFrame] = []
    quality_report: dict[str, Any] = {}

    for vtype in types:
        if not medallion_exists(spark, config, "bronze", "trips", f"vehicle_type={vtype}"):
            continue

        with metrics.track("silver", f"transform_{vtype}", vehicle_type=vtype) as metric:
            raw_df = spark.read.parquet(
                medallion_uri(config, "bronze", "trips", f"vehicle_type={vtype}")
            )
            metric.rows_read = raw_df.count()

            normalized = _normalize_trips(raw_df, vtype)
            null_rate = (
                normalized.filter(F.col("pickup_ts").isNull()).count() / max(metric.rows_read, 1)
            )
            metric.null_rate = round(null_rate, 4)

            filtered, invalid = _apply_quality_filters(normalized, config)
            metric.invalid_records = invalid

            deduped, dup_removed = _deduplicate(filtered)
            metric.duplicates_removed = dup_removed

            enriched = _enrich_zones(deduped, zones_df)
            metric.rows_written = enriched.count()
            all_frames.append(enriched)

            quality_report[vtype] = {
                "rows_read": metric.rows_read,
                "rows_written": metric.rows_written,
                "invalid_records": invalid,
                "duplicates_removed": dup_removed,
                "null_rate": metric.null_rate,
            }

    if not all_frames:
        return

    unified = all_frames[0]
    for frame in all_frames[1:]:
        unified = unified.unionByName(frame, allowMissingColumns=True)

    unified.write.mode("overwrite").partitionBy("vehicle_type", "year_month").parquet(silver_dir)
    quality_report_path.parent.mkdir(parents=True, exist_ok=True)
    quality_report_path.write_text(json.dumps(quality_report, indent=2), encoding="utf-8")


def read_silver_trips(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    return spark.read.parquet(medallion_uri(config, "silver", "trips_unified"))
