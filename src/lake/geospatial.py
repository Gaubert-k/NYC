"""Lake layer: geospatial analytics (O-D flows, heatmaps)."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.metrics import MetricsTracker
from src.common.storage import local_path, medallion_uri
from src.transform.silver_cleaner import read_silver_trips


def compute_od_flows(trips: DataFrame) -> DataFrame:
    return (
        trips.filter(F.col("pu_location_id").isNotNull() & F.col("do_location_id").isNotNull())
        .groupBy(
            "vehicle_type",
            "pu_location_id",
            "pu_borough",
            "pu_zone_name",
            "do_location_id",
            "do_borough",
            "do_zone_name",
            "pickup_hour",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy(F.desc("trip_count"))
    )


def compute_zone_heatmap(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("vehicle_type", "pu_location_id", "pu_borough", "pu_zone_name", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy(F.desc("trip_count"))
    )


def compute_top_routes(trips: DataFrame, top_n: int = 50) -> DataFrame:
    return (
        compute_od_flows(trips)
        .select(
            "vehicle_type",
            "pu_zone_name",
            "pu_borough",
            "do_zone_name",
            "do_borough",
            "trip_count",
            "avg_fare",
            "avg_distance",
        )
        .orderBy(F.desc("trip_count"))
        .limit(top_n)
    )


def run_geospatial(
    spark: SparkSession,
    config: dict[str, Any],
    metrics: MetricsTracker,
    trips: DataFrame | None = None,
) -> None:
    if trips is None:
        trips = read_silver_trips(spark, config)
        if config.get("light_mode"):
            trips = trips.filter(F.col("vehicle_type") == "green")

    lake_cfg = config.get("lake", {})
    geo_frac = lake_cfg.get("geospatial_sample_fraction", 0.05)
    if geo_frac < 1.0 and not config.get("light_mode"):
        trips = trips.sample(fraction=geo_frac, seed=42)

    with metrics.track("lake", "geospatial_od_flows") as metric:
        od = compute_od_flows(trips)
        od.coalesce(8).write.mode("overwrite").parquet(medallion_uri(config, "lake", "geospatial", "od_zone_flows"))

    with metrics.track("lake", "geospatial_heatmap") as metric:
        heatmap = compute_zone_heatmap(trips)
        heatmap.write.mode("overwrite").parquet(medallion_uri(config, "lake", "geospatial", "zone_heatmap"))

    with metrics.track("lake", "geospatial_top_routes") as metric:
        routes = (
            od.select(
                "vehicle_type",
                "pu_zone_name",
                "pu_borough",
                "do_zone_name",
                "do_borough",
                "trip_count",
                "avg_fare",
                "avg_distance",
            )
            .orderBy(F.desc("trip_count"))
            .limit(50)
        )
        routes.write.mode("overwrite").parquet(medallion_uri(config, "lake", "geospatial", "top_routes"))
