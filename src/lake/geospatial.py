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


def run_geospatial(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    trips = read_silver_trips(spark, config)

    if config.get("light_mode"):
        trips = trips.filter(F.col("vehicle_type") == "green")

    with metrics.track("lake", "geospatial_od_flows") as metric:
        metric.rows_read = trips.count()
        od = compute_od_flows(trips)
        od.write.mode("overwrite").parquet(medallion_uri(config, "lake", "geospatial", "od_zone_flows"))
        metric.rows_written = od.count()

    with metrics.track("lake", "geospatial_heatmap") as metric:
        heatmap = compute_zone_heatmap(trips)
        metric.rows_read = metric.rows_written = heatmap.count()
        heatmap.write.mode("overwrite").parquet(medallion_uri(config, "lake", "geospatial", "zone_heatmap"))

    with metrics.track("lake", "geospatial_top_routes") as metric:
        routes = compute_top_routes(trips)
        metric.rows_written = routes.count()
        routes.write.mode("overwrite").parquet(medallion_uri(config, "lake", "geospatial", "top_routes"))
