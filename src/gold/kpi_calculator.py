"""Gold layer: compute KPI aggregations from Silver."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.metrics import MetricsTracker
from src.common.storage import local_path, medallion_uri
from src.transform.silver_cleaner import read_silver_trips


def _read_weather(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    return spark.read.parquet(medallion_uri(config, "bronze", "weather", "daily"))


def compute_kpis(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> dict[str, DataFrame]:
    trips = read_silver_trips(spark, config)
    weather = _read_weather(spark, config)

    with metrics.track("gold", "compute_kpis") as metric:
        kpis: dict[str, DataFrame] = {}

        kpis["kpi_trips_by_zone_hour"] = (
            trips.groupBy("pu_location_id", "pu_borough", "pu_zone_name", "pickup_hour")
            .agg(F.count("*").alias("trip_count"))
            .orderBy(F.desc("trip_count"))
        )

        kpis["kpi_trips_by_zone_day"] = (
            trips.groupBy("pu_location_id", "pu_borough", "pu_zone_name", "pickup_dow")
            .agg(F.count("*").alias("trip_count"))
            .orderBy(F.desc("trip_count"))
        )

        kpis["kpi_avg_fare_by_vehicle"] = trips.groupBy("vehicle_type").agg(
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("total_amount").alias("avg_total"),
            F.count("*").alias("trip_count"),
        )

        kpis["kpi_avg_fare_by_hour"] = (
            trips.groupBy("vehicle_type", "pickup_hour")
            .agg(F.avg("fare_amount").alias("avg_fare"), F.count("*").alias("trip_count"))
            .orderBy("vehicle_type", "pickup_hour")
        )

        kpis["kpi_avg_distance"] = (
            trips.filter(F.col("trip_distance").isNotNull())
            .groupBy("vehicle_type", "pu_location_id", "pu_zone_name")
            .agg(F.avg("trip_distance").alias("avg_distance"), F.count("*").alias("trip_count"))
        )

        kpis["kpi_monthly_trend"] = (
            trips.groupBy("vehicle_type", "year_month")
            .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
            .orderBy("vehicle_type", "year_month")
        )

        kpis["kpi_top_pickup_hotspots"] = (
            trips.groupBy("pu_location_id", "pu_borough", "pu_zone_name")
            .agg(F.count("*").alias("trip_count"))
            .orderBy(F.desc("trip_count"))
            .limit(10)
        )

        kpis["kpi_top_dropoff_hotspots"] = (
            trips.groupBy("do_location_id", "do_borough", "do_zone_name")
            .agg(F.count("*").alias("trip_count"))
            .orderBy(F.desc("trip_count"))
            .limit(10)
        )

        kpis["kpi_payment_distribution"] = (
            trips.filter(F.col("payment_type").isNotNull())
            .groupBy("vehicle_type", "payment_type")
            .agg(F.count("*").alias("trip_count"))
            .orderBy("vehicle_type", "payment_type")
        )

        kpis["kpi_avg_trip_duration"] = (
            trips.filter(F.col("trip_duration_sec").isNotNull())
            .groupBy("vehicle_type")
            .agg(
                F.avg("trip_duration_sec").alias("avg_duration_sec"),
                F.count("*").alias("trip_count"),
            )
        )

        kpis["kpi_shared_ride_rate"] = (
            trips.filter(F.col("vehicle_type") == "fhvhv")
            .groupBy("vehicle_type")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum(F.when(F.col("shared_match_flag") == "Y", 1).otherwise(0)).alias("shared_trips"),
            )
            .withColumn("shared_rate", F.col("shared_trips") / F.col("total_trips"))
        )

        daily_trips = trips.groupBy("trip_date").agg(F.count("*").alias("trip_count"))
        kpis["kpi_weather_correlation"] = (
            daily_trips.join(weather, daily_trips.trip_date == weather.date, "inner")
            .groupBy("date", "temp_max", "temp_min", "precipitation_sum", "weather_code")
            .agg(F.sum("trip_count").alias("trip_count"))
            .orderBy("date")
        )

        for name, df in kpis.items():
            df.write.mode("overwrite").parquet(medallion_uri(config, "gold", name))

        metric.rows_written = len(kpis)
        return kpis
