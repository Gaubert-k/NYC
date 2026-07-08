"""Gold layer: load KPIs into MongoDB."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pymongo import MongoClient

from src.common.metrics import MetricsTracker


def _df_to_docs(df: DataFrame) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in df.collect()]


def _mongo_available(uri: str) -> bool:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


def load_kpis_to_mongo(
    kpis: dict[str, DataFrame],
    config: dict[str, Any],
    metrics: MetricsTracker,
) -> bool:
    uri = config["mongo_uri"]
    db_name = config["mongo_database"]

    if not _mongo_available(uri):
        return False

    client = MongoClient(uri)
    db = client[db_name]

    with metrics.track("gold", "mongo_load") as metric:
        total_written = 0
        for collection_name, df in kpis.items():
            docs = _df_to_docs(df)
            collection = db[collection_name]
            collection.delete_many({})

            if docs:
                collection.insert_many(docs)
                total_written += len(docs)

            _ensure_indexes(collection, collection_name)

        metric.rows_written = total_written

    client.close()
    return True


def _ensure_indexes(collection: Any, collection_name: str) -> None:
    index_map = {
        "kpi_trips_by_zone_hour": [("pu_location_id", 1), ("pickup_hour", 1)],
        "kpi_trips_by_zone_day": [("pu_location_id", 1), ("pickup_dow", 1)],
        "kpi_avg_fare_by_vehicle": [("vehicle_type", 1)],
        "kpi_avg_fare_by_hour": [("vehicle_type", 1), ("pickup_hour", 1)],
        "kpi_avg_distance": [("vehicle_type", 1), ("pu_location_id", 1)],
        "kpi_monthly_trend": [("vehicle_type", 1), ("year_month", 1)],
        "kpi_top_pickup_hotspots": [("trip_count", -1)],
        "kpi_top_dropoff_hotspots": [("trip_count", -1)],
        "kpi_payment_distribution": [("vehicle_type", 1), ("payment_type", 1)],
        "kpi_avg_trip_duration": [("vehicle_type", 1)],
        "kpi_shared_ride_rate": [("vehicle_type", 1)],
        "kpi_weather_correlation": [("date", 1)],
    }
    keys = index_map.get(collection_name)
    if keys:
        collection.create_index(keys)
