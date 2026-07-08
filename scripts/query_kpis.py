"""Query KPIs from MongoDB or Parquet fallback."""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pymongo import MongoClient

from src.common.config import load_config, path


COLLECTIONS = [
    "kpi_trips_by_zone_hour",
    "kpi_trips_by_zone_day",
    "kpi_avg_fare_by_vehicle",
    "kpi_avg_fare_by_hour",
    "kpi_avg_distance",
    "kpi_monthly_trend",
    "kpi_top_pickup_hotspots",
    "kpi_top_dropoff_hotspots",
    "kpi_payment_distribution",
    "kpi_avg_trip_duration",
    "kpi_shared_ride_rate",
    "kpi_weather_correlation",
]


def query_mongo(config: dict) -> None:
    client = MongoClient(config["mongo_uri"], serverSelectionTimeoutMS=3000)
    db = client[config["mongo_database"]]

    for name in COLLECTIONS:
        start = time.perf_counter()
        docs = list(db[name].find({}, {"_id": 0}).limit(5))
        elapsed = (time.perf_counter() - start) * 1000
        print(f"\n--- {name} ({elapsed:.1f} ms, {db[name].count_documents({})} docs) ---")
        for doc in docs:
            print(doc)

    client.close()


def query_parquet_fallback(config: dict) -> None:
    from src.common.spark_session import create_spark_session

    spark = create_spark_session(config)
    gold_dir = path(config, "gold")

    for name in COLLECTIONS:
        p = gold_dir / name
        if not p.exists():
            print(f"\n--- {name} : absent ---")
            continue
        start = time.perf_counter()
        df = spark.read.parquet(str(p))
        count = df.count()
        elapsed = (time.perf_counter() - start) * 1000
        print(f"\n--- {name} ({elapsed:.1f} ms, {count} rows) ---")
        df.show(5, truncate=False)

    spark.stop()


def main():
    config = load_config()
    try:
        client = MongoClient(config["mongo_uri"], serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        client.close()
        query_mongo(config)
    except Exception:
        print("MongoDB indisponible — lecture depuis data/gold/ Parquet")
        query_parquet_fallback(config)


if __name__ == "__main__":
    main()
