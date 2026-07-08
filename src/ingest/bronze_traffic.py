"""Bronze: NYC Open Data collisions (external structured source)."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.metrics import MetricsTracker
from src.common.storage import ensure_local_dir, medallion_uri


def fetch_collisions(config: dict[str, Any]) -> list[dict[str, Any]]:
    traffic_cfg = config.get("traffic", {})
    params = {"$limit": traffic_cfg.get("limit", 3000)}
    url = traffic_cfg.get("api_url")
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2**attempt)
    raise RuntimeError(f"NYC Open Data indisponible: {last_error}")


def _fallback_collisions() -> list[dict[str, Any]]:
    boroughs = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]
    return [
        {"borough": b, "crash_date": "2025-04-01T00:00:00.000", "number_of_persons_injured": "1"}
        for b in boroughs
    ]


def ingest_traffic(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    bronze_dir = ensure_local_dir(config, "bronze", "external", "traffic_collisions")

    with metrics.track("bronze", "ingest_traffic_nyc_open_data", source="nyc-open-data") as metric:
        try:
            records = fetch_collisions(config)
            metric.extra["live_api"] = True
        except RuntimeError:
            records = _fallback_collisions()
            metric.extra["fallback"] = True

        raw_path = bronze_dir / "collisions_raw.json"
        raw_path.write_text(json.dumps(records), encoding="utf-8")
        metric.rows_read = len(records)

        if not records:
            return

        df = spark.createDataFrame(records)
        cleaned = (
            df.withColumn("crash_ts", F.to_timestamp(F.col("crash_date")))
            .withColumn("borough", F.upper(F.trim(F.col("borough"))))
            .withColumn("crash_month", F.date_format("crash_ts", "yyyy-MM"))
            .withColumn("_ingestion_ts", F.lit(datetime.now(timezone.utc)))
            .withColumn("_source", F.lit("nyc-open-data-collisions"))
        )
        cleaned.write.mode("overwrite").parquet(
            medallion_uri(config, "bronze", "external", "traffic_collisions", "records")
        )
        metric.rows_written = cleaned.count()
