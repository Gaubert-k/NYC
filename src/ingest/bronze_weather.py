"""Bronze layer: external weather data via Open-Meteo API."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import path
from src.common.metrics import MetricsTracker
from src.common.storage import ensure_local_dir, local_path, medallion_uri


def _date_range_from_raw(config: dict[str, Any]) -> tuple[str, str]:
    sample_month = config.get("sample_month")
    if sample_month:
        year, month = sample_month.split("-")
        import calendar

        last_day = calendar.monthrange(int(year), int(month))[1]
        return f"{sample_month}-01", f"{sample_month}-{last_day:02d}"

    raw_dir = local_path(config, "raw")
    dates: list[str] = []
    for vtype in config.get("vehicle_types", []):
        for f in (raw_dir / vtype).glob("*.parquet"):
            part = f.stem.split("_")[-1]
            if len(part) == 7:
                dates.append(part + "-01")
    if not dates:
        return "2025-04-01", "2025-05-31"
    dates.sort()
    return dates[0][:8] + "01", dates[-1][:8] + "28"


def fetch_weather(config: dict[str, Any]) -> dict[str, Any]:
    weather_cfg = config.get("weather", {})
    start_date, end_date = _date_range_from_raw(config)
    params = {
        "latitude": weather_cfg.get("latitude", 40.7128),
        "longitude": weather_cfg.get("longitude", -74.0060),
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode",
        "timezone": weather_cfg.get("timezone", "America/New_York"),
    }
    url = weather_cfg.get("api_url")
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=90)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Open-Meteo API indisponible après 3 tentatives: {last_error}")


def _fallback_weather_payload(start_date: str, end_date: str) -> dict[str, Any]:
    """Données météo minimales si l'API externe échoue."""
    from datetime import date, timedelta

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    days: list[str] = []
    temps_max: list[float] = []
    temps_min: list[float] = []
    precip: list[float] = []
    codes: list[int] = []
    current = start
    while current <= end:
        days.append(current.isoformat())
        temps_max.append(22.0)
        temps_min.append(12.0)
        precip.append(0.0)
        codes.append(0)
        current += timedelta(days=1)
    return {
        "daily": {
            "time": days,
            "temperature_2m_max": temps_max,
            "temperature_2m_min": temps_min,
            "precipitation_sum": precip,
            "weathercode": codes,
        },
        "_fallback": True,
    }


def ingest_weather(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    bronze_weather = ensure_local_dir(config, "bronze", "weather")
    start_date, end_date = _date_range_from_raw(config)

    with metrics.track("bronze", "ingest_weather_api", source="open-meteo") as metric:
        try:
            payload = fetch_weather(config)
        except RuntimeError:
            payload = _fallback_weather_payload(start_date, end_date)
            metric.extra["fallback"] = True

        raw_json_path = bronze_weather / "weather_raw.json"
        raw_json_path.write_text(json.dumps(payload), encoding="utf-8")

        daily = payload.get("daily", {})
        dates = daily.get("time", [])
        rows = [
            {
                "date": dates[i],
                "temp_max": daily.get("temperature_2m_max", [None])[i],
                "temp_min": daily.get("temperature_2m_min", [None])[i],
                "precipitation_sum": daily.get("precipitation_sum", [None])[i],
                "weather_code": daily.get("weathercode", [None])[i],
            }
            for i in range(len(dates))
        ]
        metric.rows_read = len(rows)

        df = (
            spark.createDataFrame(rows)
            .withColumn("_ingestion_ts", F.lit(datetime.now(timezone.utc)))
            .withColumn("_source", F.lit("open-meteo"))
        )
        df.write.mode("overwrite").parquet(medallion_uri(config, "bronze", "weather", "daily"))
        metric.rows_written = len(rows)
