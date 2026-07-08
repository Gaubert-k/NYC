"""Bronze layer: ingest raw trip Parquet files."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.metrics import MetricsTracker
from src.common.storage import local_path, medallion_uri
from src.schemas import SCHEMAS


def _list_parquet_files(
    raw_dir: Path,
    vehicle_type: str,
    sample_mode: bool,
    max_months: int,
    sample_month: str | None = None,
    sample_year: str | None = None,
) -> list[Path]:
    files = sorted((raw_dir / vehicle_type).glob("*.parquet"))
    if sample_month:
        files = [f for f in files if sample_month in f.name]
    elif sample_year:
        files = [f for f in files if f"{sample_year}-" in f.name]
    elif sample_mode:
        files = files[:max_months]
    return files


def _extract_year_month(filename: str) -> str:
    match = re.search(r"(\d{4}-\d{2})", filename)
    return match.group(1) if match else "unknown"


def ingest_trips(
    spark: SparkSession,
    config: dict[str, Any],
    metrics: MetricsTracker,
    vehicle_type: str | None = None,
) -> None:
    raw_dir = local_path(config, "raw")
    types = [vehicle_type] if vehicle_type else config.get("vehicle_types", [])

    for vtype in types:
        files = _list_parquet_files(
            raw_dir,
            vtype,
            config.get("sample_mode", False),
            config.get("max_months_per_type", 2),
            config.get("sample_month"),
            config.get("sample_year"),
        )
        if not files:
            continue

        with metrics.track("bronze", f"ingest_trips_{vtype}", vehicle_type=vtype) as metric:
            for file_path in files:
                year_month = _extract_year_month(file_path.name)
                df = spark.read.parquet(file_path.as_uri())
                metric.rows_read += df.count()

                bronze_df = (
                    df.withColumn("_ingestion_ts", F.lit(datetime.now(timezone.utc)))
                    .withColumn("_source_file", F.lit(file_path.name))
                    .withColumn("_source_type", F.lit(vtype))
                    .withColumn("year_month", F.lit(year_month))
                )

                output = medallion_uri(
                    config, "bronze", "trips", f"vehicle_type={vtype}", f"year_month={year_month}"
                )
                bronze_df.write.mode("overwrite").parquet(output)
                metric.rows_written += bronze_df.count()


def read_bronze_trips(spark: SparkSession, config: dict[str, Any], vehicle_type: str | None = None) -> DataFrame:
    if vehicle_type:
        return spark.read.parquet(medallion_uri(config, "bronze", "trips", f"vehicle_type={vehicle_type}"))
    return spark.read.parquet(medallion_uri(config, "bronze", "trips"))