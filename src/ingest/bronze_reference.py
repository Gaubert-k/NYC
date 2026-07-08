"""Bronze layer: reference data (zones CSV + image manifest)."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.common.config import path
from src.common.metrics import MetricsTracker
from src.common.storage import ensure_local_dir, local_path, local_uri, medallion_uri


def _file_hash(file_path: Path) -> str:
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_reference(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    ref_dir = local_path(config, "reference")
    if config.get("storage_backend") != "hdfs":
        ensure_local_dir(config, "bronze", "reference")
        ensure_local_dir(config, "bronze", "unstructured")

    with metrics.track("bronze", "ingest_reference_csv") as metric:
        zones_csv = ref_dir / "taxi_zone_lookup.csv"
        zones_df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(local_uri(config, "reference", "taxi_zone_lookup.csv"))
            .withColumn("_ingestion_ts", F.lit(datetime.now(timezone.utc)))
            .withColumn("_source_file", F.lit(zones_csv.name))
        )
        metric.rows_read = zones_df.count()
        zones_df.write.mode("overwrite").parquet(medallion_uri(config, "bronze", "reference", "taxi_zones"))
        metric.rows_written = metric.rows_read

    with metrics.track("bronze", "ingest_images_manifest") as metric:
        maps_dir = ref_dir / "maps"
        rows = []
        for img in sorted(maps_dir.glob("*.jpg")):
            borough = img.stem.replace("taxi_zone_map_", "")
            rows.append(
                {
                    "file_name": img.name,
                    "file_path": str(img),
                    "borough": borough,
                    "file_size_bytes": img.stat().st_size,
                    "content_hash": _file_hash(img),
                    "media_type": "image/jpeg",
                }
            )
        metric.rows_read = len(rows)

        schema = StructType(
            [
                StructField("file_name", StringType(), False),
                StructField("file_path", StringType(), False),
                StructField("borough", StringType(), True),
                StructField("file_size_bytes", LongType(), True),
                StructField("content_hash", StringType(), True),
                StructField("media_type", StringType(), True),
            ]
        )
        manifest_df = spark.createDataFrame(rows, schema=schema).withColumn(
            "_ingestion_ts", F.lit(datetime.now(timezone.utc))
        )
        manifest_df.write.mode("overwrite").parquet(
            medallion_uri(config, "bronze", "unstructured", "images_manifest")
        )
        metric.rows_written = len(rows)
