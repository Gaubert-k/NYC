"""Charge Silver une seule fois pour toute la couche Lake."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from src.transform.silver_cleaner import read_silver_trips


def load_lake_trips(spark: SparkSession, config: dict[str, Any]) -> tuple[DataFrame, int]:
    """Lit Silver une fois et persiste (evite 4-5 lectures HDFS)."""
    trips = read_silver_trips(spark, config)
    if config.get("light_mode"):
        trips = trips.filter(F.col("vehicle_type") == "green")
    trips = trips.persist(StorageLevel.DISK_ONLY)
    return trips, 0


def release_lake_trips(trips: DataFrame) -> None:
    trips.unpersist()
