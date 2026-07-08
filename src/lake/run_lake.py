"""Orchestrateur couche Lake (analytics hors warehouse)."""

from __future__ import annotations

import gc
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.common.metrics import MetricsTracker
from src.ingest.bronze_traffic import ingest_traffic
from src.lake.anomalies import run_anomalies
from src.lake.exploration import run_schema_exploration
from src.lake.geospatial import run_geospatial
from src.lake.ml_analytics import run_duration_model, run_traffic_correlation
from src.lake.silver_cache import load_lake_trips, release_lake_trips


def run_lake(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    traffic_path = config["paths"]["bronze"] + "/external/traffic_collisions/records"
    if not Path(traffic_path).exists():
        print("  -> Ingestion trafic NYC Open Data (Bronze)")
        ingest_traffic(spark, config, metrics)

    trips, _ = load_lake_trips(spark, config)
    try:
        print("  -> Geospatial (flux O-D, heatmaps)")
        run_geospatial(spark, config, metrics, trips=trips)
        gc.collect()

        print("  -> Detection d'anomalies")
        run_anomalies(spark, config, metrics, trips=trips)
        gc.collect()

        print("  -> ML prediction duree (GradientBoosting)")
        run_duration_model(spark, config, metrics, trips=trips)
        gc.collect()

        print("  -> Correlation trafic NYC Open Data x taxi")
        run_traffic_correlation(spark, config, metrics, trips=trips)
        gc.collect()
    finally:
        release_lake_trips(trips)

    print("  -> Exploration schema drift Bronze")
    run_schema_exploration(spark, config, metrics)
