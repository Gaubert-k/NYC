"""Pipeline orchestrator CLI."""

from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import load_config
from src.common.metrics import MetricsTracker
from src.common.spark_session import create_spark_session
from src.gold.kpi_calculator import compute_kpis
from src.gold.lake_mongo_loader import load_lake_to_mongo
from src.gold.mongo_loader import load_kpis_to_mongo
from src.ingest.bronze_reference import ingest_reference
from src.ingest.bronze_traffic import ingest_traffic
from src.ingest.bronze_trips import ingest_trips
from src.ingest.bronze_weather import ingest_weather
from src.lake.run_lake import run_lake as execute_lake_analytics
from src.transform.silver_cleaner import transform_silver


def _print_startup_config(config: dict, args: argparse.Namespace) -> None:
    spark = config.get("spark", {})
    filtres = []
    if config.get("sample_year"):
        filtres.append(f"annee={config['sample_year']}")
    if config.get("sample_month"):
        filtres.append(f"mois={config['sample_month']}")
    if config.get("sample_mode"):
        filtres.append("sample_mode=ON")
    if config.get("light_mode"):
        filtres.append("light_mode=ON")
    if args.vehicle_type:
        filtres.append(f"vehicle={args.vehicle_type}")

    print("=" * 60)
    print("  NYC TAXI LAKEHOUSE - CONFIGURATION")
    print("=" * 60)
    print(f"  Couche          : {args.layer}")
    print(f"  Mode Docker     : {config.get('docker_mode')}")
    print(f"  Stockage        : {config.get('storage_backend')}")
    print(f"  HDFS            : {config.get('hdfs_uri')}")
    print(f"  Filtres         : {', '.join(filtres) if filtres else 'aucun (dataset complet)'}")
    print(f"  Types vehicules : {', '.join(config.get('vehicle_types', []))}")
    print("  --- Spark ---")
    print(f"  Master          : {spark.get('master')}")
    print(f"  Driver memory   : {spark.get('driver_memory')}")
    print(f"  Executor memory : {spark.get('executor_memory')}")
    print(f"  Executor cores  : {spark.get('executor_cores', 2)}")
    print(f"  Executors       : {config.get('spark_num_executors')}")
    print(f"  Shuffle parts   : {spark.get('shuffle_partitions')}")
    print("  --- MongoDB ---")
    print(f"  URI             : {config.get('mongo_uri')}")
    print(f"  Database        : {config.get('mongo_database')}")
    print("=" * 60)


def run_bronze(spark, config, metrics, vehicle_type=None):
    ingest_reference(spark, config, metrics)
    ingest_weather(spark, config, metrics)
    ingest_traffic(spark, config, metrics)
    types = [vehicle_type] if vehicle_type else config.get("vehicle_types", [])
    for vtype in types:
        ingest_trips(spark, config, metrics, vehicle_type=vtype)
        gc.collect()


def run_silver(spark, config, metrics, vehicle_type=None):
    transform_silver(spark, config, metrics, vehicle_type=vehicle_type)
    gc.collect()


def run_gold(spark, config, metrics):
    kpis = compute_kpis(spark, config, metrics)
    load_kpis_to_mongo(kpis, config, metrics)
    gc.collect()


def run_lake_layer(spark, config, metrics):
    from pathlib import Path

    from src.gold.kpi_prometheus import export_kpi_prometheus

    execute_lake_analytics(spark, config, metrics)
    if load_lake_to_mongo(spark, config, metrics):
        prom_path = Path(config.get("prometheus_export_path", "logs/pipeline_metrics.prom")).parent / "kpi_metrics.prom"
        export_kpi_prometheus(config["mongo_uri"], config["mongo_database"], prom_path)
    gc.collect()


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Data Lakehouse Pipeline")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "lake", "all"],
        default="all",
        help="Couche à exécuter (lake = analytics ML/géo hors warehouse)",
    )
    parser.add_argument(
        "--vehicle-type",
        choices=["yellow", "green", "fhv", "fhvhv"],
        default=None,
        help="Limiter à un type de véhicule",
    )
    parser.add_argument("--sample", action="store_true", help="Activer SAMPLE_MODE")
    parser.add_argument(
        "--year",
        metavar="YYYY",
        default=None,
        help="Limiter a une annee (ex: 2026)",
    )
    parser.add_argument(
        "--month",
        metavar="YYYY-MM",
        default=None,
        help="Limiter a un mois precis (ex: 2026-01)",
    )
    parser.add_argument("--light", action="store_true", help="Mode leger (~1 Go RAM)")
    parser.add_argument("--num-executors", type=int, default=None, help="Nombre d'executors Spark")
    args = parser.parse_args()

    config = load_config()
    if args.sample:
        config["sample_mode"] = True
    if args.month:
        config["sample_month"] = args.month
        config["sample_mode"] = True
        config["max_months_per_type"] = 1
    if args.year:
        config["sample_year"] = args.year
    if args.light:
        config["light_mode"] = True
        config["sample_mode"] = True
        config["max_months_per_type"] = 1

    metrics = MetricsTracker(config["metrics_log_path"], config["prometheus_export_path"])
    MetricsTracker.rebuild_prometheus_from_jsonl(
        config["metrics_log_path"], config["prometheus_export_path"]
    )
    _print_startup_config(config, args)
    spark = create_spark_session(config, num_executors=args.num_executors)

    try:
        if args.layer in ("bronze", "all"):
            run_bronze(spark, config, metrics, args.vehicle_type)

        if args.layer in ("silver", "all"):
            run_silver(spark, config, metrics, args.vehicle_type)

        if args.layer in ("gold", "all"):
            run_gold(spark, config, metrics)

        if args.layer in ("lake", "all"):
            run_lake_layer(spark, config, metrics)
    finally:
        MetricsTracker.rebuild_prometheus_from_jsonl(
            config["metrics_log_path"], config["prometheus_export_path"]
        )
        spark.stop()


if __name__ == "__main__":
    main()
