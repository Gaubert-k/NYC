"""Configuration loader: YAML + .env."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _ROOT / "config" / "config.yaml"


def get_project_root() -> Path:
    env_root = os.getenv("PROJECT_ROOT")
    if env_root:
        return Path(env_root).resolve()
    return _ROOT


def load_config() -> dict[str, Any]:
    load_dotenv(get_project_root() / ".env")
    load_dotenv(get_project_root() / "config" / ".env")

    with open(_CONFIG_PATH, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    root = get_project_root()
    for key, rel in config.get("paths", {}).items():
        config["paths"][key] = str(root / rel)

    config["sample_mode"] = os.getenv("SAMPLE_MODE", "false").lower() == "true"
    config["max_months_per_type"] = int(os.getenv("MAX_MONTHS_PER_TYPE", "2"))
    sample_month = os.getenv("SAMPLE_MONTH", "").strip()
    config["sample_month"] = sample_month or None
    sample_year = os.getenv("SAMPLE_YEAR", "").strip()
    config["sample_year"] = sample_year or None
    config["mongo_uri"] = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    config["mongo_database"] = os.getenv(
        "MONGO_DATABASE", config.get("mongodb", {}).get("database", "nyc_taxi_warehouse")
    )
    config["gold_parquet_fallback"] = (
        os.getenv("GOLD_PARQUET_FALLBACK", "true").lower() == "true"
    )
    config["metrics_log_path"] = os.getenv(
        "METRICS_LOG_PATH", str(root / "logs" / "pipeline_metrics.jsonl")
    )
    config["prometheus_export_path"] = os.getenv(
        "PROMETHEUS_EXPORT_PATH", str(root / "logs" / "pipeline_metrics.prom")
    )
    config["spark_num_executors"] = int(os.getenv("SPARK_NUM_EXECUTORS", "2"))
    config["light_mode"] = os.getenv("LIGHT_MODE", "false").lower() == "true"
    config["docker_mode"] = os.getenv("DOCKER_MODE", "false").lower() == "true"
    config["storage_backend"] = os.getenv(
        "STORAGE_BACKEND", "hdfs" if os.getenv("DOCKER_MODE", "false").lower() == "true" else "local"
    )
    config["hdfs_uri"] = os.getenv("HDFS_URI", "hdfs://namenode:9000")

    spark_cfg = config.setdefault("spark", {})
    if config["light_mode"]:
        light = config.get("spark_light", {})
        spark_cfg["driver_memory"] = os.getenv("SPARK_DRIVER_MEMORY", light.get("driver_memory", "512m"))
        spark_cfg["executor_memory"] = os.getenv("SPARK_EXECUTOR_MEMORY", light.get("executor_memory", "512m"))
        spark_cfg["shuffle_partitions"] = light.get("shuffle_partitions", 4)
        spark_cfg["master"] = light.get("master", "local[1]")
        config["spark_num_executors"] = 1
    elif config["docker_mode"]:
        spark_cfg["master"] = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        config["mongo_uri"] = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
        spark_cfg["driver_memory"] = os.getenv("SPARK_DRIVER_MEMORY", "1g")
        spark_cfg["executor_memory"] = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
        spark_cfg["executor_cores"] = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
        spark_cfg["shuffle_partitions"] = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        config["spark_num_executors"] = int(os.getenv("SPARK_NUM_EXECUTORS", "2"))
    else:
        spark_cfg["driver_memory"] = os.getenv("SPARK_DRIVER_MEMORY", spark_cfg.get("driver_memory", "4g"))
        spark_cfg["executor_memory"] = os.getenv(
            "SPARK_EXECUTOR_MEMORY", spark_cfg.get("executor_memory", "4g")
        )
    return config


def path(config: dict[str, Any], name: str) -> Path:
    return Path(config["paths"][name])
