"""Abstraction chemins local vs HDFS pour Spark."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

_LAYER_MAP = {
    "raw": "raw",
    "reference": "reference",
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "lake": "lake",
    "logs": "logs",
}


def uses_hdfs(config: dict[str, Any]) -> bool:
    return config.get("storage_backend") == "hdfs"


def medallion_uri(config: dict[str, Any], layer: str, *parts: str) -> str:
    """Chemin Spark pour les couches Medallion (HDFS ou local)."""
    if uses_hdfs(config) and layer in _LAYER_MAP:
        base = config.get("hdfs_uri", "hdfs://namenode:9000").rstrip("/")
        sub = "/".join(parts) if parts else ""
        path = f"{base}/data/{_LAYER_MAP[layer]}"
        return f"{path}/{sub}" if sub else path
    p = Path(config["paths"][layer])
    for part in parts:
        p = p / part
    return str(p)


def local_uri(config: dict[str, Any], layer: str, *parts: str) -> str:
    """Lecture depuis le volume monte (raw, reference) — toujours file://."""
    p = Path(config["paths"][layer])
    for part in parts:
        p = p / part
    return p.as_uri()


def local_path(config: dict[str, Any], layer: str, *parts: str) -> Path:
    p = Path(config["paths"][layer])
    for part in parts:
        p = p / part
    return p


def ensure_local_dir(config: dict[str, Any], layer: str, *parts: str) -> Path:
    p = local_path(config, layer, *parts)
    p.mkdir(parents=True, exist_ok=True)
    return p


def hdfs_path_exists(spark: SparkSession, uri: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(uri), hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(uri))


def medallion_exists(spark: SparkSession, config: dict[str, Any], layer: str, *parts: str) -> bool:
    if not uses_hdfs(config):
        return local_path(config, layer, *parts).exists()
    return hdfs_path_exists(spark, medallion_uri(config, layer, *parts))
