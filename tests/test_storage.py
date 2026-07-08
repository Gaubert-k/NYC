"""Tests unitaires pour l'abstraction stockage."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.config import load_config
from src.common.storage import local_uri, medallion_uri, uses_hdfs


@pytest.fixture
def base_config():
    config = load_config()
    config["paths"] = {
        "raw": str(ROOT / "data" / "raw"),
        "reference": str(ROOT / "data" / "reference"),
        "bronze": str(ROOT / "data" / "bronze"),
        "silver": str(ROOT / "data" / "silver"),
        "gold": str(ROOT / "data" / "gold"),
        "lake": str(ROOT / "data" / "lake"),
    }
    config["storage_backend"] = "local"
    config["hdfs_uri"] = "hdfs://namenode:9000"
    return config


def test_uses_hdfs_local(base_config):
    assert uses_hdfs(base_config) is False
    base_config["storage_backend"] = "hdfs"
    assert uses_hdfs(base_config) is True


def test_medallion_uri_local(base_config):
    uri = medallion_uri(base_config, "silver", "trips_unified")
    assert uri.endswith("data\\silver\\trips_unified") or uri.endswith("data/silver/trips_unified")


def test_medallion_uri_hdfs(base_config):
    base_config["storage_backend"] = "hdfs"
    uri = medallion_uri(base_config, "gold", "kpi_monthly_trend")
    assert uri == "hdfs://namenode:9000/data/gold/kpi_monthly_trend"


def test_local_uri_file_scheme(base_config):
    uri = local_uri(base_config, "reference", "taxi_zone_lookup.csv")
    assert uri.startswith("file://")
    assert "taxi_zone_lookup.csv" in uri


def test_docker_mode_storage_default(monkeypatch):
    monkeypatch.setenv("DOCKER_MODE", "true")
    monkeypatch.delenv("STORAGE_BACKEND", raising=False)
    config = load_config()
    assert config["docker_mode"] is True
    assert config["storage_backend"] == "hdfs"
