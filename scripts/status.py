"""Tableau de bord : état du lakehouse (Docker, HDFS, MongoDB, données locales)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pymongo import MongoClient

from src.common.config import load_config, path


def _docker_containers() -> list[tuple[str, str]]:
    try:
        out = subprocess.check_output(
            ["docker", "ps", "--filter", "network=nyc-data-net", "--format", "{{.Names}}|{{.Status}}"],
            text=True,
            timeout=10,
        )
        rows = []
        for line in out.strip().splitlines():
            if "|" in line:
                name, status = line.split("|", 1)
                rows.append((name, status))
        return rows
    except Exception:
        return []


def _hdfs_layers() -> dict[str, int]:
    try:
        out = subprocess.check_output(
            ["docker", "exec", "nyc-namenode", "hdfs", "dfs", "-ls", "/data"],
            text=True,
            timeout=15,
            stderr=subprocess.DEVNULL,
        )
        layers = {}
        for line in out.strip().splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 8 and parts[-1].startswith("/data/"):
                layer = parts[-1].split("/")[-1]
                layers[layer] = 1
        return layers
    except Exception:
        return {}


def _local_layers() -> dict[str, str]:
    result = {}
    for layer in ("bronze", "silver", "gold", "lake"):
        p = path(load_config(), layer)
        if p.exists():
            count = sum(1 for _ in p.rglob("*") if _.is_file())
            result[layer] = f"{count} fichiers"
    return result


def _mongo_summary(uri: str, database: str) -> dict[str, int] | None:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        db = client[database]
        summary = {c: db[c].count_documents({}) for c in sorted(db.list_collection_names())}
        client.close()
        return summary
    except Exception:
        return None


def _last_metrics(metrics_path: Path) -> list[dict]:
    if not metrics_path.exists():
        return []
    lines = metrics_path.read_text(encoding="utf-8").strip().splitlines()
    records = []
    for line in lines[-8:]:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def main() -> None:
    config = load_config()
    print("=" * 60)
    print("  NYC TAXI DATA LAKEHOUSE — STATUS")
    print("=" * 60)

    containers = _docker_containers()
    print(f"\n[Docker] {len(containers)} conteneur(s) actif(s)")
    for name, status in containers:
        print(f"  {name}: {status}")

    hdfs = _hdfs_layers()
    if hdfs:
        print(f"\n[HDFS] Couches presentes: {', '.join(sorted(hdfs))}")
        print("  UI: http://localhost:9870")
    else:
        print("\n[HDFS] Non disponible")

    local = _local_layers()
    print("\n[Local data/]")
    for layer, info in local.items():
        print(f"  {layer}: {info}")

    mongo = _mongo_summary(config["mongo_uri"], config["mongo_database"])
    if mongo:
        kpi = {k: v for k, v in mongo.items() if k.startswith("kpi_")}
        lake = {k: v for k, v in mongo.items() if k.startswith("lake_")}
        print(f"\n[MongoDB] {config['mongo_database']} — {len(mongo)} collections")
        print(f"  KPIs warehouse: {len(kpi)} collections, {sum(kpi.values())} docs")
        print(f"  Lake analytics: {len(lake)} collections, {sum(lake.values())} docs")
    else:
        print(f"\n[MongoDB] Indisponible ({config['mongo_uri']})")

    metrics = _last_metrics(Path(config["metrics_log_path"]))
    if metrics:
        print(f"\n[Dernieres metriques pipeline] ({config['metrics_log_path']})")
        for rec in metrics[-5:]:
            op = rec.get("operation", "?")
            layer = rec.get("layer", "?")
            dur = rec.get("duration_ms", 0)
            rw = rec.get("rows_written", 0)
            status = rec.get("status", "?")
            print(f"  [{layer}] {op}: {status}, {rw} rows, {dur} ms")

    print("\n[Dashboards Grafana] http://localhost:3000 (admin/admin)")
    print("  Dossier NYC Lakehouse :")
    print("    01 — Pipeline (technique)   : perf, qualite, volumes")
    print("    02 — KPIs metier            : tarifs, zones, tendances")
    print("    03 — Lake                   : ML, geo, anomalies")
    print("\n[Autres URLs]")
    urls = {
        "Spark Master": "http://localhost:8080",
        "Spark History": "http://localhost:18080",
        "Prometheus": "http://localhost:9090",
        "HDFS": "http://localhost:9870",
    }
    for label, url in urls.items():
        print(f"  {label}: {url}")
    print()


if __name__ == "__main__":
    main()
