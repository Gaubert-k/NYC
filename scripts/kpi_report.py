"""Compte rendu detaille des KPIs Gold + Lake depuis MongoDB."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pymongo import MongoClient

from src.common.config import load_config

GOLD_COLLECTIONS = [
    ("kpi_trips_by_zone_hour", "Courses par zone et heure"),
    ("kpi_trips_by_zone_day", "Courses par zone et jour"),
    ("kpi_avg_fare_by_vehicle", "Tarif moyen par type vehicule"),
    ("kpi_avg_fare_by_hour", "Tarif moyen par heure"),
    ("kpi_avg_distance", "Distance moyenne par vehicule"),
    ("kpi_monthly_trend", "Tendance mensuelle"),
    ("kpi_top_pickup_hotspots", "Top zones pickup"),
    ("kpi_top_dropoff_hotspots", "Top zones dropoff"),
    ("kpi_payment_distribution", "Repartition paiements"),
    ("kpi_avg_trip_duration", "Duree moyenne trajet"),
    ("kpi_shared_ride_rate", "Taux courses partagees (fhvhv)"),
    ("kpi_weather_correlation", "Correlation meteo"),
]

LAKE_COLLECTIONS = [
    ("lake_top_routes", "Top routes geographiques"),
    ("lake_zone_heatmap", "Heatmap zones pickup"),
    ("lake_anomaly_trips", "Anomalies detectees"),
    ("lake_traffic_correlation", "Correlation trafic NYC"),
    ("lake_ml_duration_model", "Metriques modele ML duree"),
    ("lake_schema_drift", "Rapport schema drift Bronze"),
]


def _sample_docs(coll, n: int = 3) -> list[dict]:
    return list(coll.find({}, {"_id": 0}).limit(n))


def _summarize(coll_name: str, coll) -> dict:
    count = coll.count_documents({})
    summary: dict = {"collection": coll_name, "count": count, "samples": _sample_docs(coll)}
    if count == 0:
        return summary

    first = coll.find_one({}, {"_id": 0})
    if not first:
        return summary

    keys = list(first.keys())
    summary["fields"] = keys

    if coll_name == "kpi_avg_fare_by_vehicle":
        rows = list(coll.find({}, {"_id": 0, "vehicle_type": 1, "avg_fare": 1, "trip_count": 1}))
        summary["highlights"] = rows
    elif coll_name == "kpi_monthly_trend":
        rows = list(coll.find({}, {"_id": 0}).sort("year_month", 1))
        summary["highlights"] = rows
    elif coll_name == "kpi_top_pickup_hotspots":
        rows = list(coll.find({}, {"_id": 0}).sort("trip_count", -1).limit(5))
        summary["highlights"] = rows
    elif coll_name == "lake_top_routes":
        rows = list(coll.find({}, {"_id": 0}).sort("trip_count", -1).limit(5))
        summary["highlights"] = rows
    elif coll_name == "lake_anomaly_trips":
        summary["highlights"] = _sample_docs(coll, 5)

    return summary


def build_report(config: dict) -> str:
    client = MongoClient(config["mongo_uri"], serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[config["mongo_database"]]

    lines = [
        "# Compte rendu KPI — NYC Taxi Lakehouse",
        "",
        f"**Date** : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**MongoDB** : `{config['mongo_uri']}` / `{config['mongo_database']}`",
        "",
        "## Gold — 12 KPIs warehouse",
        "",
        "| Collection | Description | Documents |",
        "|------------|-------------|-----------|",
    ]

    gold_details: list[dict] = []
    total_gold = 0
    for name, desc in GOLD_COLLECTIONS:
        coll = db[name]
        count = coll.count_documents({})
        total_gold += count
        lines.append(f"| `{name}` | {desc} | **{count:,}** |")
        gold_details.append(_summarize(name, coll))

    lines += [
        "",
        f"**Total documents Gold** : {total_gold:,}",
        "",
        "## Lake — analytics",
        "",
        "| Collection | Description | Documents |",
        "|------------|-------------|-----------|",
    ]

    lake_details: list[dict] = []
    total_lake = 0
    for name, desc in LAKE_COLLECTIONS:
        coll = db[name]
        count = coll.count_documents({})
        total_lake += count
        lines.append(f"| `{name}` | {desc} | **{count:,}** |")
        lake_details.append(_summarize(name, coll))

    lines += ["", f"**Total documents Lake** : {total_lake:,}", ""]

    for item in gold_details:
        if item.get("highlights"):
            lines += [f"### {item['collection']}", "", "```json"]
            lines.append(json.dumps(item["highlights"], indent=2, default=str))
            lines += ["```", ""]

    for item in lake_details:
        if item.get("highlights"):
            lines += [f"### {item['collection']}", "", "```json"]
            lines.append(json.dumps(item["highlights"], indent=2, default=str))
            lines += ["```", ""]

    client.close()
    return "\n".join(lines)


def main():
    config = load_config()
    report = build_report(config)
    out = ROOT / "logs" / "compte_rendu_kpi.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n>>> Sauvegarde : {out}")


if __name__ == "__main__":
    main()
