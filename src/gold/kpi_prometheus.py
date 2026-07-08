"""Exporte les KPIs metier vers Prometheus (textfile pour node-exporter)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pymongo import MongoClient


def _f(value: Any, digits: int = 4) -> str:
    return f"{float(value):.{digits}f}"


def export_kpi_prometheus(uri: str, database: str, export_path: str | Path) -> None:
    """Ecrit les gauges taxi_* / lake_* dans logs/kpi_metrics.prom."""
    path = Path(export_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[database]
    lines: list[str] = []

    for doc in db.kpi_avg_fare_by_vehicle.find():
        vt = doc.get("vehicle_type", "unknown")
        if doc.get("avg_fare") is not None:
            lines.append(f'taxi_avg_fare{{vehicle_type="{vt}"}} {_f(doc["avg_fare"])}')
        if doc.get("trip_count") is not None:
            lines.append(f'taxi_trips_total{{vehicle_type="{vt}"}} {int(doc["trip_count"])}')

    for doc in db.kpi_monthly_trend.find():
        vt = doc.get("vehicle_type", "unknown")
        ym = doc.get("year_month", "unknown")
        if doc.get("trip_count") is not None:
            lines.append(
                f'taxi_monthly_trips{{vehicle_type="{vt}",year_month="{ym}"}} {int(doc["trip_count"])}'
            )
        if doc.get("avg_fare") is not None:
            lines.append(
                f'taxi_monthly_avg_fare{{vehicle_type="{vt}",year_month="{ym}"}} {_f(doc["avg_fare"])}'
            )

    for i, doc in enumerate(db.kpi_top_pickup_hotspots.find().sort("trip_count", -1).limit(10), 1):
        zone = str(doc.get("pu_zone_name", "unknown")).replace('"', "'")
        borough = str(doc.get("pu_borough", "unknown")).replace('"', "'")
        lines.append(
            f'taxi_top_pickup{{rank="{i}",zone="{zone}",borough="{borough}"}} {int(doc["trip_count"])}'
        )

    for i, doc in enumerate(db.kpi_top_dropoff_hotspots.find().sort("trip_count", -1).limit(10), 1):
        zone = str(doc.get("do_zone_name", "unknown")).replace('"', "'")
        borough = str(doc.get("do_borough", "unknown")).replace('"', "'")
        lines.append(
            f'taxi_top_dropoff{{rank="{i}",zone="{zone}",borough="{borough}"}} {int(doc["trip_count"])}'
        )

    for doc in db.kpi_payment_distribution.find():
        vt = doc.get("vehicle_type", "unknown")
        pt = str(doc.get("payment_type", "unknown")).replace('"', "'")
        lines.append(
            f'taxi_payment_trips{{vehicle_type="{vt}",payment_type="{pt}"}} {int(doc["trip_count"])}'
        )

    dur = db.kpi_avg_trip_duration.find_one({"vehicle_type": "fhvhv"})
    if dur and dur.get("avg_duration_sec") is not None:
        lines.append(f'taxi_avg_duration_sec{{vehicle_type="fhvhv"}} {_f(dur["avg_duration_sec"], 2)}')

    shared = db.kpi_shared_ride_rate.find_one({"vehicle_type": "fhvhv"})
    if shared and shared.get("shared_rate") is not None:
        lines.append(f'taxi_shared_ride_rate{{vehicle_type="fhvhv"}} {_f(shared["shared_rate"])}')

    for i, doc in enumerate(db.lake_top_routes.find().sort("trip_count", -1).limit(15), 1):
        pu = str(doc.get("pu_zone_name", "?")).replace('"', "'")
        do = str(doc.get("do_zone_name", "?")).replace('"', "'")
        lines.append(f'lake_top_route{{rank="{i}",from_zone="{pu}",to_zone="{do}"}} {int(doc["trip_count"])}')

    anomaly_count = db.lake_anomaly_trips.count_documents({})
    lines.append(f"lake_anomaly_trips_total {anomaly_count}")

    ml = db.lake_ml_duration_model.find_one()
    if ml and ml.get("mae_seconds") is not None:
        lines.append(f"lake_ml_mae_seconds {_f(ml['mae_seconds'], 2)}")
    if ml and ml.get("r2_score") is not None:
        lines.append(f"lake_ml_r2_score {_f(ml['r2_score'])}")

    lines.append(f'mongo_kpi_collections {len([c for c in db.list_collection_names() if c.startswith("kpi_")])}')
    lines.append(f'mongo_lake_collections {len([c for c in db.list_collection_names() if c.startswith("lake_")])}')

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    client.close()
