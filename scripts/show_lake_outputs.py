"""Affiche un résumé des outputs Lake (géo, ML, exploration)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.config import load_config, path


def main():
    config = load_config()
    lake = path(config, "lake")

    print("=== LAKE ANALYTICS (hors warehouse) ===\n")

    ml_metrics = lake / "ml" / "duration_model_metrics.json"
    if ml_metrics.exists():
        print("--- ML : prédiction durée trajet ---")
        print(ml_metrics.read_text(encoding="utf-8"))

    schema = lake / "exploration" / "schema_drift_report.json"
    if schema.exists():
        print("\n--- Exploration : schema drift ---")
        data = json.loads(schema.read_text(encoding="utf-8"))
        for vtype, info in data.get("vehicle_types", {}).items():
            print(f"  {vtype}: {info['files_analyzed']} fichier(s), {len(info['union_columns'])} colonnes union")

    for label, subpath in [
        ("Géo O-D flows", "geospatial/od_zone_flows"),
        ("Géo heatmap", "geospatial/zone_heatmap"),
        ("Top routes", "geospatial/top_routes"),
        ("Anomalies", "ml/anomaly_trips"),
        ("Trafic × taxi", "exploration/traffic_taxi_correlation"),
    ]:
        p = lake / subpath
        if p.exists():
            print(f"\n--- {label} ---")
            print(f"  -> {p}")

    print("\n(Warehouse KPIs séparés dans data/gold/)")


if __name__ == "__main__":
    main()
