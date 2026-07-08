"""Requêtes Lake analytics depuis MongoDB."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pymongo import MongoClient

from src.common.config import load_config

LAKE_COLLECTIONS = [
    "lake_top_routes",
    "lake_zone_heatmap",
    "lake_anomaly_trips",
    "lake_traffic_correlation",
    "lake_ml_duration_model",
    "lake_schema_drift",
]


def main() -> None:
    config = load_config()
    client = MongoClient(config["mongo_uri"], serverSelectionTimeoutMS=3000)
    db = client[config["mongo_database"]]

    print("=== LAKE ANALYTICS (MongoDB) ===\n")
    for name in LAKE_COLLECTIONS:
        if name not in db.list_collection_names():
            print(f"--- {name} : absent ---")
            continue
        start = time.perf_counter()
        count = db[name].count_documents({})
        elapsed = (time.perf_counter() - start) * 1000
        print(f"--- {name} ({elapsed:.1f} ms, {count} docs) ---")
        if name.endswith("_model") or name.endswith("_drift"):
            doc = db[name].find_one({}, {"_id": 0})
            print(json.dumps(doc, indent=2, ensure_ascii=False)[:500])
        else:
            for doc in db[name].find({}, {"_id": 0}).limit(3):
                print(doc)
        print()

    client.close()


if __name__ == "__main__":
    main()
