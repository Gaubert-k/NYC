"""Charge les resultats Lake dans MongoDB (analytics hors warehouse)."""



from __future__ import annotations



import json

from typing import Any



from pyspark.sql import SparkSession

from pymongo import MongoClient



from src.common.metrics import MetricsTracker

from src.common.storage import local_path, medallion_uri





def _mongo_available(uri: str) -> bool:

    try:

        client = MongoClient(uri, serverSelectionTimeoutMS=3000)

        client.admin.command("ping")

        client.close()

        return True

    except Exception:

        return False





def _ensure_lake_indexes(db: Any) -> None:

    index_map = {

        "lake_top_routes": [("trip_count", -1)],

        "lake_zone_heatmap": [("pu_location_id", 1), ("pickup_hour", 1)],

        "lake_anomaly_trips": [("vehicle_type", 1), ("anomaly_reason", 1)],

        "lake_traffic_correlation": [("borough", 1), ("crash_month", 1)],

    }

    for coll_name, keys in index_map.items():

        if coll_name in db.list_collection_names():

            db[coll_name].create_index(keys)





def load_lake_to_mongo(

    spark: SparkSession,

    config: dict[str, Any],

    metrics: MetricsTracker,

) -> bool:

    uri = config["mongo_uri"]

    if not _mongo_available(uri):

        return False



    lake_dir = local_path(config, "lake")

    client = MongoClient(uri)

    db = client[config["mongo_database"]]



    with metrics.track("lake", "mongo_load") as metric:

        total = 0



        parquet_collections = {

            "lake_top_routes": medallion_uri(config, "lake", "geospatial", "top_routes"),

            "lake_zone_heatmap": medallion_uri(config, "lake", "geospatial", "zone_heatmap"),

            "lake_anomaly_trips": medallion_uri(config, "lake", "ml", "anomaly_trips"),

            "lake_traffic_correlation": medallion_uri(config, "lake", "exploration", "traffic_taxi_correlation"),

        }

        for coll_name, p in parquet_collections.items():

            try:

                df = spark.read.parquet(p)

            except Exception:

                continue

            docs = [row.asDict(recursive=True) for row in df.limit(500).collect()]

            db[coll_name].delete_many({})

            if docs:

                db[coll_name].insert_many(docs)

                total += len(docs)



        ml_metrics = lake_dir / "ml" / "duration_model_metrics.json"

        if ml_metrics.exists():

            doc = json.loads(ml_metrics.read_text(encoding="utf-8"))

            db["lake_ml_duration_model"].delete_many({})

            db["lake_ml_duration_model"].insert_one(doc)

            total += 1



        schema_report = lake_dir / "exploration" / "schema_drift_report.json"

        if schema_report.exists():

            doc = json.loads(schema_report.read_text(encoding="utf-8"))

            db["lake_schema_drift"].delete_many({})

            db["lake_schema_drift"].insert_one(doc)

            total += 1



        _ensure_lake_indexes(db)

        metric.rows_written = total



    client.close()

    return True

