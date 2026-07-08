"""Schema registry for all vehicle types."""

from __future__ import annotations

from pyspark.sql.types import StructType

from src.schemas import fhv, fhvhv, green, yellow

SCHEMAS: dict[str, StructType] = {
    "yellow": yellow.YELLOW_SCHEMA,
    "green": green.GREEN_SCHEMA,
    "fhv": fhv.FHV_SCHEMA,
    "fhvhv": fhvhv.FHVHV_SCHEMA,
}

PICKUP_COLUMNS: dict[str, str] = {
    "yellow": yellow.PICKUP_COL,
    "green": green.PICKUP_COL,
    "fhv": fhv.PICKUP_COL,
    "fhvhv": fhvhv.PICKUP_COL,
}

DROPOFF_COLUMNS: dict[str, str] = {
    "yellow": yellow.DROPOFF_COL,
    "green": green.DROPOFF_COL,
    "fhv": fhv.DROPOFF_COL,
    "fhvhv": fhvhv.DROPOFF_COL,
}

DISTANCE_COLUMNS: dict[str, str | None] = {
    "yellow": yellow.DISTANCE_COL,
    "green": green.DISTANCE_COL,
    "fhv": fhv.DISTANCE_COL,
    "fhvhv": fhvhv.DISTANCE_COL,
}

PU_LOCATION_COLUMNS: dict[str, str] = {
    "yellow": "PULocationID",
    "green": "PULocationID",
    "fhv": "PUlocationID",
    "fhvhv": "PULocationID",
}

DO_LOCATION_COLUMNS: dict[str, str] = {
    "yellow": "DOLocationID",
    "green": "DOLocationID",
    "fhv": "DOlocationID",
    "fhvhv": "DOLocationID",
}
