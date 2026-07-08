"""FHV schema helpers."""

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

FHV_SCHEMA = StructType(
    [
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropOff_datetime", TimestampType(), True),
        StructField("PUlocationID", IntegerType(), True),
        StructField("DOlocationID", IntegerType(), True),
        StructField("SR_Flag", StringType(), True),
        StructField("Affiliated_base_number", StringType(), True),
    ]
)

PICKUP_COL = "pickup_datetime"
DROPOFF_COL = "dropOff_datetime"
DISTANCE_COL = None
