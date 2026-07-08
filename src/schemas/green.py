"""Green taxi schema helpers."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GREEN_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("trip_type", IntegerType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("cbd_congestion_fee", DoubleType(), True),
    ]
)

PICKUP_COL = "lpep_pickup_datetime"
DROPOFF_COL = "lpep_dropoff_datetime"
DISTANCE_COL = "trip_distance"
