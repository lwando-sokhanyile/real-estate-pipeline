from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

PRICE_PAID_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("price", DecimalType(12, 2), False),
    StructField("transfer_date", DateType(), False),
    StructField("postcode", StringType(), True),
    StructField("property_type", StringType(), False),
    StructField("old_new", StringType(), False),
    StructField("duration", StringType(), False),
    StructField("paon", StringType(), False),
    StructField("saon", StringType(), True),
    StructField("street", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("town_city", StringType(), False),
    StructField("district", StringType(), False),
    StructField("county", StringType(), False),
    StructField("ppd_category", StringType(), False),
    StructField("record_status", StringType(), False),
])

def ingest_price_paid(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """Read HM Land Registry Price Paid data into a typed DataFrame."""

    return (
        spark.read
        .option("header", "false")
        .option("quote", '"')
        .schema(PRICE_PAID_SCHEMA)
        .csv(input_path)
    )