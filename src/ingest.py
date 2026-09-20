from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    DateType,
)


spark = (
    SparkSession.builder
    .appName("real-estate-ingestion")
    .getOrCreate()
)

schema = StructType([
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

df = (
    spark.read
    .option("header", "false")
    .option("quote", '"')
    .schema(schema)
    .csv("data/raw/price_paid_2025.csv")
)

print("Rows:", df.count())

print("\nSchema:")
df.printSchema()

print("\nSample:")
df.show(5, truncate=False)

spark.stop()