from pyspark.sql import SparkSession

from src.ingest import ingest_price_paid


spark = (
    SparkSession.builder
    .appName("real-estate-ingestion-test")
    .getOrCreate()
)

df = ingest_price_paid(
    spark,
    "data/raw/pp-complete.csv",
)

print("Rows:", df.count())
df.printSchema()
df.show(5, truncate=False)

spark.stop()
