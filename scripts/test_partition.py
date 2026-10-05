import os
from pyspark.sql import SparkSession
from src.load_postgres import load_partition

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TestHistoricalPartition").config("spark.jars", "lib/postgresql-42.7.8.jar")
    .getOrCreate()
)

try:
    load_partition(
        spark=spark,
        silver_root="data/silver/price_paid",
        year=1995,
        month=1,
        jdbc_user=os.environ["POSTGRES_USER"],
        jdbc_password=os.environ["POSTGRES_PASSWORD"],
    )
finally:
    spark.stop()
