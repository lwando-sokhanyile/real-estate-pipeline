import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

spark = SparkSession.builder \
    .appName("RealEstateS3Load") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read cleaned parquet
df = spark.read.parquet("data/processed/housing_cleaned")

print(f"Total rows: {df.count()}")

# Write to S3 partitioned by Neighborhood
S3_BUCKET = os.getenv("S3_BUCKET")

df.write \
    .mode("overwrite") \
    .partitionBy("Neighborhood") \
    .parquet(f"{S3_BUCKET}/processed/housing/")

print("Data successfully written to S3 with partitioning by Neighborhood")

spark.stop()