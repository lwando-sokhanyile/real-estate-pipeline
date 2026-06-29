import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

# Test S3 connection
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

try:
    s3.list_objects_v2(Bucket="real-estate-pipeline-lwando")
    print("S3 connection successful")
except Exception as e:
    print(f"S3 connection failed: {e}")
    exit(1)

# Spark session for reading and partitioning
spark = SparkSession.builder \
    .appName("RealEstateS3Load") \
    .getOrCreate()

# Read cleaned parquet
df = spark.read.parquet("data/processed/housing_cleaned")
print(f"Total rows: {df.count()}")

# Write partitioned parquet locally first
LOCAL_OUTPUT = "data/processed/housing_partitioned"

df.write \
    .mode("overwrite") \
    .partitionBy("Neighborhood") \
    .parquet(LOCAL_OUTPUT)

print("Partitioned Parquet written locally")

spark.stop()

# Upload all parquet files to S3 using boto3
BUCKET = "real-estate-pipeline-lwando"
uploaded = 0

for root, dirs, files in os.walk(LOCAL_OUTPUT):
    for file in files:
        if file.endswith(".parquet"):
            local_path = os.path.join(root, file)
            s3_key = local_path.replace("\\", "/").replace("data/processed/", "processed/")
            s3.upload_file(local_path, BUCKET, s3_key)
            print(f"Uploaded: {s3_key}")
            uploaded += 1

print(f"\nTotal files uploaded to S3: {uploaded}")
print("Data successfully written to S3 with partitioning by Neighborhood")