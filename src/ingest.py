from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RealEstateIngestion") \
    .getOrCreate()

# Read raw CSV data
df = spark.read.csv(
    "data/raw/housing_test.csv",
    header=True,
    inferSchema=True
)

# Display basic info
print("=== Schema ===")
df.printSchema()

print(f"\n=== Row Count ===")
print(f"Total rows: {df.count()}")

print("\n=== Sample Data ===")
df.show(5)

spark.stop()