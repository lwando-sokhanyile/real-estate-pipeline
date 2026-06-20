from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("RealEstateCleaning") \
    .getOrCreate()

# Load raw data
df = spark.read.csv(
    "data/raw/housing_test.csv",
    header=True,
    inferSchema=True
)

print(f"Rows before cleaning: {df.count()}")

# Cast string columns that should be numeric
numeric_cols = [
    "LotFrontage", "MasVnrArea", "BsmtFinSF1", "BsmtFinSF2",
    "BsmtUnfSF", "TotalBsmtSF", "GarageYrBlt", "GarageCars",
    "GarageArea", "BsmtFullBath", "BsmtHalfBath"
]

for col_name in numeric_cols:
    df = df.withColumn(col_name, col(col_name).cast("double"))

# Drop duplicate rows
df = df.dropDuplicates()

# Drop rows where critical columns are null
critical_cols = ["Id", "MSSubClass", "LotArea", "YearBuilt"]
df = df.dropna(subset=critical_cols)

print(f"Rows after cleaning: {df.count()}")

print("\n=== Cleaned Schema ===")
df.printSchema()

print("\n=== Sample Cleaned Data ===")
df.show(5)

# Save cleaned data
df.write.mode("overwrite").parquet("data/processed/housing_cleaned")

print("\nCleaned data saved to data/processed/housing_cleaned")

spark.stop()