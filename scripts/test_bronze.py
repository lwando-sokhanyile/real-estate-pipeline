import os

from pyspark.sql import SparkSession

from src.bronze import write_bronze
from src.ingest import ingest_price_paid


INPUT_PATH = os.getenv(
    "REAL_ESTATE_INPUT",
    "data/pp-complete.csv",
)
OUTPUT_PATH = "data/bronze/price_paid"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("RealEstateBronzeTest")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = ingest_price_paid(spark, INPUT_PATH)

        print(f"Input rows: {df.count()}")

        write_bronze(df, OUTPUT_PATH)

        bronze_df = spark.read.parquet(OUTPUT_PATH)

        print(f"Bronze rows: {bronze_df.count()}")

        bronze_df.printSchema()

        print("Bronze sample:")
        bronze_df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()