import os

from pyspark.sql import SparkSession

from src.ingest import ingest_price_paid
from src.silver import (
    find_invalid_rows,
    standardize_text,
    write_silver,
)

INPUT_PATH = os.getenv(
    "REAL_ESTATE_INPUT",
    "data/pp-complete.csv",
)
OUTPUT_PATH = "data/silver/price_paid"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("RealEstateSilverTest")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = ingest_price_paid(spark, INPUT_PATH)

        cleaned_df = standardize_text(df)

        invalid_df = find_invalid_rows(cleaned_df)

        total_rows = cleaned_df.count()
        invalid_rows = invalid_df.count()

        print(f"Total rows: {total_rows}")
        print(f"Invalid rows: {invalid_rows}")

        if invalid_rows > 0:
            print("Invalid row sample:")
            invalid_df.show(10, truncate=False)
            return

        write_silver(cleaned_df, OUTPUT_PATH)

        silver_df = spark.read.parquet(OUTPUT_PATH)

        silver_rows = silver_df.count()

        print(f"Silver rows: {silver_rows}")

        print("Silver schema:")
        silver_df.printSchema()

        print("Silver sample:")
        silver_df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()