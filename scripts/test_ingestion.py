from pathlib import Path

from pyspark.sql import SparkSession
from src.ingest import ingest_price_paid

INPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "price_paid_sample.csv"
)


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("real-estate-ingestion-test")
        .getOrCreate()
    )

    try:
        df = ingest_price_paid(spark, str(INPUT_PATH))

        row_count = df.count()
        assert row_count == 5, f"Expected 5 rows, got {row_count}"
        print(f"PASS: All {row_count} sample transactions loaded")

        column_count = len(df.columns)
        assert column_count == 16, f"Expected 16 columns, got {column_count}"
        print(f"PASS: All {column_count} columns are present")

        null_count = df.filter("transaction_id IS NULL").count()
        assert null_count == 0, f"Found {null_count} null IDs"
        print(f"PASS: No null transaction IDs ({null_count} found)")

        df.printSchema()
        df.show(5, truncate=False)

        print("\nSUCCESS: All ingestion tests passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()