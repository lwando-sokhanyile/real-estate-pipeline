from pyspark.sql import SparkSession

from src.ingest import ingest_price_paid
from src.silver import find_invalid_rows, standardize_text

INPUT_PATH = "data/raw/price_paid_2025.csv"


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
        valid_rows = total_rows - invalid_rows

        print(f"Total rows: {total_rows}")
        print(f"Valid rows: {valid_rows}")
        print(f"Invalid rows: {invalid_rows}")

        if invalid_rows > 0:
            print("Invalid row sample:")
            invalid_df.show(10, truncate=False)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()