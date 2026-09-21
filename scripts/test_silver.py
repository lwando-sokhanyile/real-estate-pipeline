from pyspark.sql import SparkSession

from src.ingest import ingest_price_paid
from src.silver import standardize_text
from pyspark.sql.functions import length


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

        print("Postcode length check:")
        cleaned_df.select(
            "postcode",
        ).withColumn(
            "postcode_length",
            length("postcode")
        ).show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()