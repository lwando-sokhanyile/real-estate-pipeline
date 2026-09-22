import os

from pyspark.sql import SparkSession

from src.load_postgres import load_to_postgres


INPUT_PATH = "data/silver/price_paid"
JDBC_JAR = "lib/postgresql-42.7.8.jar"


def main() -> None:
    postgres_user = os.environ["POSTGRES_USER"]
    postgres_password = os.environ["POSTGRES_PASSWORD"]

    spark = (
        SparkSession.builder
        .appName("RealEstatePostgresLoad")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )

    try:
        load_to_postgres(
            spark=spark,
            input_path=INPUT_PATH,
            jdbc_user=postgres_user,
            jdbc_password=postgres_password,
        )

        print("PostgreSQL load completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()