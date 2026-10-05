import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.load_postgres import load_partition
from src.monitoring import start_run, finish_run


SILVER_ROOT = Path("data/silver/price_paid")
JDBC_JAR = "lib/postgresql-42.7.8.jar"


def get_partition_path(year, month):
    return (
        SILVER_ROOT
        / f"transfer_year={year}"
        / f"transfer_month={month}"
    )


def create_spark_session():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("RealEstatePartitionRecovery")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )


def main():
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python scripts/recover_partition.py <year> <month>"
        )

    try:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    except ValueError:
        raise SystemExit("Year and month must be integers.")

    if month < 1 or month > 12:
        raise SystemExit("Month must be between 1 and 12.")

    partition_path = get_partition_path(year, month)

    print(f"Recovery requested for partition: {year}-{month:02d}")
    print(f"Partition path: {partition_path}")

    if not partition_path.is_dir():
        raise SystemExit(
            f"Silver partition does not exist: {partition_path}"
        )

    print("Silver partition exists.")

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]

    print(f"PostgreSQL user configured: {user}")
    print(f"JDBC driver: {JDBC_JAR}")

    spark = create_spark_session()

    try:
        print("Spark session created successfully.")

        run_id = start_run(
            stage="postgres_recovery",
            partition_name=f"{year}-{month:02d}",
        )

        try:
            rows_processed = load_partition(
                spark=spark,
                silver_root=str(SILVER_ROOT),
                year=year,
                month=month,
                jdbc_user=user,
                jdbc_password=password,
            )

            finish_run(
                run_id=run_id,
                status="SUCCESS",
                rows_processed=rows_processed,
            )

            print(
                f"Recovery completed successfully: "
                f"{year}-{month:02d}"
            )
            print(f"Rows inserted: {rows_processed}")

        except Exception as exc:
            finish_run(
                run_id=run_id,
                status="FAILED",
                error_message=str(exc),
            )
            raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
