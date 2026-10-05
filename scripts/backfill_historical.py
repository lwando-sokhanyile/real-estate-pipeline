import os
from pathlib import Path

from pyspark.sql import SparkSession

from src.load_postgres import load_partition


SILVER_ROOT = "data/silver/price_paid"
JDBC_JAR = "lib/postgresql-42.7.8.jar"


def discover_partitions(silver_root: str) -> list[tuple[int, int]]:
    """Discover available year/month Silver partitions."""

    root = Path(silver_root)
    partitions = []

    for year_dir in root.glob("transfer_year=*"):
        if not year_dir.is_dir():
            continue

        try:
            year = int(year_dir.name.split("=", 1)[1])
        except ValueError:
            continue

        for month_dir in year_dir.glob("transfer_month=*"):
            if not month_dir.is_dir():
                continue

            try:
                month = int(month_dir.name.split("=", 1)[1])
            except ValueError:
                continue

            partitions.append((year, month))

    return sorted(partitions)


def main() -> None:
    """Run the historical Silver-to-PostgreSQL backfill month by month."""

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]

    partitions = discover_partitions(SILVER_ROOT)

    if not partitions:
        raise RuntimeError(f"No Silver partitions found under {SILVER_ROOT}")

    print(f"Discovered {len(partitions)} partitions.")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("RealEstateHistoricalBackfill")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )

    try:
        for index, (year, month) in enumerate(partitions, start=1):
            print()
            print("=" * 60)
            print(f"Partition {index}/{len(partitions)}: {year}-{month:02d}")
            print("=" * 60)

            try:
                load_partition(
                    spark=spark,
                    silver_root=SILVER_ROOT,
                    year=year,
                    month=month,
                    jdbc_user=user,
                    jdbc_password=password,
                )
            except Exception as exc:
                print()
                print("=" * 60)
                print(f"FAILED: {year}-{month:02d}")
                print(f"Reason: {exc}")
                print("=" * 60)
                raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
