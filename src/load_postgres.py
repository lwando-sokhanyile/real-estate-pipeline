import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


JDBC_URL = "jdbc:postgresql://127.0.0.1:5432/real_estate"
JDBC_DRIVER = "org.postgresql.Driver"

TARGET_TABLE = "staging.price_paid"
LOAD_TABLE = "staging.price_paid_load"


def load_to_staging(
    spark: SparkSession,
    input_path: str,
    jdbc_user: str,
    jdbc_password: str,
    limit: int | None = None,
) -> None:
    """Load Silver Price Paid data into PostgreSQL staging."""

    df = spark.read.parquet(input_path)

    if limit is not None:
        df = df.limit(limit)

    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", LOAD_TABLE)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", JDBC_DRIVER)
        .option("sslmode", "disable")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )

def load_partition(
    spark: SparkSession,
    silver_root: str,
    year: int,
    month: int,
    jdbc_user: str,
    jdbc_password: str,
) -> int:
    """Load one Silver year/month partition into PostgreSQL."""

    partition_path = (
        f"{silver_root}/"
        f"transfer_year={year}/"
        f"transfer_month={month}"
    )

    print(f"Loading partition: {year}-{month:02d}")
    print(f"Path: {partition_path}")

    df = spark.read.parquet(partition_path)

    df = (
        df
        .withColumn("transfer_year", lit(year))
        .withColumn("transfer_month", lit(month))
    )

    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", LOAD_TABLE)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", JDBC_DRIVER)
        .option("sslmode", "disable")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )
    rows_inserted = merge_staging_to_target(
        jdbc_user=jdbc_user,
        jdbc_password=jdbc_password,
    )

    print(f"Completed partition: {year}-{month:02d}")
    return rows_inserted

def merge_staging_to_target(
    jdbc_user: str,
    jdbc_password: str,
) -> int:
    """Merge staged rows into the target table idempotently."""

    columns = [
        "transaction_id",
        "price",
        "transfer_date",
        "postcode",
        "property_type",
        "old_new",
        "duration",
        "paon",
        "saon",
        "street",
        "locality",
        "town_city",
        "district",
        "county",
        "ppd_category",
        "record_status",
        "transfer_year",
        "transfer_month",
    ]

    column_list = ", ".join(columns)

    source_column_list = ", ".join(
        f"load.{column}" for column in columns
    )

    sql = f"""
        INSERT INTO {TARGET_TABLE} ({column_list})
        SELECT {source_column_list}
        FROM {LOAD_TABLE} AS load
        ON CONFLICT (transaction_id) DO NOTHING;
    """

    connection = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="real_estate",
        user=jdbc_user,
        password=jdbc_password,
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                print(f"Rows inserted: {cursor.rowcount}")
                return cursor.rowcount

    finally:
        connection.close()
