import psycopg2
from pyspark.sql import SparkSession


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


def merge_staging_to_target(
    jdbc_user: str,
    jdbc_password: str,
) -> None:
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

    sql = f"""
        INSERT INTO {TARGET_TABLE} ({column_list})
        SELECT {column_list}
        FROM {LOAD_TABLE}
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

    finally:
        connection.close()
