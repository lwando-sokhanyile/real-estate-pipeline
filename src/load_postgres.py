from pyspark.sql import SparkSession


JDBC_URL = "jdbc:postgresql://127.0.0.1:5432/real_estate"
JDBC_DRIVER = "org.postgresql.Driver"
TARGET_TABLE = "staging.price_paid"


def load_to_postgres(
    spark: SparkSession,
    input_path: str,
    jdbc_user: str,
    jdbc_password: str,
) -> None:
    """Load Silver Price Paid data into PostgreSQL staging."""

    df = spark.read.parquet(input_path)

    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TARGET_TABLE)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", JDBC_DRIVER)
        .option("batchsize", 10000)
        .mode("append")
        .save()
    )