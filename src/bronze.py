from pyspark.sql import DataFrame
from pyspark.sql.functions import month, year


def write_bronze(
    df: DataFrame,
    output_path: str,
) -> None:
    """Write typed Price Paid data to partitioned Parquet Bronze storage."""

    bronze_df = (
        df
        .withColumn("transfer_year", year("transfer_date"))
        .withColumn("transfer_month", month("transfer_date"))
    )

    (
        bronze_df.write
        .mode("overwrite")
        .partitionBy("transfer_year", "transfer_month")
        .parquet(output_path)
    )