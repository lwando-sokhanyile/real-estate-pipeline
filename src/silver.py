from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper


def standardize_text(df: DataFrame) -> DataFrame:
    """Standardize text fields in the Price Paid dataset."""

    text_columns = [
        "postcode",
        "property_type",
        "old_new",
        "duration",
        "town_city",
        "district",
        "county",
        "ppd_category",
        "record_status",
    ]

  
    for column_name in text_columns:
        df = df.withColumn(
            column_name,
            upper(trim(col(column_name))),
        )

    return df


def find_invalid_rows(df: DataFrame) -> DataFrame:
    """Return rows that violate required Price Paid data-quality rules."""

    invalid_rows = df.filter(
        col("transaction_id").isNull()
        | (col("price") <= 0)
        | col("transfer_date").isNull()
        | ~col("property_type").isin("D", "F", "O", "S", "T")
        | ~col("old_new").isin("Y", "N")
        | ~col("duration").isin("F", "L")
        | ~col("ppd_category").isin("A", "B")
        | ~col("record_status").isin("A")
    )

    return invalid_rows