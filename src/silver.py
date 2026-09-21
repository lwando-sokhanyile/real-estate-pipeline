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