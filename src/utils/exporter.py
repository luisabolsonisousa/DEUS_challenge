from pyspark.sql import DataFrame
import os


def save_enriched_parquet(df: DataFrame, output_path: str) -> None:
    """
    Saves the enriched dataset in Parquet format, partitioned by category and transaction_date.
    """
    df.write.mode("overwrite") \
        .partitionBy("category", "transaction_date") \
        .parquet(os.path.join(output_path, "enriched_data"))


def save_revenue_csv(df: DataFrame, output_path: str) -> None:
    """
    Saves the store/category revenue insights in CSV format.
    """
    df.write.mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(output_path, "revenue_insights"))
