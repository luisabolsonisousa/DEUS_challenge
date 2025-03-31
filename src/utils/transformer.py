from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, year, month


def calculate_store_category_revenue(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Calculates total revenue per store and product category.
    """
    df = sales_df.join(products_df, on="product_id", how="inner")
    return df.groupBy("store_id", "category").agg(
        sum(col("quantity") * col("price")).alias("total_revenue")
    )


def calculate_monthly_sales_by_category(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Computes monthly total quantity sold per product category.
    """
    df = sales_df.join(products_df, on="product_id", how="inner")
    return df.withColumn("year", year("transaction_date")) \
             .withColumn("month", month("transaction_date")) \
             .groupBy("year", "month", "category") \
             .agg(sum("quantity").alias("total_quantity_sold"))


def enrich_sales_data(sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame) -> DataFrame:
    """
    Merges sales, products, and stores into a single enriched DataFrame.
    """
    df = sales_df.join(products_df, on="product_id", how="left") \
                 .join(stores_df, on="store_id", how="left")
    return df.select(
        "transaction_id",
        "store_name",
        "location",
        "product_name",
        "category",
        "quantity",
        "transaction_date",
        "price"
    )
