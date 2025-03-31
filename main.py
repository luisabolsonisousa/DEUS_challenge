from pyspark.sql import SparkSession
import yaml

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

from src.utils.loader import load_sales_data, load_products_data, load_stores_data
from src.utils.validator import drop_nulls, drop_duplicates
from src.utils.transformer import (
    calculate_store_category_revenue,
    calculate_monthly_sales_by_category,
    enrich_sales_data
)
from src.utils.udf import price_category_udf
from src.utils.exporter import save_enriched_parquet, save_revenue_csv


def main():
    # Load config
    logging.info("Loading configuration...")
    with open("src/configs/config.yaml", "r") as file:
        config = yaml.safe_load(file)

    app_name = config["general"]["app_name"]
    input_path = config["files"]["input_data_folder"]
    export_path = config["files"]["export_data_folder"]

    # Start Spark
    logging.info(f"Starting Spark session with app name: {app_name}")
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    # Load data
    logging.info("Reading input CSV files...")
    sales_df = load_sales_data(spark, input_path)
    products_df = load_products_data(spark, input_path)
    stores_df = load_stores_data(spark, input_path)

    # Validate
    logging.info("Cleaning sales data...")
    sales_df = drop_nulls(sales_df, ["transaction_id", "product_id", "store_id", "quantity", "price"])
    sales_df = drop_duplicates(sales_df)

    # Transform
    logging.info("Generating revenue insights...")
    revenue_df = calculate_store_category_revenue(sales_df, products_df)

    logging.info("Generating monthly sales insights...")
    monthly_sales_df = calculate_monthly_sales_by_category(sales_df, products_df)

    logging.info("Enriching sales data...")
    enriched_df = enrich_sales_data(sales_df, products_df, stores_df)

    logging.info("Adding price category...")
    enriched_df = enriched_df.withColumn("price_category", price_category_udf("price"))

    # Export
    logging.info("Saving enriched data to Parquet...")
    save_enriched_parquet(enriched_df, export_path)

    logging.info("Saving revenue insights to CSV...")
    save_revenue_csv(revenue_df, export_path)

    logging.info("Pipeline completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
