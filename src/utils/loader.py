from pyspark.sql import SparkSession, DataFrame
from src.utils.metadata import ProductDataFormat, SalesTransactionFormat, StoreInfoFormat
import os


def load_sales_data(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.csv(
        os.path.join(input_path, "sales_uuid.csv"),
        header=True,
        schema=SalesTransactionFormat().schema,
    )


def load_products_data(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.csv(
        os.path.join(input_path, "products_uuid.csv"),
        header=True,
        schema=ProductDataFormat().schema,
    )


def load_stores_data(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.csv(
        os.path.join(input_path, "stores_uuid.csv"),
        header=True,
        schema=StoreInfoFormat().schema,
    )
