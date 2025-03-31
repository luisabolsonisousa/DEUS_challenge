"""
This module defines custom schemas for the project's DataFrames using PySpark.

To keep the code modular and easier to maintain, each schema is wrapped in a class that describes 
the structure of the corresponding CSV file.

By explicitly setting the data types for each column, we avoid relying on automatic schema inference 
and ensure that data is loaded consistently.

These schema classes can be used when reading the related CSV files into Spark.
"""


from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class ProductDataFormat:
    """
    Schema for reading product-related data.
    Columns: ID, name, and associated category.
    """
    def __init__(self):
        self.schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
        ])


class SalesTransactionFormat:
    """
    Structure used to load sales records.
    Includes transaction metadata, product sold, and pricing info.
    """
    def __init__(self):
        self.schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("price", FloatType(), True),
        ])


class StoreInfoFormat:
    """
    Schema layout for store data, covering unique ID, name, and geographic location.
    """
    def __init__(self):
        self.schema = StructType([
            StructField("store_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("location", StringType(), True),
        ])
