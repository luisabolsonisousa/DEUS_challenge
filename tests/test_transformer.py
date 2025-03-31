import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.utils.transformer import calculate_store_category_revenue


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Test").getOrCreate()


def test_calculate_store_category_revenue(spark):
    sales_data = [
        Row(transaction_id="1", store_id="A", product_id="p1", quantity=2, transaction_date="2024-01-01", price=10.0),
        Row(transaction_id="2", store_id="A", product_id="p2", quantity=1, transaction_date="2024-01-01", price=100.0),
    ]
    products_data = [
        Row(product_id="p1", product_name="apple", category="fruit"),
        Row(product_id="p2", product_name="hammer", category="tools"),
    ]

    sales_df = spark.createDataFrame(sales_data)
    products_df = spark.createDataFrame(products_data)

    result_df = calculate_store_category_revenue(sales_df, products_df)

    expected_data = [
        Row(store_id="A", category="fruit", total_revenue=20.0),
        Row(store_id="A", category="tools", total_revenue=100.0),
    ]
    expected_df = spark.createDataFrame(expected_data)

    from chispa.dataframe_comparer import assert_df_equality
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
