import pytest
from pyspark.sql import SparkSession, Row
from src.utils.udf import price_category_udf
from chispa.dataframe_comparer import assert_df_equality


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("UDF Test").getOrCreate()


def test_price_category_udf(spark):
    data = [
        Row(price=10.0),
        Row(price=50.0),
        Row(price=150.0),
        Row(price=None),
    ]
    expected = [
        Row(price=10.0, price_category="Low"),
        Row(price=50.0, price_category="Medium"),
        Row(price=150.0, price_category="High"),
        Row(price=None, price_category="Unknown"),
    ]

    df = spark.createDataFrame(data)
    expected_df = spark.createDataFrame(expected)

    result_df = df.withColumn("price_category", price_category_udf("price"))

    assert_df_equality(result_df, expected_df, ignore_row_order=True)
