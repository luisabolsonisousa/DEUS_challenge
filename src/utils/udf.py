from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def classify_price(price: float) -> str:
    """
    Classifies a product based on its price into Low, Medium, or High.
    """
    if price is None:
        return "Unknown"
    elif price < 20:
        return "Low"
    elif 20 <= price <= 100:
        return "Medium"
    else:
        return "High"


price_category_udf = udf(classify_price, StringType())
