from pyspark.sql import DataFrame


def drop_nulls(df: DataFrame, cols: list[str]) -> DataFrame:
    """Removes rows with null values in the specified columns."""
    return df.dropna(subset=cols)


def drop_duplicates(df: DataFrame, subset: list[str] = None) -> DataFrame:
    """Removes duplicate rows based on specific columns (or all columns if none specified)."""
    return df.dropDuplicates(subset=subset)


def count_nulls(df: DataFrame) -> DataFrame:
    """Counts null values per column (for logging or debugging purposes)."""
    return df.select([df.filter(df[col].isNull()).count().alias(col) for col in df.columns])
