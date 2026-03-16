import logging
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, DoubleType, BooleanType, IntegerType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

def replace_no_data(df):
    """
    Replace ND values inside the dataframe with None.
    ND values are associated with labels that explain why the value is missing.
    This function simply replaces any value starting with "ND" with None.
    Future versions should handle missingness explanations more granularly.

    :param df: Spark dataframe with data.
    :return df: Spark dataframe without ND values.
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).startswith("ND"), None).otherwise(F.col(col_name)),
        )
    logger.info("Replaced ND values with None.")
    return df

def replace_bool_data(df):
    """
    Replace Y/N (case-insensitive) with true/false flags in all string columns.

    :param df: Spark dataframe with loan asset data.
    :return df: Spark dataframe with boolean-like string values.
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.lower(F.col(col_name)) == "y", "true")
            .when(F.lower(F.col(col_name)) == "n", "false")
            .otherwise(F.col(col_name)),
        )
    logger.info("Replaced Y/N values with true/false strings.")
    return df

def cast_to_datatype(df, columns):
    """
    Cast data to the respective datatype.

    :param df: Spark dataframe with loan deal details data.
    :param columns: dictionary of column names and respective data types.
    :return df: Spark dataframe with correct column types.
    :raises Exception: if a column in columns is missing from df.
    """
    for col_name, data_type in columns.items():
        if col_name not in df.columns:
            logger.warning(f"Column '{col_name}' specified for casting not in DataFrame columns.")
            continue
        if data_type == BooleanType():
            # Accepts both boolean and string "true"/"false"
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).cast(BooleanType()).isNotNull(), F.col(col_name).cast(BooleanType()))
                .when(F.col(col_name) == "true", F.lit(True))
                .when(F.col(col_name) == "false", F.lit(False))
                .otherwise(None)
            )
        elif data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        elif data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
        elif data_type == IntegerType():
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
        else:
            logger.warning(f"Data type '{data_type}' for column '{col_name}' not explicitly handled; column left as is.")
    logger.info("Cast columns to specified datatypes where possible.")
    return df
