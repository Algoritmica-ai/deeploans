import pyspark.sql.functions as F
from pyspark.sql.types import DateType, DoubleType, BooleanType, IntegerType

def replace_no_data(df):
    """
    Replace ND values inside the dataframe
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).startswith("ND"), None).otherwise(F.col(col_name)),
        )
    return df

def replace_bool_data(df):
    """
    Replace Y/N with boolean True/False values in the dataframe.
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "Y", F.lit(True))
            .when(F.col(col_name) == "N", F.lit(False))
            .otherwise(F.col(col_name)),
        )
    return df

def cast_to_datatype(df, columns):
    """
    Cast data to the respective datatype.
    """
    for col_name, data_type in columns.items():
        if data_type == BooleanType():
            # Solo se non già booleano
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) == True, F.lit(True))
                 .when(F.col(col_name) == False, F.lit(False))
                 .otherwise(None)
            )
        elif data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        elif data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
        elif data_type == IntegerType():
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
    return df
