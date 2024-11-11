from pyspark.sql import SparkSession
from delta import *


def start_spark():
    """
    Create Spark application using Delta Lake dependencies.

    :param app_name: Name of the Spark App
    :return: SparkSession
    """

    spark = (
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-core:2.1.0")
        .config(
            "spark.delta.logStore.gs.impl",
            "io.delta.storage.GCSLogStore",
        )
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    return spark
