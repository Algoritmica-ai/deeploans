from pyspark.sql import SparkSession
from delta import *


def start_spark(app_name="DeeploansApp"):
    """
    Create and configure a SparkSession with Delta Lake and Google Cloud Storage support.

    :param app_name: Name of the Spark App.
    :return: SparkSession configured for Delta Lake and GCS.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
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
