from pyspark.sql import SparkSession
from delta import *
from src.aut_etl_pipeline.config import DELTA_CORE_VERSION, DELTA_LOGSTORE_IMPL

def start_spark(app_name="ESMA_ETL"):
    """
    Create Spark application using Delta Lake dependencies.
    :param app_name: Name of the Spark App (default 'ESMA_ETL')
    :return: SparkSession
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
        .config("spark.jars.packages", f"io.delta:delta-core:{DELTA_CORE_VERSION}")
        .config(
            "spark.delta.logStore.gs.impl",
            DELTA_LOGSTORE_IMPL,
        )
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    return spark
