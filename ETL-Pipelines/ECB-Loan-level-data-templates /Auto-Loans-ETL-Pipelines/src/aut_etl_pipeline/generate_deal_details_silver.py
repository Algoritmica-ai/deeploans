import logging
import sys
import os
import yaml
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType, IntegerType
from src.aut_etl_pipeline.utils.silver_funcs import (
    cast_to_datatype,
)

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_project_id(config_path="config.yaml"):
    """
    Retrieve GCP project id from config file or environment variable.
    Priority: ENV var > config file > raise Exception
    """
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT_ID")
    if project_id:
        return project_id
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            project_id = config.get("gcp_project_id")
            if project_id:
                return project_id
    raise Exception(
        "GCP project id not found. Set GOOGLE_CLOUD_PROJECT_ID env variable or provide config.yaml with gcp_project_id."
    )

class NoDealDetailsBronzeFoundException(Exception):
    """Raised when no deal details bronze data is found for the silver step."""
    pass

def set_job_params():
    """
    Setup parameters used for this module.

    :return config: dictionary with properties used in this job.
    """
    config = {}
    config["DATE_COLUMNS"] = [
        # Insert here the date columns names, next an example:
        "Creation_Date",
        "Payment_Date"
        # ... and so on.
    ]
    config["DEAL_DETAILS_COLUMNS"] = {
        # Insert here the deal details columns names, next an example:
        "Asset_ID": StringType(),
        "Asset_Name": StringType(),
        "Country_Code": StringType()
        # ... and so on.
    }
    return config

def process_deal_info(df):
    """
    Extract deal info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.dropDuplicates()
    return new_df

def generate_deal_details_silver(spark, bucket_name, source_prefix, target_prefix, config_path="config.yaml"):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :param config_path: path to config file for GCP project id.
    :return status: 0 if successful, raises if required data is missing.
    :raises NoDealDetailsBronzeFoundException: if no bronze data is found.
    """
    logger.info("Start DEAL DETAILS SILVER job.")
    run_props = set_job_params()
    # Project id kept for uniformity/future needs, not used directly here
    project_id = get_project_id(config_path)
    try:
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{source_prefix}")
            .filter(F.col("iscurrent") == 1)
            .drop("valid_from", "valid_to", "checksum", "iscurrent")
        )
    except Exception as e:
        logger.error("Could not read bronze table for deal details: %s", str(e))
        raise NoDealDetailsBronzeFoundException(
            f"Could not read deal details bronze data from {bucket_name}/{source_prefix}"
        )
    if bronze_df.rdd.isEmpty():
        logger.error("Bronze deal details DataFrame is empty. Workflow stopped!")
        raise NoDealDetailsBronzeFoundException(
            f"No deal details bronze data found in {bucket_name}/{source_prefix}"
        )
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(bronze_df, run_props["DEAL_DETAILS_COLUMNS"])
    logger.info("Generate deal info dataframe")
    deal_info_df = process_deal_info(cleaned_df)

    logger.info("Write dataframe")
    (
        deal_info_df.write.format("parquet")
        .mode("overwrite")
        .save(f"gs://{bucket_name}/{target_prefix}/deal_info_table")
    )
    logger.info("End DEAL DETAILS SILVER job.")
    return 0
