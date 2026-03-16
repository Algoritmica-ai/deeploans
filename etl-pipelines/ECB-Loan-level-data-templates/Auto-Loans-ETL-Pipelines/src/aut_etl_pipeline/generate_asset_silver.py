import logging
import sys
import os
import yaml
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from google.cloud import storage
from src.aut_etl_pipeline.utils.silver_funcs import (
    replace_no_data,
    replace_bool_data,
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

class NoAssetCleanDumpFoundException(Exception):
    """Raised when no clean_dump CSV files are found for the current asset silver step."""
    pass

def set_job_params():
    """
    Setup parameters used for this module.

    :return config: dictionary with properties used in this job.
    """
    config = {}
    config["DATE_COLUMNS"] = [
        "AA1",
        "AA22",
        "AA23",
        "AA25",
        "AA32",
        "AA47",
        "AA53",
        "AA58",
        "AA67",
        "AA72",
    ]
    config["ASSET_COLUMNS"] = {
        "AA1": DateType(),
        "AA2": StringType(),
        "AA3": StringType(),
        "AA4": StringType(),
        "AA5": StringType(),
        "AA6": StringType(),
        "AA7": BooleanType(),
        "AA8": StringType(),
        "AA9": StringType(),
        "AA10": StringType(),
        "AA15": StringType(),
        "AA16": StringType(),
        "AA17": DoubleType(),
        "AA18": StringType(),
        "AA19": StringType(),
        "AA20": StringType(),
        "AA21": StringType(),
        "AA22": DateType(),
        "AA23": DateType(),
        "AA24": DoubleType(),
        "AA25": DateType(),
        "AA26": DoubleType(),
        "AA27": DoubleType(),
        "AA28": DoubleType(),
        "AA29": StringType(),
        "AA30": DoubleType(),
        "AA31": DoubleType(),
        "AA32": DateType(),
        "AA33": StringType(),
        "AA34": DoubleType(),
        "AA35": DoubleType(),
        "AA36": StringType(),
        "AA37": DoubleType(),
        "AA39": DoubleType(),
        "AA40": DoubleType(),
        "AA41": StringType(),
        "AA42": DoubleType(),
        "AA43": DoubleType(),
        "AA44": StringType(),
        "AA45": StringType(),
        "AA46": StringType(),
        "AA47": DateType(),
        "AA48": StringType(),
        "AA49": DoubleType(),
        "AA50": DoubleType(),
        "AA51": DoubleType(),
        "AA52": DoubleType(),
        "AA53": DateType(),
        "AA54": StringType(),
        "AA55": StringType(),
        "AA56": BooleanType(),
        "AA57": StringType(),
        "AA58": DateType(),
        "AA59": DoubleType(),
        "AA60": DoubleType(),
        "AA61": DoubleType(),
        "AA65": DoubleType(),
        "AA66": DoubleType(),
        "AA67": DateType(),
        "AA68": DoubleType(),
        "AA69": DoubleType(),
        "AA70": DoubleType(),
        "AA71": DoubleType(),
        "AA72": DateType(),
        "AA73": DoubleType(),
        "AA74": StringType(),
    }
    return config

def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Asset bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "general": ["dl_code", "pcd_year", "pcd_month"]
        + [f"AA{i}" for i in range(1, 5) if f"AA{i}" in df.columns],
        "lease_info": [f"AA{i}" for i in range(5, 79) if f"AA{i}" in df.columns],
    }
    return cols_dict

def process_lease_info(df, cols_dict):
    """
    Extract lease info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(cols_dict["general"] + cols_dict["lease_info"]).dropDuplicates()
    return new_df

def generate_asset_silver(
    spark, bucket_name, source_prefix, target_prefix, dl_code, ingestion_date, config_path="config.yaml"
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :param dl_code: deal code to process.
    :param ingestion_date: date of the ETL ingestion.
    :param config_path: path to config file for GCP project id.
    :return status: 0 if successful, raises if required data is missing.
    :raises NoAssetCleanDumpFoundException: if no clean_dump is found for asset.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    project_id = get_project_id(config_path)
    storage_client = storage.Client(project=project_id)
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(bucket_name, prefix="clean_dump/assets")
        if f"{ingestion_date}_{dl_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            "Could not find clean CSV dump file from ASSETS BRONZE PROFILING job. Workflow stopped!"
        )
        raise NoAssetCleanDumpFoundException(
            f"No clean_dump files found for asset with dl_code={dl_code}, ingestion_date={ingestion_date}."
        )
    else:
        for clean_dump_csv in all_clean_dumps:
            pcd = "_".join(clean_dump_csv.name.split("/")[-1].split("_")[2:4])
            logger.info(f"Processing data for deal {dl_code}:{pcd}")
            part_pcd = pcd.replace("_0", "").replace("_", "")
            bronze_df = (
                spark.read.format("delta")
                .load(f"gs://{bucket_name}/{source_prefix}")
                .where(F.col("part") == f"{dl_code}_{part_pcd}")
                .filter(F.col("iscurrent") == 1)
                .drop("valid_from", "valid_to", "checksum", "iscurrent")
            )
            assets_columns = get_columns_collection(bronze_df)
            logger.info("Remove ND values.")
            tmp_df1 = replace_no_data(bronze_df)
            logger.info("Replace Y/N with boolean flags.")
            tmp_df2 = replace_bool_data(tmp_df1)
            logger.info("Cast data to correct types.")
            cleaned_df = cast_to_datatype(tmp_df2, run_props["ASSET_COLUMNS"])
            logger.info("Generate lease info dataframe")
            lease_info_df = process_lease_info(cleaned_df, assets_columns)

            logger.info("Write dataframe")
            (
                lease_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/lease_info_table")
            )

    logger.info("Remove clean dumps.")
    for clean_dump_csv in all_clean_dumps:
        clean_dump_csv.delete()
    logger.info("End ASSET SILVER job.")
    return 0
