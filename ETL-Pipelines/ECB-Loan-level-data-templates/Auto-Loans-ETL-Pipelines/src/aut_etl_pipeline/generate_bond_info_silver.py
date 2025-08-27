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

class NoBondInfoCleanDumpFoundException(Exception):
    """Raised when no clean_dump CSV files are found for the current bond info silver step."""
    pass

def set_job_params():
    """
    Setup parameters used for this module.

    :return config: dictionary with properties used in this job.
    """
    config = {}
    config["DATE_COLUMNS"] = [
        "BAA1",
        "BAA18",
        "BAA27",
        "BAA28",
        "BAA38",
        "BAA39",
        "BAA40",
        "BAA42",
    ]
    config["BOND_COLUMNS"] = {
        "BAA1": DateType(),
        "BAA2": StringType(),
        "BAA4": BooleanType(),
        "BAA5": BooleanType(),
        "BAA11": DoubleType(),
        "BAA12": BooleanType(),
        "BAA13": DoubleType(),
        "BAA14": DoubleType(),
        "BAA15": DoubleType(),
        "BAA16": DoubleType(),
        "BAA17": DoubleType(),
        "BAA18": DateType(),
        "BAA19": StringType(),
        "BAA20": StringType(),
        "BAA25": StringType(),
        "BAA26": StringType(),
        "BAA27": DateType(),
        "BAA28": DateType(),
        "BAA29": StringType(),
        "BAA30": DoubleType(),
        "BAA31": DoubleType(),
        "BAA32": StringType(),
        "BAA33": DoubleType(),
        "BAA34": DoubleType(),
        "BAA35": DoubleType(),
        "BAA36": DoubleType(),
        "BAA37": DoubleType(),
        "BAA38": DateType(),
        "BAA39": DateType(),
        "BAA40": DateType(),
        "BAA41": StringType(),
        "BAA42": DateType(),
        "BAA43": DoubleType(),
        "BAA44": DoubleType(),
        "BAA45": DoubleType(),
        "BAA46": DoubleType(),
    }
    return config

def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Bond Info bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "general": ["dl_code", "pcd_year", "pcd_month", "BAA1", "BAA2"],
        "bond_info": [f"BAA{i}" for i in range(3, 19) if f"BAA{i}" in df.columns],
        "contact_info": [f"BAA{i}" for i in range(19, 25) if f"BAA{i}" in df.columns],
        "tranche_info": [f"BAA{i}" for i in range(25, 51) if f"BAA{i}" in df.columns],
    }
    return cols_dict

def process_bond_info(df, cols_dict):
    """
    Extract bond info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(cols_dict["general"] + cols_dict["bond_info"]).dropDuplicates()
    return new_df

def process_tranche_info(df, cols_dict):
    """
    Extract tranche info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["tranche_info"]
    ).dropDuplicates()
    return new_df

def generate_bond_info_silver(
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
    :raises NoBondInfoCleanDumpFoundException: if no clean_dump is found for bond info.
    """
    logger.info("Start BOND_INFO SILVER job.")
    run_props = set_job_params()
    project_id = get_project_id(config_path)
    storage_client = storage.Client(project=project_id)
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(bucket_name, prefix="clean_dump/bond_info")
        if f"{ingestion_date}_{dl_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            "Could not find clean CSV dump file from BOND_INFO BRONZE PROFILING job. Workflow stopped!"
        )
        raise NoBondInfoCleanDumpFoundException(
            f"No clean_dump files found for bond_info with dl_code={dl_code}, ingestion_date={ingestion_date}."
        )
    else:
        for clean_dump_csv in all_clean_dumps:
            pcd = "_".join(clean_dump_csv.name.split("/")[-1].split("_")[2:4])
            logger.info(f"Processing data for deal {dl_code}:{pcd}")
            part_pcd = pcd.replace("_0", "").replace("_", "")
            logger.info(f"Processing {pcd} data from bronze to silver. ")
            bronze_df = (
                spark.read.format("delta")
                .load(f"gs://{bucket_name}/{source_prefix}")
                .where(F.col("part") == f"{dl_code}_{part_pcd}")
                .filter(F.col("iscurrent") == 1)
                .drop("valid_from", "valid_to", "checksum", "iscurrent")
            )
            logger.info("Remove ND values.")
            tmp_df1 = replace_no_data(bronze_df)
            logger.info("Replace Y/N with boolean flags.")
            tmp_df2 = replace_bool_data(tmp_df1)
            logger.info("Cast data to correct types.")
            cleaned_df = cast_to_datatype(tmp_df2, run_props["BOND_COLUMNS"])
            bond_info_columns = get_columns_collection(cleaned_df)
            logger.info("Generate bond info dataframe")
            info_df = process_bond_info(cleaned_df, bond_info_columns)
            logger.info("Generate tranche info dataframe")
            tranche_df = process_tranche_info(cleaned_df, bond_info_columns)

            logger.info("Write dataframe")
            (
                info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/info_table")
            )
            (
                tranche_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/tranche_info_table")
            )
    logger.info("Remove clean dumps.")
    for clean_dump_csv in all_clean_dumps:
        clean_dump_csv.delete()
    logger.info("End BOND INFO SILVER job.")
    return 0
