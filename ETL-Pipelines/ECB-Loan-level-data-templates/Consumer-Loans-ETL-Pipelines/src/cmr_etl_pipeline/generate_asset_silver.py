import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from google.cloud import storage
from src.cmr_etl_pipeline.utils.silver_funcs import (
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


def set_job_params():
    """
    Setup parameters used for this module.

    :return config: dictionary with properties used in this job.
    """
    config = {}
    config["DATE_COLUMNS"] = [
        "AN1",
        "AN12",
        "AN21",
        "AN22",
        "AN24",
        "AN41",
        "AN51",
        "AN56",
        "AN60",
    ]
    config["ASSET_COLUMNS"] = {
        "AN1": DateType(),
        "AN2": StringType(),
        "AN3": StringType(),
        "AN4": StringType(),
        "AN5": StringType(),
        "AN6": StringType(),
        "AN7": BooleanType(),
        "AN8": StringType(),
        "AN10": StringType(),
        "AN11": DoubleType(),
        "AN12": DateType(),
        "AN15": StringType(),
        "AN16": StringType(),
        "AN17": DoubleType(),
        "AN18": StringType(),
        "AN19": StringType(),
        "AN20": StringType(),
        "AN21": DateType(),
        "AN22": DateType(),
        "AN23": DoubleType(),
        "AN24": DateType(),
        "AN25": DoubleType(),
        "AN26": DoubleType(),
        "AN27": DoubleType(),
        "AN28": StringType(),
        "AN29": StringType(),
        "AN30": StringType(),
        "AN31": DoubleType(),
        "AN32": DoubleType(),
        "AN33": StringType(),
        "AN34": DoubleType(),
        "AN35": DoubleType(),
        "AN36": DoubleType(),
        "AN37": DoubleType(),
        "AN38": StringType(),
        "AN39": StringType(),
        "AN40": StringType(),
        "AN41": DateType(),
        "AN42": DoubleType(),
        "AN43": BooleanType(),
        "AN44": DoubleType(),
        "AN45": DoubleType(),
        "AN49": DoubleType(),
        "AN50": DoubleType(),
        "AN51": DateType(),
        "AN52": DoubleType(),
        "AN55": DoubleType(),
        "AN56": DateType(),
        "AN58": StringType(),
        "AN59": DoubleType(),
        "AN60": DateType(),
        "AN61": BooleanType(),
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
        + [f"AN{i}" for i in range(1, 6) if f"AN{i}" in df.columns],
        "loan_info": [f"AN{i}" for i in range(6, 49) if f"AN{i}" in df.columns],
        "performance_info": [f"AN{i}" for i in range(49, 65) if f"AN{i}" in df.columns],
    }
    return cols_dict


def process_loan_info(df, cols_dict):
    """
    Extract loan info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(cols_dict["general"] + cols_dict["loan_info"]).dropDuplicates()
    return new_df


def process_performance_info(df, cols_dict):
    """
    Extract performance info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["performance_info"]
    ).dropDuplicates()
    return new_df


def generate_asset_silver(
    spark, bucket_name, source_prefix, target_prefix, dl_code, ingestion_date
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :param dl_code: deal code to process.
    :param ingestion_date: date of the ETL ingestion.
    :return status: 0 if successful.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    storage_client = storage.Client(project="your project_id")
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(bucket_name, prefix="clean_dump/assets")
        if f"{ingestion_date}_{dl_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            "Could not find clean CSV dump file from ASSETS BRONZE PROFILING BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)
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
            logger.info("Generate loan info dataframe")
            loan_info_df = process_loan_info(cleaned_df, assets_columns)
            logger.info("Generate performace info dataframe")
            performance_info_df = process_performance_info(cleaned_df, assets_columns)
            logger.info("Write dataframe")

            (
                loan_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/loan_info_table")
            )
            (
                performance_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/performance_info_table")
            )

    logger.info("Remove clean dumps.")
    for clean_dump_csv in all_clean_dumps:
        clean_dump_csv.delete()
    logger.info("End ASSET SILVER job.")
    return 0
