import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from google.cloud import storage
from src.rmb_etl_pipeline.utils.silver_funcs import (
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
    config["DATE_COLUMNS"] = ["BR1", "BR27", "BR28", "BR38", "BR39"]
    config["BOND_COLUMNS"] = {
        "BR1": DateType(),
        "BR2": StringType(),
        "BR3": DoubleType(),
        "BR4": DoubleType(),
        "BR5": BooleanType(),
        "BR11": DoubleType(),
        "BR12": BooleanType(),
        "BR13": DoubleType(),
        "BR19": StringType(),
        "BR20": StringType(),
        "BR25": StringType(),
        "BR26": StringType(),
        "BR27": DateType(),
        "BR28": DateType(),
        "BR29": StringType(),
        "BR30": DoubleType(),
        "BR31": DoubleType(),
        "BR32": StringType(),
        "BR33": DoubleType(),
        "BR34": DoubleType(),
        "BR35": DoubleType(),
        "BR36": DoubleType(),
        "BR37": DoubleType(),
        "BR38": DateType(),
        "BR39": DateType(),
    }
    return config


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Bond Info bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "general": ["dl_code", "pcd_year", "pcd_month", "BR1", "BR2"],
        "bond_info": [f"BR{i}" for i in range(3, 11) if f"BR{i}" in df.columns],
        "collateral_info": [f"BR{i}" for i in range(11, 19) if f"BR{i}" in df.columns],
        "transaction_info": [f"BR{i}" for i in range(19, 25) if f"BR{i}" in df.columns],
        "tranche_info": [f"BR{i}" for i in range(25, 44) if f"BR{i}" in df.columns],
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


def process_collateral_info(df, cols_dict):
    """
    Extract collateral info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["collateral_info"]
    ).dropDuplicates()
    return new_df


def process_transaction_info(df, cols_dict):
    """
    Extract transaction info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["transaction_info"]
    ).dropDuplicates()
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
    logger.info("Start BOND_INFO SILVER job.")
    run_props = set_job_params()
    storage_client = storage.Client(project="your project_id")
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(bucket_name, prefix="clean_dump/bond_info")
        if f"{ingestion_date}_{dl_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            "Could not find clean CSV dump file from BOND_INFO BRONZE PROFILING BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)
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
            logger.info("Generate collateral info dataframe")
            collateral_df = process_collateral_info(cleaned_df, bond_info_columns)
            logger.info("Generate transaction info dataframe")
            transaction_df = process_transaction_info(cleaned_df, bond_info_columns)
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
                collateral_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/collaterls_table")
            )
            (
                transaction_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/transaction_table")
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
