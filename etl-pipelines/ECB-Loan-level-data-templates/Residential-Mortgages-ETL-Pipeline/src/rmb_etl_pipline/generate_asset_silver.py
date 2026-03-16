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
    config["DATE_COLUMNS"] = [
        "AR1",
        "AR18",
        "AR20",
        "AR35",
        "AR38",
        "AR45",
        "AR55",
        "AR56",
        "AR57",
        "AR86",
        "AR89",
        "AR98",
        "AR114",
        "AR116",
        "AR118",
        "AR121",
        "AR133",
        "AR138",
        "AR145",
        "AR151",
        "AR167",
        "AR168",
        "AR173",
        "AR175",
    ]
    config["ASSET_COLUMNS"] = {
        "AR1": DateType(),
        "AR2": StringType(),
        "AR3": StringType(),
        "AR4": BooleanType(),
        "AR5": StringType(),
        "AR6": StringType(),
        "AR7": StringType(),
        "AR8": StringType(),
        "AR15": StringType(),
        "AR16": BooleanType(),
        "AR17": StringType(),
        "AR18": DateType(),
        "AR19": DoubleType(),
        "AR20": DateType(),
        "AR21": StringType(),
        "AR22": BooleanType(),
        "AR23": BooleanType(),
        "AR24": DoubleType(),
        "AR25": StringType(),
        "AR26": DoubleType(),
        "AR27": StringType(),
        "AR28": DoubleType(),
        "AR29": StringType(),
        "AR30": StringType(),
        "AR31": DoubleType(),
        "AR32": DoubleType(),
        "AR33": DoubleType(),
        "AR34": DoubleType(),
        "AR35": DateType(),
        "AR36": BooleanType(),
        "AR37": StringType(),
        "AR38": DateType(),
        "AR39": StringType(),
        "AR40": DoubleType(),
        "AR41": BooleanType(),
        "AR42": DoubleType(),
        "AR43": StringType(),
        "AR44": StringType(),
        "AR45": DateType(),
        "AR46": StringType(),
        "AR47": BooleanType(),
        "AR48": DoubleType(),
        "AR49": DoubleType(),
        "AR55": DateType(),
        "AR56": DateType(),
        "AR57": DateType(),
        "AR58": StringType(),
        "AR59": StringType(),
        "AR60": StringType(),
        "AR61": DoubleType(),
        "AR62": DoubleType(),
        "AR63": DoubleType(),
        "AR64": BooleanType(),
        "AR65": StringType(),
        "AR66": DoubleType(),
        "AR67": DoubleType(),
        "AR68": BooleanType(),
        "AR69": StringType(),
        "AR70": StringType(),
        "AR71": DoubleType(),
        "AR72": StringType(),
        "AR73": DoubleType(),
        "AR74": StringType(),
        "AR75": StringType(),
        "AR76": DoubleType(),
        "AR77": DoubleType(),
        "AR78": StringType(),
        "AR79": DoubleType(),
        "AR80": DoubleType(),
        "AR81": DoubleType(),
        "AR82": DoubleType(),
        "AR83": DoubleType(),
        "AR84": StringType(),
        "AR85": DoubleType(),
        "AR86": DateType(),
        "AR87": DoubleType(),
        "AR88": DoubleType(),
        "AR89": DateType(),
        "AR90": DoubleType(),
        "AR91": BooleanType(),
        "AR92": DoubleType(),
        "AR93": DoubleType(),
        "AR94": DoubleType(),
        "AR95": DoubleType(),
        "AR96": BooleanType(),
        "AR97": DoubleType(),
        "AR98": DateType(),
        "AR99": DoubleType(),
        "AR100": DoubleType(),
        "AR101": DoubleType(),
        "AR107": StringType(),
        "AR108": StringType(),
        "AR109": DoubleType(),
        "AR110": DoubleType(),
        "AR111": DoubleType(),
        "AR112": DoubleType(),
        "AR113": DoubleType(),
        "AR114": DateType(),
        "AR115": DoubleType(),
        "AR116": DateType(),
        "AR117": DoubleType(),
        "AR118": DateType(),
        "AR119": StringType(),
        "AR120": DoubleType(),
        "AR121": DateType(),
        "AR122": BooleanType(),
        "AR128": StringType(),
        "AR129": StringType(),
        "AR130": StringType(),
        "AR131": StringType(),
        "AR132": StringType(),
        "AR133": DateType(),
        "AR134": StringType(),
        "AR135": DoubleType(),
        "AR136": DoubleType(),
        "AR137": StringType(),
        "AR138": DateType(),
        "AR139": DoubleType(),
        "AR140": StringType(),
        "AR141": DoubleType(),
        "AR142": DoubleType(),
        "AR143": DoubleType(),
        "AR144": StringType(),
        "AR145": DateType(),
        "AR146": DoubleType(),
        "AR147": StringType(),
        "AR148": DoubleType(),
        "AR149": DoubleType(),
        "AR150": DoubleType(),
        "AR151": DateType(),
        "AR152": StringType(),
        "AR153": StringType(),
        "AR154": DoubleType(),
        "AR155": DoubleType(),
        "AR156": DoubleType(),
        "AR157": DoubleType(),
        "AR158": BooleanType(),
        "AR159": BooleanType(),
        "AR160": DoubleType(),
        "AR166": StringType(),
        "AR167": DateType(),
        "AR168": DateType(),
        "AR169": DoubleType(),
        "AR170": DoubleType(),
        "AR171": DoubleType(),
        "AR172": DoubleType(),
        "AR173": DateType(),
        "AR174": BooleanType(),
        "AR175": DateType(),
        "AR176": DoubleType(),
        "AR177": DoubleType(),
        "AR178": DoubleType(),
        "AR179": DoubleType(),
        "AR180": DoubleType(),
        "AR181": DoubleType(),
        "AR182": DoubleType(),
        "AR183": BooleanType(),
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
        + [f"AR{i}" for i in range(1, 15) if f"AR{i}" in df.columns],
        "borrower_info": [f"AR{i}" for i in range(15, 55) if f"AR{i}" in df.columns],
        "loan_info": [f"AR{i}" for i in range(55, 107) if f"AR{i}" in df.columns],
        "interest_rate": [f"AR{i}" for i in range(107, 128) if f"AR{i}" in df.columns],
        "collateral_info": [
            f"AR{i}" for i in range(128, 166) if f"AR{i}" in df.columns
        ],
        "performance_info": [
            f"AR{i}" for i in range(166, 196) if f"AR{i}" in df.columns
        ],
    }
    return cols_dict


def process_borrower_info(df, cols_dict):
    """
    Extract borrower info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["borrower_info"]
    ).dropDuplicates()
    return new_df


def process_loan_info(df, cols_dict):
    """
    Extract loan info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(cols_dict["general"] + cols_dict["loan_info"]).dropDuplicates()
    return new_df


def process_interest_rate(df, cols_dict):
    """
    Extract interest rate dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["interest_rate"]
    ).dropDuplicates()
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
            logger.info("Generate borrower info dataframe")
            borrower_info_df = process_borrower_info(cleaned_df, assets_columns)
            logger.info("Generate loan info dataframe")
            loan_info_df = process_loan_info(cleaned_df, assets_columns)
            logger.info("Generate interest rate dataframe")
            interest_rate_df = process_interest_rate(cleaned_df, assets_columns)
            logger.info("Generate collateral info dataframe")
            collateral_info_df = process_collateral_info(cleaned_df, assets_columns)
            logger.info("Generate performace info dataframe")
            performance_info_df = process_performance_info(cleaned_df, assets_columns)

            logger.info("Write dataframe")

            (
                borrower_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/borrower_info_table")
            )
            (
                loan_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/loan_info_table")
            )
            (
                collateral_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/collateral_info_table")
            )
            (
                interest_rate_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/interest_rate_table")
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
