import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from google.cloud import storage
from src.loan_etl_pipeline.utils.silver_funcs import (
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
        "AS1",
        "AS19",
        "AS20",
        "AS31",
        "AS50",
        "AS51",
        "AS67",
        "AS70",
        "AS71",
        "AS87",
        "AS91",
        "AS112",
        "AS124",
        "AS127",
        "AS130",
        "AS133",
        "AS134",
        "AS137",
    ]
    config["ASSET_COLUMNS"] = {
        "AS1": DateType(),
        "AS2": StringType(),
        "AS3": StringType(),
        "AS4": StringType(),
        "AS5": StringType(),
        "AS6": StringType(),
        "AS7": StringType(),
        "AS8": StringType(),
        "AS15": StringType(),
        "AS16": StringType(),
        "AS17": StringType(),
        "AS18": StringType(),
        "AS19": DateType(),
        "AS20": DateType(),
        "AS21": StringType(),
        "AS22": StringType(),
        "AS23": BooleanType(),
        "AS24": StringType(),
        "AS25": StringType(),
        "AS26": StringType(),
        "AS27": DoubleType(),
        "AS28": DoubleType(),
        "AS29": BooleanType(),
        "AS30": DoubleType(),
        "AS31": DateType(),
        "AS32": StringType(),
        "AS33": StringType(),
        "AS34": StringType(),
        "AS35": StringType(),
        "AS36": StringType(),
        "AS37": DoubleType(),
        "AS38": DoubleType(),
        "AS39": DoubleType(),
        "AS40": DoubleType(),
        "AS41": DoubleType(),
        "AS42": StringType(),
        "AS43": StringType(),
        "AS44": DoubleType(),
        "AS45": StringType(),
        "AS50": DateType(),
        "AS51": DateType(),
        "AS52": StringType(),
        "AS53": BooleanType(),
        "AS54": DoubleType(),
        "AS55": DoubleType(),
        "AS56": DoubleType(),
        "AS57": StringType(),
        "AS58": StringType(),
        "AS59": StringType(),
        "AS60": DoubleType(),
        "AS61": DoubleType(),
        "AS62": StringType(),
        "AS63": DoubleType(),
        "AS64": DoubleType(),
        "AS65": StringType(),
        "AS66": DoubleType(),
        "AS67": DateType(),
        "AS68": StringType(),
        "AS69": DoubleType(),
        "AS70": DateType(),
        "AS71": DateType(),
        "AS80": DoubleType(),
        "AS81": DoubleType(),
        "AS82": DoubleType(),
        "AS83": StringType(),
        "AS84": StringType(),
        "AS85": DoubleType(),
        "AS86": DoubleType(),
        "AS87": DateType(),
        "AS88": DoubleType(),
        "AS89": StringType(),
        "AS90": DoubleType(),
        "AS91": DateType(),
        "AS92": StringType(),
        "AS93": DoubleType(),
        "AS94": StringType(),
        "AS100": DoubleType(),
        "AS101": DoubleType(),
        "AS102": DoubleType(),
        "AS103": DoubleType(),
        "AS104": DoubleType(),
        "AS105": DoubleType(),
        "AS106": DoubleType(),
        "AS107": DoubleType(),
        "AS108": DoubleType(),
        "AS109": DoubleType(),
        "AS110": DoubleType(),
        "AS111": StringType(),
        "AS112": DateType(),
        "AS115": DoubleType(),
        "AS116": DoubleType(),
        "AS117": DoubleType(),
        "AS118": DoubleType(),
        "AS119": DoubleType(),
        "AS120": DoubleType(),
        "AS121": BooleanType(),
        "AS122": BooleanType(),
        "AS123": StringType(),
        "AS124": DateType(),
        "AS125": DoubleType(),
        "AS126": DoubleType(),
        "AS127": DateType(),
        "AS128": DoubleType(),
        "AS129": StringType(),
        "AS130": DateType(),
        "AS131": BooleanType(),
        "AS132": DoubleType(),
        "AS133": DateType(),
        "AS134": DateType(),
        "AS135": DoubleType(),
        "AS136": DoubleType(),
        "AS137": DateType(),
        "AS138": DoubleType(),
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
        + [f"AS{i}" for i in range(1, 15) if f"AS{i}" in df.columns],
        "obligor_info": [f"AS{i}" for i in range(15, 50) if f"AS{i}" in df.columns],
        "loan_info": [f"AS{i}" for i in range(50, 80) if f"AS{i}" in df.columns],
        "interest_rate": [f"AS{i}" for i in range(80, 100) if f"AS{i}" in df.columns],
        "financial_info": [f"AS{i}" for i in range(100, 115) if f"AS{i}" in df.columns],
        "performance_info": [
            f"AS{i}" for i in range(115, 146) if f"AS{i}" in df.columns
        ],
    }
    return cols_dict


def process_obligor_info(df, cols_dict):
    """
    Extract obligor info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["obligor_info"]
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


def process_financial_info(df, cols_dict):
    """
    Extract financial info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["financial_info"]
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
            logger.info("Generate obligor info dataframe")
            obligor_info_df = process_obligor_info(cleaned_df, assets_columns)
            logger.info("Generate loan info dataframe")
            loan_info_df = process_loan_info(cleaned_df, assets_columns)
            logger.info("Generate interest rate dataframe")
            interest_rate_df = process_interest_rate(cleaned_df, assets_columns)
            logger.info("Generate financial info dataframe")
            financial_info_df = process_financial_info(cleaned_df, assets_columns)
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
                obligor_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/obligor_info_table")
            )
            (
                financial_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/financial_info_table")
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
