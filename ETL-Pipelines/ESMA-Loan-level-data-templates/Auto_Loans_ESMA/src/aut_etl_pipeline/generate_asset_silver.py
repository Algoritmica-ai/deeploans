import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from google.cloud import storage
from src.aut_etl_pipeline.utils.silver_funcs import (
    replace_no_data,
    replace_bool_data,
    cast_to_datatype,
)
from src.aut_etl_pipeline.config import GCP_PROJECT_ID

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
    """
    # TODO: Aggiorna questi parametri con lo schema ESMA (AUTL*) e tipologie corrette
    config = {}
    config["DATE_COLUMNS"] = [
        "AUTL6", 
        "AUTL7", 
        "AUTL8", 
        "AUTL9", 
        "AUTL24", 
        "AUTL25", 
        "AUTL33", 
        "AUTL50", 
        "AUTL51", 
        "AUTL65", 
        "AUTL66", 
        "AUTL67", 
        "AUTL73"
        ]

    config["ASSET_COLUMNS"] = {
    "AUTL1": StringType(),
    "AUTL2": StringType(),
    "AUTL3": StringType(),
    "AUTL4": StringType(),
    "AUTL5": StringType(),
    "AUTL10": StringType(),
    "AUTL11": StringType(),
    "AUTL12": StringType(),
    "AUTL13": StringType(),
    "AUTL14": StringType(),
    "AUTL15": StringType(),
    "AUTL16": NumberType(),
    "AUTL17": StringType(),
    "AUTL18": StringType(),
    "AUTL19": StringType(),
    "AUTL20": NumberType(),
    "AUTL21": StringType(),
    "AUTL22": StringType(),
    "AUTL23": StringType(),
    "AUTL26": IntegerType(),
    "AUTL27": StringType(),
    "AUTL28": StringType(),
    "AUTL29": NumberType(),
    "AUTL30": NumberType(),
    "AUTL31": NumberType(),
    "AUTL32": StringType(),
    "AUTL34": StringType(),
    "AUTL35": StringType(),
    "AUTL36": StringType(),
    "AUTL37": NumberType(),
    "AUTL38": NumberType(),
    "AUTL39": NumberType(),
    "AUTL40": NumberType(),
    "AUTL41": StringType(),
    "AUTL42": StringType(),
    "AUTL43": NumberType(),
    "AUTL44": IntegerType(),
    "AUTL45": NumberType(),
    "AUTL46": NumberType(),
    "AUTL47": IntegerType(),
    "AUTL48": NumberType(),
    "AUTL49": NumberType(),
    "AUTL52": NumberType(),
    "AUTL53": StringType(),
    "AUTL54": StringType(),
    "AUTL55": StringType(),
    "AUTL56": StringType(),
    "AUTL57": StringType(),
    "AUTL58": StringType(),
    "AUTL59": NumberType(),
    "AUTL60": NumberType(),
    "AUTL61": NumberType(),
    "AUTL62": NumberType(),
    "AUTL63": NumberType(),
    "AUTL64": NumberType(),
    "AUTL68": NumberType(),
    "AUTL69": IntegerType(),
    "AUTL70": StringType(),
    "AUTL71": StringType(),
    "AUTL72": NumberType(),
    "AUTL74": NumberType(),
    "AUTL75": NumberType(),
    "AUTL76": NumberType(),
    "AUTL77": NumberType(),
    "AUTL78": NumberType(),
    "AUTL79": StringType(),
    "AUTL80": StringType(),
    "AUTL81": StringType(),
    "AUTL82": StringType(),
    "AUTL83": StringType(),
    "AUTL84": StringType()
}
    return config


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.
    """
    # Aggiorna i range secondo schema ESMA
    cols_dict = {
        "general": ["dl_code", "pcd_year", "pcd_month"] + [c for c in df.columns if c.startswith("AUTL") and int(c[4:]) <= 5],
        "lease_info": [c for c in df.columns if c.startswith("AUTL") and int(c[4:]) > 5],
    }
    return cols_dict


def process_lease_info(df, cols_dict):
    """
    Extract lease info dimension from bronze Spark dataframe.
    """
    new_df = df.select(cols_dict["general"] + cols_dict["lease_info"]).dropDuplicates()
    return new_df


def generate_asset_silver(
    spark, bucket_name, source_prefix, target_prefix, dl_code, ingestion_date
):
    """
    Run main steps of the module.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(bucket_name, prefix="clean_dump/assets")
        if f"{ingestion_date}_{dl_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            "Could not find clean CSV dump file from ASSETS BRONZE PROFILING job. Workflow stopped!"
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