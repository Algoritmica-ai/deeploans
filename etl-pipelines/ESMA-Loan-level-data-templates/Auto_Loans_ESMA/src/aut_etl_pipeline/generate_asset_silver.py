import os
import sys
import logging
from pyspark.sql.functions import col, length
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType, IntegerType
from google.cloud import storage
from src.aut_etl_pipeline.utils.silver_funcs import (
    replace_no_data,
    replace_bool_data,
    cast_to_datatype,
)
from src.aut_etl_pipeline.config import PROJECT_ID

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def set_job_params():
    """
    Setup schema config for the asset silver job.
    """
    date_columns = [
        "AUTL6", "AUTL7", "AUTL8", "AUTL9", "AUTL24", "AUTL25",
        "AUTL33", "AUTL50", "AUTL51", "AUTL65", "AUTL66", "AUTL67", "AUTL73"
    ]

    asset_columns = {
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
    "AUTL16": DoubleType(),
    "AUTL17": StringType(),
    "AUTL18": StringType(),
    "AUTL19": StringType(),
    "AUTL20": DoubleType(),
    "AUTL21": StringType(),
    "AUTL22": StringType(),
    "AUTL23": StringType(),
    "AUTL26": IntegerType(),
    "AUTL27": StringType(),
    "AUTL28": StringType(),
    "AUTL29": DoubleType(),
    "AUTL30": DoubleType(),
    "AUTL31": DoubleType(),
    "AUTL32": StringType(),
    "AUTL34": StringType(),
    "AUTL35": StringType(),
    "AUTL36": StringType(),
    "AUTL37": DoubleType(),
    "AUTL38": DoubleType(),
    "AUTL39": DoubleType(),
    "AUTL40": DoubleType(),
    "AUTL41": StringType(),
    "AUTL42": StringType(),
    "AUTL43": DoubleType(),
    "AUTL44": IntegerType(),
    "AUTL45": DoubleType(),
    "AUTL46": DoubleType(),
    "AUTL47": IntegerType(),
    "AUTL48": DoubleType(),
    "AUTL49": DoubleType(),
    "AUTL52": DoubleType(),
    "AUTL53": StringType(),
    "AUTL54": StringType(),
    "AUTL55": StringType(),
    "AUTL56": StringType(),
    "AUTL57": StringType(),
    "AUTL58": StringType(),
    "AUTL59": DoubleType(),
    "AUTL60": DoubleType(),
    "AUTL61": DoubleType(),
    "AUTL62": DoubleType(),
    "AUTL63": DoubleType(),
    "AUTL64": DoubleType(),
    "AUTL68": DoubleType(),
    "AUTL69": IntegerType(),
    "AUTL70": StringType(),
    "AUTL71": StringType(),
    "AUTL72": DoubleType(),
    "AUTL74": DoubleType(),
    "AUTL75": DoubleType(),
    "AUTL76": DoubleType(),
    "AUTL77": DoubleType(),
    "AUTL78": DoubleType(),
    "AUTL79": StringType(),
    "AUTL80": StringType(),
    "AUTL81": StringType(),
    "AUTL82": StringType(),
    "AUTL83": StringType(),
    "AUTL84": StringType()
}
    return {"DATE_COLUMNS": date_columns, "ASSET_COLUMNS": asset_columns}


def get_columns_collection(df):
    """
    Divide le colonne in generiche e specifiche lease_info (secondo schema AUTL).
    """
    return {
        "general": ["dl_code", "pcd_year", "pcd_month"] + [c for c in df.columns if c.startswith("AUTL") and int(c[4:]) <= 5],
        "lease_info": [c for c in df.columns if c.startswith("AUTL") and int(c[4:]) > 5],
    }


def process_lease_info(df, cols_dict):
    """
    Estrai sottoinsieme lease info con eliminazione duplicati.
    """
    return df.select(cols_dict["general"] + cols_dict["lease_info"]).dropDuplicates()


def generate_asset_silver(
    spark,
    bucket_name,
    source_prefix,
    target_prefix,
    dl_code,
    ingestion_date,
    local_mode=False
):
    """
    Genera tabella Silver degli asset da Bronze, in modalità locale o su GCS.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()

    if local_mode:
        # LOCAL MODE
        bronze_path = source_prefix
        df = spark.read.parquet(bronze_path)

        logger.info("Apply enrichment for LOCAL mode")
        if df.columns:
            col_da_arricchire = df.columns[0]
            df_silver = df.withColumn("field_length", length(df[col_da_arricchire]))
        else:
            df_silver = df

        os.makedirs(target_prefix, exist_ok=True)
        df_silver.write.mode("overwrite").parquet(target_prefix)
        logger.info(f"Silver table (LOCAL) scritta in {target_prefix}")
        return 0

    # GCS MODE
    storage_client = storage.Client(project=PROJECT_ID)
    all_clean_dumps = [
        b for b in storage_client.list_blobs(bucket_name, prefix="clean_dump/assets")
        if f"{ingestion_date}_{dl_code}" in b.name
    ]

    if not all_clean_dumps:
        logger.warning("Nessun file clean trovato. Interrompo il job.")
        sys.exit(1)

    for clean_dump_csv in all_clean_dumps:
        pcd = "_".join(clean_dump_csv.name.split("/")[-1].split("_")[2:4])
        part_pcd = pcd.replace("_0", "").replace("_", "")
        logger.info(f"Processing deal {dl_code}:{pcd}")

        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{source_prefix}")
            .where(col("part") == f"{dl_code}_{part_pcd}")
            .filter(col("iscurrent") == 1)
            .drop("valid_from", "valid_to", "checksum", "iscurrent")
        )

        logger.info("Applico pulizia e trasformazioni.")
        cols_dict = get_columns_collection(bronze_df)
        tmp_df1 = replace_no_data(bronze_df)
        tmp_df2 = replace_bool_data(tmp_df1)
        cleaned_df = cast_to_datatype(tmp_df2, run_props["ASSET_COLUMNS"])

        logger.info("Genero il lease_info dataframe.")
        lease_info_df = process_lease_info(cleaned_df, cols_dict)

        logger.info("Scrivo il lease_info parquet in GCS.")
        (
            lease_info_df.write.format("parquet")
            .partitionBy("pcd_year", "pcd_month")
            .mode("append")
            .save(f"gs://{bucket_name}/{target_prefix}/lease_info_table")
        )

    logger.info("Pulizia: eliminazione file clean dump.")
    for clean_dump_csv in all_clean_dumps:
        clean_dump_csv.delete()

    logger.info("End ASSET SILVER job.")
    return 0