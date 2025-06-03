import os
import sys
import logging
from google.cloud import storage
from delta import *
from src.aut_etl_pipeline.utils.bronze_funcs import (
    get_old_df,
    create_dataframe,
    perform_scd2,
)
from src.aut_etl_pipeline.config import PROJECT_ID

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def generate_bronze_tables(
    spark,
    data_bucketname,
    source_prefix,
    target_prefix,
    data_type,
    ingestion_date,
    local_mode=False
):
    """
    Carica i dati clean in tabella Bronze. Supporta modalità locale o GCS.
    """
    logger.info(f"Start {data_type.upper()} BRONZE job.")

    if local_mode:
        logger.info("Running in LOCAL mode.")
        clean_folder = os.path.join(source_prefix, "clean_dump", data_type)
        file_list = [os.path.join(clean_folder, f) for f in os.listdir(clean_folder) if f.endswith(".csv")]
        if not file_list:
            raise FileNotFoundError(f"Nessun file clean trovato in {clean_folder}")
        df = spark.read.csv(file_list, header=True, inferSchema=True)
        os.makedirs(target_prefix, exist_ok=True)
        df.write.mode("overwrite").parquet(target_prefix)
        logger.info(f"Bronze table (LOCAL) scritta in {target_prefix}")
        return 0

    # GCS mode
    dl_code = source_prefix.split("/")[-1]
    storage_client = storage.Client(project=PROJECT_ID)
    all_clean_dumps = [
        b for b in storage_client.list_blobs(
            data_bucketname, prefix=f"clean_dump/{data_type}"
        )
        if f"{ingestion_date}_{dl_code}" in b.name
    ]

    if not all_clean_dumps:
        logger.warning(
            f"Could not find clean CSV dump file from {data_type.upper()} BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)

    logger.info(f"Retrieved {len(all_clean_dumps)} clean files for {dl_code}.")

    for clean_dump_csv in all_clean_dumps:
        logger.info(f"Processing {clean_dump_csv.name}.")
        pcd = "_".join(clean_dump_csv.name.split("/")[-1].split("_")[2:4])
        part_pcd = pcd.replace("_0", "").replace("_", "")
        logger.info(f"Processing deal {dl_code}:{pcd}")

        old_df = get_old_df(spark, data_bucketname, target_prefix, part_pcd, dl_code)
        new_df = create_dataframe(spark, clean_dump_csv, data_type)

        if new_df is None:
            logger.warning("No dataframe extracted. Skipping.")
            continue

        if old_df is None:
            logger.info(f"Initial load into {data_type.upper()} BRONZE")
            (
                new_df.write.partitionBy("part")
                .format("delta")
                .mode("append")
                .save(f"gs://{data_bucketname}/{target_prefix}")
            )
        else:
            logger.info(f"Upsert not enabled. Skipping existing data (quick-fix).")
            # Uncomment for full SCD2 support:
            # perform_scd2(spark, old_df, new_df, data_type)

    logger.info(f"End {data_type.upper()} BRONZE job.")
    return 0
