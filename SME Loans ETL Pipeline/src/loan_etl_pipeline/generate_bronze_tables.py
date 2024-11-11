import logging
import sys
from google.cloud import storage
from src.loan_etl_pipeline.utils.bronze_funcs import (
    get_old_df,
    create_dataframe,
    perform_scd2,
)
from delta import *

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def generate_bronze_tables(
    spark, data_bucketname, source_prefix, target_prefix, data_type, ingestion_date
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze new data.
    :param target_prefix: specific bucket prefix from where to collect bronze old data.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param ingestion_date: date of the ETL ingestion.
    :return status: 0 if successful.
    """
    logger.info(f"Start {data_type.upper()} BRONZE job.")
    dl_code = source_prefix.split("/")[-1]
    storage_client = storage.Client(project="your project_id")
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(
            data_bucketname, prefix=f"clean_dump/{data_type}"
        )
        if f"{ingestion_date}_{dl_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            f"Could not find clean CSV dump file from {data_type.upper()} BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)
    else:
        logger.info(f"Create NEW {dl_code} dataframe")
        for clean_dump_csv in all_clean_dumps:
            logger.info(f"Processing {clean_dump_csv.name}.")
            pcd = "_".join(clean_dump_csv.name.split("/")[-1].split("_")[2:4])
            logger.info(f"Processing data for deal {dl_code}:{pcd}")
            part_pcd = pcd.replace("_0", "").replace("_", "")
            logger.info(f"Retrieve OLD {dl_code} dataframe. Use following PCD: {pcd}")
            old_df = get_old_df(
                spark, data_bucketname, target_prefix, part_pcd, dl_code
            )
            new_df = create_dataframe(spark, clean_dump_csv, data_type)
            if new_df is None:
                logger.error("No dataframes were extracted from file. Skip!")
                continue
            else:
                if old_df is None:
                    logger.info(f"Initial load into {data_type.upper()} BRONZE")
                    (
                        new_df.write.partitionBy("part")
                        .format("delta")
                        .mode("append")
                        .save(f"gs://{data_bucketname}/{target_prefix}")
                    )
                else:
                    logger.info(f"Upsert data into {data_type.upper()} BRONZE")
                    perform_scd2(spark, old_df, new_df, data_type)

    logger.info(f"End {data_type.upper()} BRONZE job.")
    return 0
