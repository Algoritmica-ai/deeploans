import logging
import sys
from google.cloud import storage
import pandas as pd
from cerberus import Validator
from src.aut_etl_pipeline.utils.bronze_profile_funcs import (
    get_csv_files,
    profile_data,
)
from src.aut_etl_pipeline.utils.validation_rules import asset_schema, bond_info_schema

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def profile_bronze_data(
    raw_bucketname, data_bucketname, source_prefix, file_key, data_type, ingestion_date
):
    """
    Run main steps of the module.

    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect source files.
    :param file_key: label for file name that helps with the cherry picking with data_type.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param ingestion_date: date of the ETL ingestion.
    :return status: 0 if successful.
    """
    logger.info(f"Start {data_type.upper()} BRONZE PROFILING job.")
    dl_code = source_prefix.split("/")[-1]
    storage_client = storage.Client(project="your project_id")
    bucket = storage_client.get_bucket(data_bucketname)
    # Pick Cerberus validator
    if data_type == "assets":
        validator = Validator(asset_schema())
    if data_type == "bond_info":
        validator = Validator(bond_info_schema())
    # Get all CSV files from Securitisation Repository.
    logger.info(f"Profile {dl_code} files.")
    all_new_files = get_csv_files(raw_bucketname, source_prefix, file_key, data_type)
    if len(all_new_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_new_files)} {data_type} data CSV files.")
        clean_content = []
        dirty_content = []
        for new_file_name in all_new_files:
            logger.info(f"Checking {new_file_name}..")
            pcd = "_".join(new_file_name.split("/")[-1].split("_")[1:4])
            clean_dump_csv = bucket.blob(
                f"clean_dump/{data_type}/{ingestion_date}_{dl_code}_{pcd}.csv"
            )
            # Check if this file has already been profiled. Skip in this case.
            if clean_dump_csv.exists():
                logger.info(
                    f"{clean_dump_csv} BRONZE PROFILING job has been already done. Skip!"
                )
                continue
            clean_content, dirty_content = profile_data(
                raw_bucketname, new_file_name, data_type, validator
            )
            if dirty_content == []:
                logger.info("No failed records found.")
            else:
                logger.info(f"Found {len(dirty_content)} failed records found.")
                dirty_df = pd.DataFrame(data=dirty_content)
                bucket.blob(
                    f"dirty_dump/{data_type}/{ingestion_date}_{dl_code}_{pcd}.csv"
                ).upload_from_string(dirty_df.to_csv(index=False), "text/csv")
            if clean_content == []:
                logger.info("No passed records found. Skip!")
                continue
            else:
                logger.info(f"Found {len(clean_content)} clean CSV found.")
                clean_df = pd.DataFrame(data=clean_content)
                bucket.blob(
                    f"clean_dump/{data_type}/{ingestion_date}_{dl_code}_{pcd}.csv"
                ).upload_from_string(clean_df.to_csv(index=False), "text/csv")
            # START DEBUG ONLY 1 FILE
            # break
            # END DEBUG
    logger.info(f"End {data_type.upper()} BRONZE PROFILING job.")
    return 0
