import logging
import sys
import os
import yaml
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


def profile_bronze_data(
    raw_bucketname, data_bucketname, source_prefix, file_key, data_type, ingestion_date, config_path="config.yaml"
):
    """
    Execute profiling (validation and separation of clean/dirty records) for the specified bronze data type.

    Args:
        raw_bucketname (str): GS bucket where raw files are stored.
        data_bucketname (str): GS bucket where transformed files are stored.
        source_prefix (str): Specific bucket prefix from where to collect source files.
        file_key (str): Label for file name that helps with cherry picking by data_type.
        data_type (str): Type of data to handle (e.g., amortisation, assets, collaterals, bond_info).
        ingestion_date (str): Date of the ETL ingestion.
        config_path (str): Path to the configuration file (default: config.yaml).

    Returns:
        int: 0 if successful, exits with code 1 if no new files are found or other errors occur.

    Workflow:
        - Selects validation schema based on data_type.
        - Retrieves relevant CSV files from the raw bucket.
        - For each file, skips processing if already profiled.
        - Profiles the file (validation), separating "clean" and "dirty" records.
        - Uploads clean and dirty records to separate locations in the target bucket.
        - Logs all major steps and outcomes.

    Side effects:
        - Uploads profiled CSVs to Google Cloud Storage.
        - Writes logs to stdout.
        - Exits the process if no files are found.
    """
    logger.info(f"Start {data_type.upper()} BRONZE PROFILING job.")
    dl_code = source_prefix.split("/")[-1]
    project_id = get_project_id(config_path)
    storage_client = storage.Client(project=project_id)
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
