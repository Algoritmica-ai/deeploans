import os
import sys
import logging
import pandas as pd
from cerberus import Validator
from google.cloud import storage
from src.aut_etl_pipeline.utils.bronze_profile_funcs import (
    get_csv_files,
    profile_data,
)
from src.aut_etl_pipeline.utils.validation_rules import asset_schema
from aut_etl_pipeline.config import PROJECT_ID

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def profile_bronze_data(
    raw_bucketname,
    data_bucketname,
    source_prefix,
    file_key,
    data_type,
    ingestion_date,
    local_mode=False,
    local_output_path=None
):
    """
    Esegue profiling e validazione su dati CSV in modalità locale o su GCS.

    Se local_mode: usa file locali, scrive su disco locale.
    Altrimenti: legge e scrive su Google Cloud Storage.
    """
    logger.info(f"Start {data_type.upper()} BRONZE PROFILING job.")


    if data_type == "assets":
        validator = Validator(asset_schema())
    else:
        logger.error(f"No schema defined for data_type: {data_type}")
        sys.exit(1)

    if local_mode:
        # Profiling locale
        logger.info("Running in LOCAL mode.")
        if os.path.isdir(source_prefix):
            files = [os.path.join(source_prefix, f) for f in os.listdir(source_prefix) if f.endswith(".csv")]
        else:
            files = [source_prefix] if source_prefix.endswith(".csv") else []
        clean_rows = []
        dirty_rows = []
        for file in files:
            logger.info(f"Processing local file: {file}")
            try:
                df = pd.read_csv(file, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(file, encoding="latin1")
            for idx, row in df.iterrows():
                record = row.to_dict()
                if validator.validate(record):
                    clean_rows.append(record)
                else:
                    dirty_record = record.copy()
                    dirty_record["__errors__"] = validator.errors
                    dirty_rows.append(dirty_record)
        # Output
        output_clean = os.path.join(local_output_path, "clean_dump", data_type)
        output_dirty = os.path.join(local_output_path, "dirty_dump", data_type)
        os.makedirs(output_clean, exist_ok=True)
        os.makedirs(output_dirty, exist_ok=True)
        clean_path = os.path.join(output_clean, f"clean_{file_key}_{ingestion_date}.csv")
        dirty_path = os.path.join(output_dirty, f"dirty_{file_key}_{ingestion_date}.csv")
        pd.DataFrame(clean_rows).to_csv(clean_path, index=False)
        pd.DataFrame(dirty_rows).to_csv(dirty_path, index=False)
        logger.info(f"Profilazione completata (LOCAL): {len(clean_rows)} clean, {len(dirty_rows)} dirty")
    else:
        # Profiling su GCS
        dl_code = source_prefix.split("/")[-1]
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.get_bucket(data_bucketname)

        all_new_files = get_csv_files(raw_bucketname, source_prefix, file_key, data_type)
        if len(all_new_files) == 0:
            logger.warning("No new CSV files to retrieve. Workflow stopped!")
            sys.exit(1)

        logger.info(f"Retrieved {len(all_new_files)} {data_type} data CSV files.")
        for new_file_name in all_new_files:
            logger.info(f"Checking {new_file_name}..")
            pcd = "_".join(new_file_name.split("/")[-1].split("_")[1:4])
            clean_blob_path = f"clean_dump/{data_type}/{ingestion_date}_{dl_code}_{pcd}.csv"
            clean_dump_csv = bucket.blob(clean_blob_path)

            if clean_dump_csv.exists():
                logger.info(f"{clean_blob_path} already exists. Skipping.")
                continue

            clean_content, dirty_content = profile_data(raw_bucketname, new_file_name, data_type, validator)

            if dirty_content:
                logger.info(f"Found {len(dirty_content)} failed records.")
                dirty_df = pd.DataFrame(dirty_content)
                bucket.blob(f"dirty_dump/{data_type}/{ingestion_date}_{dl_code}_{pcd}.csv") \
                      .upload_from_string(dirty_df.to_csv(index=False), "text/csv")
            else:
                logger.info("No failed records found.")

            if clean_content:
                logger.info(f"Found {len(clean_content)} clean records.")
                clean_df = pd.DataFrame(clean_content)
                bucket.blob(clean_blob_path) \
                      .upload_from_string(clean_df.to_csv(index=False), "text/csv")
            else:
                logger.info("No passed records found. Skipping clean upload.")

    logger.info(f"End {data_type.upper()} BRONZE PROFILING job.")
    return 0
