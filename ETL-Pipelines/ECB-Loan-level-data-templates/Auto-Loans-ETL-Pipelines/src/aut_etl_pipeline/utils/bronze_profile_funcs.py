import os
import yaml
import logging
from google.cloud import storage
import csv
from io import StringIO

INITIAL_COL = {
    "assets": "AA1",
    "bond_info": "BAA1",
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
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

def _correct_file_coding(raw_file_obj):
    """
    Check that the content of the CSV does not contain coding issues from Securitisation Repository.

    :param raw_file_obj: file handler from the raw CSV.
    :return clean_file_obj: file object without coding issues.
    """
    file_check = (
        raw_file_obj.read()
        .decode("unicode-escape")
        .encode("utf-8")
        .decode("utf-8", "backslashreplace")
        .replace("\ufeff", "")
    )
    if "\0" in file_check:
        file_check = file_check.replace("\0", "")
    if "\x00" in file_check:
        file_check = file_check.replace("\x00", "")
    clean_file_obj = StringIO(file_check)
    return clean_file_obj

def get_csv_files(bucket_name, prefix, file_key, data_type, config_path="config.yaml"):
    """
    Return list of source files that satisfy the file_key parameter from Securitisation Repository.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param file_key: label for file name that helps with the cherry picking.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param config_path: path to config file for GCP project id.
    :return all_files: list of desired files from source_dir.
    """
    project_id = get_project_id(config_path)
    storage_client = storage.Client(project=project_id)
    if data_type == "assets":
        all_files = [
            b.name
            for b in storage_client.list_blobs(bucket_name, prefix=prefix)
            if (b.name.endswith(".csv"))
            and (file_key in b.name)
            and not ("Labeled0M" in b.name)  # This is generated internally by data scientist working on predictive analytics
        ]
    else:
        all_files = [
            b.name
            for b in storage_client.list_blobs(bucket_name, prefix=prefix)
            if (b.name.endswith(".csv")) and (file_key in b.name)
        ]
    if len(all_files) == 0:
        logger.warning(f"No CSV files found in bucket {bucket_name} with prefix {prefix} and key {file_key}")
        return []
    else:
        logger.info(f"Found {len(all_files)} CSV files in {bucket_name}/{prefix} for key '{file_key}' and data_type '{data_type}'")
        return all_files

def profile_data(bucket_name, csv_f, data_type, validator, config_path="config.yaml"):
    """
    Check whether the file is ok to be stored in the bronze layer or not.

    :param bucket_name: GS bucket where files are stored.
    :param csv_f: CSV file to be read and profile.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param validator: Cerberus validator object.
    :param config_path: path to config file for GCP project id.
    :return profile_flag: CSV files is dirty or clean.
    :return error_text: if CSV is dirty provide reason, None otherwise.
    """
    project_id = get_project_id(config_path)
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    try:
        blob.download_to_filename(dest_csv_f)
    except Exception as e:
        logger.error(f"Error downloading {csv_f}: {e}")
        return [], [{"filename": csv_f, "qc_errors": f"Download failed: {e}"}]
    col_names = []
    clean_content = []
    dirty_content = []
    try:
        with open(dest_csv_f, "rb") as f:
            clean_f = _correct_file_coding(f)
            for i, line in enumerate(csv.reader(clean_f)):
                curr_line = line
                if i == 0:
                    col_names = curr_line
                    col_names[0] = INITIAL_COL.get(data_type, col_names[0])
                elif i == 1:
                    continue
                else:
                    if len(curr_line) == 0:
                        continue
                    clean_line = [
                        None if (el == "") or (el.startswith("ND")) else el
                        for el in curr_line
                    ]
                    # Defensive: avoid index out of range if row is shorter than header
                    if len(clean_line) < len(col_names):
                        clean_line += [None] * (len(col_names) - len(clean_line))
                    record = {
                        col_names[j]: clean_line[j] for j in range(len(col_names))
                    }
                    flag = validator.validate(record)
                    errors = None if flag else validator.errors
                    record["filename"] = csv_f
                    record["pcd"] = "-".join(csv_f.split("/")[-1].split("_")[1:4])
                    record["dl_code"] = csv_f.split("/")[-1].split("_")[0]
                    if not flag:
                        # Does not pass validation
                        record["qc_errors"] = errors
                        dirty_content.append(record)
                    else:
                        clean_content.append(record)
    except Exception as e:
        logger.error(f"Profiling failed for {csv_f}: {e}")
        dirty_content.append({"filename": csv_f, "qc_errors": str(e)})
    finally:
        # Clean up temp file
        try:
            os.remove(dest_csv_f)
        except Exception as e:
            logger.warning(f"Could not remove temporary file {dest_csv_f}: {e}")
    return (clean_content, dirty_content)
