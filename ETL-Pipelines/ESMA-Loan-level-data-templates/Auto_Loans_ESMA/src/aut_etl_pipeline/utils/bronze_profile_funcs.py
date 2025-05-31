from google.cloud import storage
import csv
from io import StringIO
from aut_etl_pipeline.config import GCP_PROJECT_ID

INITIAL_COL = {
    "assets": "AUTL1"
}

def _correct_file_coding(raw_file_obj):
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

def get_csv_files(bucket_name, prefix, file_key, data_type):
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    if data_type == "assets":
        all_files = [
            b.name
            for b in storage_client.list_blobs(bucket_name, prefix=prefix)
            if (b.name.endswith(".csv"))
            and (file_key in b.name)
            and not ("Labeled0M" in b.name)
        ]
    else:
        all_files = [
            b.name
            for b in storage_client.list_blobs(bucket_name, prefix=prefix)
            if (b.name.endswith(".csv")) and (file_key in b.name)
        ]
    return all_files if all_files else []

def profile_data(bucket_name, csv_f, data_type, validator):
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    col_names = []
    clean_content = []
    dirty_content = []
    try:
        blob.download_to_filename(dest_csv_f)
        with open(dest_csv_f, "rb") as f:
            clean_f = _correct_file_coding(f)
            for i, line in enumerate(csv.reader(clean_f)):
                curr_line = line
                if i == 0:
                    col_names = curr_line
                    col_names[0] = INITIAL_COL[data_type]
                elif i == 1:
                    continue
                else:
                    if len(curr_line) == 0:
                        continue
                    if len(col_names) != len(curr_line):
                        # Skip malformed rows
                        continue
                    clean_line = [
                        None if (el == "") or (el.startswith("ND")) else el
                        for el in curr_line
                    ]
                    record = {
                        col_names[j]: clean_line[j] for j in range(len(clean_line))
                    }
                    flag = validator.validate(record)
                    errors = None if flag else validator.errors
                    record["filename"] = csv_f
                    record["pcd"] = "-".join(csv_f.split("/")[-1].split("_")[1:4])
                    record["dl_code"] = csv_f.split("/")[-1].split("_")[0]
                    if not flag:
                        record["qc_errors"] = errors
                        dirty_content.append(record)
                    else:
                        clean_content.append(record)
    except Exception as e:
        dirty_content.append({"filename": csv_f, "qc_errors": str(e)})
    return (clean_content, dirty_content)