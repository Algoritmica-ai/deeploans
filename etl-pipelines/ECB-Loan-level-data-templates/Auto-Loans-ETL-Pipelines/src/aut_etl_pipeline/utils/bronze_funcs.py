import os
import yaml
import logging
from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
import csv

PRIMARY_COLS = {
    "assets": ["AA1", "AA2"],
    "bond_info": ["BAA1", "BAA2"],
    "deal_details": ["dl_code", "PoolCutOffDate"],
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

class NoBronzePartitionFoundException(Exception):
    """Raised when no bronze partition data is found for the requested pcd/dl_code."""
    pass

def get_old_df(spark, bucket_name, prefix, part_pcd, dl_code, config_path="config.yaml"):
    """
    Return BRONZE table, but only the partitions from the specified pcds.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param part_pcd: PCD in part format to retrieve old data.
    :param dl_code: deal code to look up for legacy data.
    :param config_path: path to config file for GCP project id.
    :return df: Spark dataframe.
    :raises NoBronzePartitionFoundException: if no files are found in the partition.
    """
    project_id = get_project_id(config_path)
    storage_client = storage.Client(project=project_id)
    partition_prefix = f"{prefix}/part={dl_code}_{part_pcd}"
    files_in_partition = [
        b.name for b in storage_client.list_blobs(bucket_name, prefix=partition_prefix)
    ]
    if len(files_in_partition) == 0:
        logger.warning(f"No files found in partition {partition_prefix}")
        raise NoBronzePartitionFoundException(
            f"No files found in partition {partition_prefix}"
        )
    else:
        df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{prefix}")
            .where(F.col("part") == f"{dl_code}_{part_pcd}")
        )
        return df

def create_dataframe(spark, csv_blob, data_type):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param csv_blob: blob object of the clean dump on GSC.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return df: PySpark dataframe for loan asset data.
    :raises Exception: if the resulting DataFrame is empty or columns are missing.
    """
    dest_csv_f = f'/tmp/{csv_blob.name.split("/")[-1]}'
    csv_blob.download_to_filename(dest_csv_f)
    col_names = []
    content = []
    with open(dest_csv_f, "r") as f:
        for i, line in enumerate(csv.reader(f)):
            if i == 0:
                col_names = line
            else:
                if len(line) == 0:
                    continue
                content.append(line)
    # Validate primary columns present
    required_cols = ["dl_code", "pcd"] + PRIMARY_COLS.get(data_type, [])
    missing_cols = [col for col in required_cols if col not in col_names]
    if missing_cols:
        raise Exception(f"Missing required columns in input csv: {missing_cols}")

    checksum_cols = [F.col("dl_code"), F.col("pcd")] + [
        F.col(col_name) for col_name in PRIMARY_COLS[data_type]
    ]
    df = (
        spark.createDataFrame(content, col_names)
        .withColumn("pcd_year", F.year(F.col("pcd")))
        .withColumn("pcd_month", F.month(F.col("pcd")))
        .withColumn(
            "valid_from", F.lit(F.current_timestamp()).cast(TimestampType())
        )
        .withColumn("valid_to", F.lit("").cast(TimestampType()))
        .withColumn("iscurrent", F.lit(1).cast("int"))
        .withColumn(
            "checksum",
            F.md5(F.concat(*checksum_cols)),
        )
        .withColumn(
            "part",
            F.concat(
                F.col("dl_code"), F.lit("_"), F.col("pcd_year"), F.col("pcd_month")
            ),
        )
    )
    # Repartition for scalability
    df = df.repartition(96)
    if df.rdd.isEmpty():
        raise Exception("The resulting dataframe from CSV is empty.")
    # Optionally, cleanup local file after use
    try:
        os.remove(dest_csv_f)
    except Exception as e:
        logger.warning(f"Could not remove temporary file {dest_csv_f}: {e}")
    return df

def perform_scd2(spark, source_df, target_df, data_type):
    """
    Perform SCD-2 to update legacy data at the bronze level tables.

    :param spark: SparkSession object.
    :param source_df: Pyspark dataframe with data from most recent fileset.
    :param target_df: Pyspark dataframe with data from legacy fileset.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :raises Exception: if PRIMARY_COLS[data_type] is missing or not a list.
    """
    if data_type not in PRIMARY_COLS or not isinstance(PRIMARY_COLS[data_type], list):
        raise Exception(f"No primary columns defined for data_type '{data_type}'")
    source_df.createOrReplaceTempView(f"delta_table_{data_type}")
    target_df.createOrReplaceTempView("staged_update")
    update_join_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in PRIMARY_COLS[data_type]]
    )
    update_col_selection = " ,".join(
        [f"{col} AS mergeKey_{i}" for i, col in enumerate(PRIMARY_COLS[data_type])]
    )
    update_qry = f"""
        SELECT NULL AS mergeKey, source.*
        FROM delta_table_{data_type} AS target
        INNER JOIN staged_update as source
        ON ({update_join_condition})
        WHERE target.checksum != source.checksum
        AND target.iscurrent = 1
    UNION
        SELECT {update_col_selection}, *
        FROM staged_update
    """
    # Upsert
    upsert_join_condition = " AND ".join(
        [
            f"target.{col} = source.mergeKey_{i}"
            for i, col in enumerate(PRIMARY_COLS[data_type])
        ]
    )
    logger.info(f"Executing SCD2 merge for {data_type}")
    spark.sql(
        f"""
        MERGE INTO delta_table_{data_type} tgt
        USING ({update_qry}) src
        ON (({upsert_join_condition}))
        WHEN MATCHED AND src.checksum != tgt.checksum AND tgt.iscurrent = 1 
        THEN UPDATE SET valid_to = src.valid_from, iscurrent = 0
        WHEN NOT MATCHED THEN INSERT *
    """
    )
    logger.info(f"SCD2 merge for {data_type} completed")
    return
