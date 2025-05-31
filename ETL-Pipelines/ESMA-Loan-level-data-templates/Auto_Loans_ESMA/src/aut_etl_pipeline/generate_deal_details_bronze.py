import re
import logging
import sys
from lxml import objectify
import pandas as pd
from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
)
from src.aut_etl_pipeline.utils.bronze_funcs import perform_scd2
from delta import *

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_raw_file(bucket_name, prefix, file_key):
    """
    Return list of XML files that satisfy the file_key parameter from Securitisation Repository.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param file_key: label for file name that helps with the cherry picking.
    """
    storage_client = storage.Client(project="your project_id")
    all_files = [
        b.name
        for b in storage_client.list_blobs(bucket_name, prefix=prefix)
        if (b.name.endswith(".xml")) and (file_key in b.name)
    ]
    if len(all_files) == 0:
        logger.error(
            f"No files with key {file_key.upper()} found in {bucket_name}. Exit process!"
        )
        return None
    elif len(all_files) > 1:
        logger.error(
            f"Multiple files with key {file_key.upper()} found in {bucket_name}. Exit process!"
        )
        return None
    else:
        return all_files[0]


def get_old_df(spark, bucket_name, prefix, pcd, dl_code):
    """
    Return BRONZE table.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param pcd: pool cutoff date from source files (valid only when generating TARGET dataframe).
    :param dl_code: deal code to look up for legacy data.
    :return df: Spark dataframe.
    """
    storage_client = storage.Client(project="your project_id")
    part_pcd = pcd.replace("-", "")
    partition_prefix = f"{prefix}/part={dl_code}_{part_pcd}"
    files_in_partition = [
        b.name for b in storage_client.list_blobs(bucket_name, prefix=partition_prefix)
    ]
    if files_in_partition == []:
        return None
    else:
        df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{prefix}")
            .where(f"part={dl_code}_{part_pcd}")
        )
        return df


def create_dataframe(spark, bucket_name, xml_file):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param xml_file: file to be read to generate the dataframe.
    :return df: PySpark dataframe.
    :return pcd: pool cutoff date.
    """
    storage_client = storage.Client(project="your project_id")
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(xml_file)
    dest_xml_f = f'/tmp/{xml_file.split("/")[-1]}'
    blob.download_to_filename(dest_xml_f)
    xml_data = objectify.parse(dest_xml_f)  # Parse XML data
    root = xml_data.getroot()  # Root element

    data = []
    cols = []
    for i in range(
        len(
            root.getchildren()[1]
            .getchildren()[0]
            .getchildren()[1]
            .getchildren()[0]
            .getchildren()
        )
    ):
        child = (
            root.getchildren()[1]
            .getchildren()[0]
            .getchildren()[1]
            .getchildren()[0]
            .getchildren()[i]
        )
        tag = re.sub(r"{[^}]*}", "", child.tag)
        if tag == "ISIN":
            # is array
            data.append(";".join(map(str, child.getchildren())))
            cols.append(tag)
        elif tag in ["Country", "DealVisibleToOrg", "DealVisibleToUser"]:
            # usually null values
            continue
        elif tag == "Submissions":
            # get Submission metadata
            submission_children = child.getchildren()[0].getchildren()
            for submission_child in submission_children:
                tag = re.sub(r"{[^}]*}", "", submission_child.tag)
                if tag in ["MetricData", "IsProvisional", "IsRestructured"]:
                    # Not needed
                    continue
                data.append(submission_child.text)
                cols.append(tag)
        else:
            data.append(child.text)
            cols.append(tag)
    # Convert None in empty strings if only one file is loaded. This should help to create a Spark dataframe
    data = ["" if v is None else v for v in data]
    df = pd.DataFrame(data).T  # Create DataFrame and transpose it
    df.columns = cols  # Update column names
    pcd = df["PoolCutOffDate"].values[0].split("T")[0]
    # Conver from Pandas to Spark dataframe and add SCD-2 columns
    spark_df = (
        spark.createDataFrame(df)
        .withColumnRenamed("DeeploansCode", "dl_code")
        .replace("", None)
        .withColumn("year", F.year(F.col("PoolCutOffDate")))
        .withColumn("month", F.month(F.col("PoolCutOffDate")))
        .withColumn("valid_from", F.lit(F.current_timestamp()).cast(TimestampType()))
        .withColumn("valid_to", F.lit("").cast(TimestampType()))
        .withColumn("iscurrent", F.lit(1).cast("int"))
        .withColumn(
            "checksum",
            F.md5(
                F.concat(
                    F.col("dl_code"),
                    F.col("PoolCutOffDate"),
                )
            ),
        )
        .withColumn(
            "part",
            F.concat(F.col("dl_code"), F.lit("_"), F.col("year"), F.col("month")),
        )
        .drop("year", "month")
    )
    return (pcd, spark_df)


def generate_deal_details_bronze(
    spark, raw_bucketname, data_bucketname, source_prefix, target_prefix, file_key
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect XML files.
    :param target_prefix: specific bucket prefix from where to save Delta Lake files.
    :param file_key: label for file name that helps with the cherry picking with Deal_Details.
    :return status: 0 is successful.
    """
    # Deal Details behaves differently and it has function on its own.
    # This is just a hack to use bronze_funcs.get_old_df and bronze_funcs.perform_scd2
    data_type = "deal_details"
    dl_code = source_prefix.split("/")[-1]
    logger.info("Start DEAL DETAILS BRONZE job.")
    xml_file = get_raw_file(raw_bucketname, source_prefix, file_key)
    logger.info(f"Create NEW {dl_code} dataframe")
    if len(xml_file) is None:
        logger.warning("No new XML file to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved deal details data XML files.")
        pcd, new_df = create_dataframe(spark, raw_bucketname, xml_file)

        logger.info("Retrieve OLD dataframe.")
        old_df = get_old_df(spark, data_bucketname, target_prefix, pcd, dl_code)
        if old_df is None:
            logger.info("Initial load into DEAL DETAILS BRONZE")
            (
                new_df.write.partitionBy("part")
                .format("delta")
                .mode("append")
                .save(f"gs://{data_bucketname}/{target_prefix}")
            )
        else:
            logger.info("Upsert data into DEAL DETAILS BRONZE")
            perform_scd2(spark, old_df, new_df, data_type)

    logger.info("End DEAL DETAILS BRONZE job.")
    return 0
