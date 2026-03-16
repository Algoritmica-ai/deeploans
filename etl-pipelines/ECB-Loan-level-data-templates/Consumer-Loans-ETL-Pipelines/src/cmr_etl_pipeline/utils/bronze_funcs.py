from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
)
import csv

PRIMARY_COLS = {
    "assets": ["AN1", "AN2"],
    "bond_info": ["BN1", "BN2"],
    "deal_details": ["dl_code", "PoolCutOffDate"],
}


def get_old_df(spark, bucket_name, prefix, part_pcd, dl_code):
    """
    Return BRONZE table, but only the partitions from the specified pcds.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param part_pcd: PCD in part format to retrieve old data.
    :param dl_code: deal code to look up for legacy data.
    :return df: Spark dataframe.
    """
    storage_client = storage.Client(project="your project_id")
    partition_prefix = f"{prefix}/part={dl_code}_{part_pcd}"
    files_in_partition = [
        b.name for b in storage_client.list_blobs(bucket_name, prefix=partition_prefix)
    ]
    if len(files_in_partition) == 0:
        return None
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
    :return df: PySpark datafram for loan asset data.
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
        # Prep array of primary cols to use in checksum column
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
        # repartition = 4 instances * 8 cores each * 3 for replication factor
        df = df.repartition(96)
    if len(df.head(1)) == 0:
        return None
    return df


def perform_scd2(spark, source_df, target_df, data_type):
    """
    Perform SCD-2 to update legacy data at the bronze level tables.

    :param spark: SparkSession object.
    :param source_df: Pyspark dataframe with data from most recent filset.
    :param target_df: Pyspark dataframe with data from legacy filset.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    """
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
    return
