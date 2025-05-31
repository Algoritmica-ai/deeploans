from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
)
import csv
from aut_etl_pipeline.config import GCP_PROJECT_ID

PRIMARY_COLS = {
    "assets": ["AUTL1", "AUTL2"],
    "deal_details": ["dl_code", "PoolCutOffDate"],
}

def get_old_df(spark, bucket_name, prefix, part_pcd, dl_code):
    storage_client = storage.Client(project=GCP_PROJECT_ID)
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
                if len(col_names) != len(line):
                    # Skip malformed rows
                    continue
                content.append(line)
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
            .withColumn("valid_to", F.lit(None).cast(TimestampType()))
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
        df = df.repartition(96)
    if len(df.head(1)) == 0:
        return None
    return df

def perform_scd2(spark, source_df, target_df, data_type):
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