from google.cloud import storage
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
)
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.decorators import task
from airflow.models import Variable
import logging
import sys

# Import config
from src.aut_etl_pipeline.config import (
    PROJECT_ID, REGION, CODE_BUCKET, RAW_BUCKET, DATA_BUCKET,
    PHS_CLUSTER, METASTORE_CLUSTER,
)

SUBNETWORK_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
PYTHON_FILE_LOCATION = f"gs://{CODE_BUCKET}/dist/aut_main.py"
PHS_CLUSTER_PATH = f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{PHS_CLUSTER}"
SPARK_DELTA_JAR_FILE = f"gs://{CODE_BUCKET}/dependencies/delta-core_2.13-2.1.0.jar"
SPARK_DELTA_STORE_JAR_FILE = f"gs://{CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar"
PY_FILES = f"gs://{CODE_BUCKET}/dist/aut_etl_pipeline_0.1.0.zip"
METASTORE_SERVICE_LOCATION = (
    f"projects/{PROJECT_ID}/locations/{REGION}/services/{METASTORE_CLUSTER}"
)

ENVIRONMENT_CONFIG = {
    "execution_config": {"subnetwork_uri": SUBNETWORK_URI},
    "peripherals_config": {
        "metastore_service": METASTORE_SERVICE_LOCATION,
        "spark_history_server_config": {
            "dataproc_cluster": PHS_CLUSTER_PATH,
        },
    },
}

RUNTIME_CONFIG = {
    "properties": {
        "spark.app.name": "aut_etl_pipeline",
        "spark.executor.instances": "4",
        "spark.driver.cores": "8",
        "spark.executor.cores": "8",
        "spark.executor.memory": "16g",
    },
    "version": "2.0",
}

@task.python(trigger_rule="all_done")
@provide_session
def cleanup_xcom(session=None, **kwargs):
    dag = kwargs["dag"]
    dag_id = dag.dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

def get_raw_prefixes():
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.get_bucket(RAW_BUCKET)
    raw_prefixes = list(
        set(
            [
                "/".join(b.name.split("/")[:-1])
                for b in storage_client.list_blobs(
                    bucket.name,
                    prefix="dl_data/downloaded-data/AUT",
                )
                if b.name.endswith(".csv")
            ]
        )
    )
    return raw_prefixes

default_args = {
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,
    "retries": 0,
}

with models.DAG(
    "aut_deal_details",
    default_args=default_args,
    schedule_interval=None,
    on_success_callback=cleanup_xcom,
    on_failure_callback=cleanup_xcom,
    max_active_tasks=1,
    catchup=False,
) as dag:
    ingestion_date = Variable.get("ingestion_date", default_var=None)
    if ingestion_date is None:
        logging.error("No ingestion date set. DAG stopped!!")
        sys.exit(1)
    logging.info(f"Ingestion date: {ingestion_date}")
    raw_prefixes = get_raw_prefixes()
    for rp in raw_prefixes:
        dl_code = rp.split("/")[-1]
        start = EmptyOperator(task_id=f"{dl_code}_start")
        with TaskGroup(group_id=f"{dl_code}_deal_details") as tg:
            bronze_task = DataprocCreateBatchOperator(
                task_id=f"bronze_{dl_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            f"--source-prefix=dl_data/downloaded-data/AUT/{dl_code}",
                            "--target-prefix=AUT/bronze/deal_details",
                            "--file-key=Deal_Details",
                            "--stage-name=bronze_deal_details",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"{dl_code.lower()}-deal-details-bronze",
            )
            silver_task = DataprocCreateBatchOperator(
                task_id=f"silver_{dl_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            "--source-prefix=AUT/bronze/deal_details",
                            "--target-prefix=AUT/silver/deal_details",
                            f"--dl-code={dl_code}",
                            "--stage-name=silver_deal_details",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"{dl_code.lower()}-deal-details-silver",
            )
            bronze_task >> silver_task
        with TaskGroup(group_id=f"{dl_code}_clean_up") as clean_up_tg:
            delete_bronze = DataprocDeleteBatchOperator(
                task_id=f"delete_bronze_{dl_code}",
                project_id=PROJECT_ID,
                region=REGION,
                batch_id=f"{dl_code.lower()}-deal-details-bronze",
            )
            delete_silver = DataprocDeleteBatchOperator(
                task_id=f"delete_silver_{dl_code}",
                project_id=PROJECT_ID,
                region=REGION,
                batch_id=f"{dl_code.lower()}-deal-details-silver",
            )
        end = EmptyOperator(task_id=f"{dl_code}_end")
        start >> tg >> clean_up_tg >> end