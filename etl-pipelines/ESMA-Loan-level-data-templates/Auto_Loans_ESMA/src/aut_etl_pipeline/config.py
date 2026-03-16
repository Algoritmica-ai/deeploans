# Configurazione centralizzata per ETL ESMA Auto Loans

# GCP e bucket
PROJECT_ID = "your_project_id"
REGION = "your_region"
CODE_BUCKET = "your_code_bucket"
RAW_BUCKET = "your_raw_bucket"
DATA_BUCKET = "your_data_bucket"
PHS_CLUSTER = "your_phs_cluster"  # Dataproc Spark History Server cluster name
METASTORE_CLUSTER = "your_metastore_cluster"

# Dataproc e Spark
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

# Delta Lake
DELTA_CORE_VERSION = "2.1.0"
DELTA_LOGSTORE_IMPL = "io.delta.storage.GCSLogStore"

# Prefissi convenzionali per i path
ASSETS_BRONZE_PREFIX = "AUT/bronze/assets"
ASSETS_SILVER_PREFIX = "AUT/silver/assets"
DEAL_DETAILS_BRONZE_PREFIX = "AUT/bronze/deal_details"
DEAL_DETAILS_SILVER_PREFIX = "AUT/silver/deal_details"

# Aggiorna qui con altri parametri di configurazione globali rilevanti