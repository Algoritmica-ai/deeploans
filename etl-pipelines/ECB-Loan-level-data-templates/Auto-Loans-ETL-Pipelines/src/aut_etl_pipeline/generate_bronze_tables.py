import logging
import sys
import os
import yaml
from google.cloud import storage
from src.aut_etl_pipeline.utils.bronze_funcs import (
    get_old_df,
    create_dataframe,
    perform_scd2,
)
from delta import *

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


class NoCleanDumpFoundException(Exception):
    """Raised when no clean_dump CSV files are found for the current bronze step."""
    pass


def generate_bronze_tables(
    spark, data_bucketname, source_prefix, target_prefix, data_type, ingestion_date, config_path="config.yaml"
):
    """
    Esegue la generazione delle tabelle bronze per uno specifico tipo di dato.

    Args:
        spark (SparkSession): Oggetto SparkSession.
        data_bucketname (str): GS bucket dove sono archiviati i dati trasformati.
        source_prefix (str): Prefisso del bucket da cui prelevare i nuovi dati bronze.
        target_prefix (str): Prefisso del bucket dove sono archiviati i dati bronze storici.
        data_type (str): Tipo di dato da gestire (es: amortisation, assets, collaterals).
        ingestion_date (str): Data di ingestione ETL.
        config_path (str): Percorso al file di configurazione (default: config.yaml).

    Returns:
        int: 0 se eseguito con successo.

    Raises:
        NoCleanDumpFoundException: se non vengono trovati file clean_dump da elaborare.

    Workflow:
        - Ricerca i file clean_dump generati dal profiling bronze per il tipo e data specificati.
        - Se nessun file viene trovato, logga e solleva eccezione.
        - Per ogni clean_dump trovato:
            - Estrae il codice deal e altri identificativi.
            - Recupera il dataframe storico se esiste, altrimenti crea una nuova tabella bronze.
            - Se non si riesce a costruire il dataframe dai nuovi dati, logga e passa al successivo.
            - (TODO) Prevede upsert tramite SCD2 ma attualmente fa solo append.

    Side effects:
        - Scrive i dati bronze su Google Cloud Storage in formato Delta.
        - Scrive log su stdout.
    """
    logger.info(f"Start {data_type.upper()} BRONZE job.")
    dl_code = source_prefix.split("/")[-1]
    project_id = get_project_id(config_path)
    storage_client = storage.Client(project=project_id)
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(
            data_bucketname, prefix=f"clean_dump/{data_type}"
        )
        if f"{ingestion_date}_{dl_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            f"Could not find clean CSV dump file from {data_type.upper()} BRONZE PROFILING job. Workflow stopped!"
        )
        raise NoCleanDumpFoundException(f"No clean_dump files found for {data_type}, ingestion_date {ingestion_date}, dl_code {dl_code}.")
    else:
        logger.info(f"Create NEW {dl_code} dataframe")
        for clean_dump_csv in all_clean_dumps:
            logger.info(f"Processing {clean_dump_csv.name}.")
            pcd = "_".join(clean_dump_csv.name.split("/")[-1].split("_")[2:4])
            logger.info(f"Processing data for deal {dl_code}:{pcd}")
            part_pcd = pcd.replace("_0", "").replace("_", "")
            logger.info(f"Retrieve OLD {dl_code} dataframe. Use following PCD: {pcd}")
            old_df = get_old_df(
                spark, data_bucketname, target_prefix, part_pcd, dl_code
            )
            new_df = create_dataframe(spark, clean_dump_csv, data_type)
            if new_df is None:
                logger.error("No dataframes were extracted from file. Skip!")
                continue
            else:
                if old_df is None:
                    logger.info(f"Initial load into {data_type.upper()} BRONZE")
                    (
                        new_df.write.partitionBy("part")
                        .format("delta")
                        .mode("append")
                        .save(f"gs://{data_bucketname}/{target_prefix}")
                    )
                else:
                    # logger.info(f"Upsert data into {data_type.upper()} BRONZE")
                    # perform_scd2(spark, old_df, new_df, data_type)
                    # TODO quick-fix
                    continue

    logger.info(f"End {data_type.upper()} BRONZE job.")
    return 0
