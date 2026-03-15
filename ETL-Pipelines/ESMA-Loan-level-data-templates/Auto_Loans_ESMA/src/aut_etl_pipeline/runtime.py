import logging
import os
import sys

from google.cloud import storage

from aut_etl_pipeline.config import PROJECT_ID


def get_project_id() -> str:
    """Return project id with env override over static config."""
    return os.getenv("GOOGLE_CLOUD_PROJECT_ID", PROJECT_ID)


def get_storage_client() -> storage.Client:
    return storage.Client(project=get_project_id())


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
