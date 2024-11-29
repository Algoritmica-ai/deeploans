import logging, sys
from typing import Callable
from pymongo import MongoClient
from google.oauth2 import service_account
from google.cloud import bigquery
from bson.objectid import ObjectId

from app import memory, config

logger = logging.getLogger(__name__)


async def _startup_tasks() -> None:
    try:
        # Upon startup, we try connecting to Big Query API
        credentials = service_account.Credentials.from_service_account_file(config.CREDENTIALS_JSON)
        big_query_client = bigquery.Client(credentials=credentials)

        # Connect to DB
        mongo_client = MongoClient(config.MONGO_URI)

        # Set up memory
        memory.memory.initialize(big_query_client=big_query_client, mongo_client=mongo_client)

    except Exception as ex:
        logger.error(f'Error on startup tasks: {str(ex)}')


async def _shutdown_tasks() -> None:
    try:
        # Close connection to DB
        memory.memory.mongo_client.close()

    except Exception as ex:
        logger.error(f'Error on shutdown tasks: {str(ex)}')


def create_start_app_handler() -> Callable:
    async def start_app() -> None:
        await _startup_tasks()

    return start_app


def create_exit_app_handler() -> Callable:
    async def exit_app() -> None:
        await _shutdown_tasks()

    return exit_app
