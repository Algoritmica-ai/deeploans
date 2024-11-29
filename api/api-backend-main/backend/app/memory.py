# This file contains runtime variables that are used throughout the app
import sys
from google.cloud.bigquery import Client
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

from app import config


class Memory:
    big_query_client: Client

    mongo_client: MongoClient
    mongo_db: Database

    mongo_users: Collection
    mongo_tables: Collection

    def initialize(self, big_query_client: Client, mongo_client: MongoClient):
        self.big_query_client = big_query_client

        self.mongo_client = mongo_client
        self.mongo_db = mongo_client[config.MONGO_DB]

        self.mongo_users = self.mongo_db['users']
        self.mongo_tables = self.mongo_db['tables']


memory: Memory = Memory()  # TODO Change please
