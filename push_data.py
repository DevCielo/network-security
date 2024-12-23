import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL=os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import certifi
ca = certifi.where()

import pandas as pd
import numpy as np
import pymongo
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Class for handling network data extraction and storage in MongoDB
class NetworkDataExtract():
    # Handles any initialization errors
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    # Converts a CSV file to a list of dictionaries
    def cv_to_json_convertor(self, file_path):
        try:
            # Reads CSV file
            data = pd.read_csv(file_path)

            # Removes the index for clean JSON
            data.reset_index(drop=True, inplace=True)

            # Converts the DataFrame to JSON format
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            # Creates mongo client
            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)

            # Connect to specific database
            self.database = self.mongo_client[self.database]

            # Connects to the collection inside the database
            self.collection = self.database[self.collection]

            # Inserts the records into the collection
            self.collection.insert_many(self.records)
            return(len(self.records))

        except Exception as e:
            raise NetworkSecurityException(e,sys)

if __name__ == "__main__":
    FILE_PATH="Network_Data/phisingData.csv"
    DATABASE="CIELOAI"
    Collection = "NetworkData"

    networkobj = NetworkDataExtract()
    networkobj.cv_to_json_convertor(file_path=FILE_PATH)
    records = networkobj.cv_to_json_convertor(file_path=FILE_PATH)
    print(records)

    no_of_records=networkobj.insert_data_mongodb(records,DATABASE,Collection)
    print(no_of_records)