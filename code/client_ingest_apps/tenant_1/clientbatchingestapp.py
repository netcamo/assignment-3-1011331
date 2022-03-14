import os
from datetime import datetime
import sys
UTILS_PATH = os.getcwd()
print(os.getcwd())
sys.path.append(UTILS_PATH)
import csv
import requests
import logging
import pandas as pd
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from mysimbdp import MySimBdpClient  # provided by mysimbdp



configuration = {
    "tenant_id": "tenant_1",
    "tables": [
        {
            "table_name": "listings",
            "primary_key": ["host_id", "id"],
            "schema": [
                {"field": "id", "type": "int"},
                {"field": "host_id", "type": "int"},
                {"field": "host_name", "type": "text"},
                {"field": "neighbourhood", "type": "text"},
                {"field": "latitude", "type": "float"},
                {"field": "longitude", "type": "float"},
                {"field": "room_type", "type": "text"},
                {"field": "price", "type": "int"},
                {"field": "availability_365", "type": "int"}
            ]
        }
    ]
}


    
       


if __name__ == '__main__':

    if len(sys.argv) >= 3:
        table_name = sys.argv[1]  # get table name
        file_name = sys.argv[2]  # get file name

    # 2. compose json ingest task
    ingest_task = {
        "table_name": table_name,
        "file_name": file_name
    }

    client = MySimBdpClient()

    # 4. use mysimbdp client to execute the ingest job
    result = client.start_batch_ingest_job(configuration, ingest_task)
    print(result)



   