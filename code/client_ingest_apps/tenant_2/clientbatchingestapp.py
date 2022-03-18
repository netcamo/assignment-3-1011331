import os
from datetime import datetime
import sys

print(os.getcwd())
sys.path.append(os.getcwd())
import csv
import requests
import logging
import pandas as pd
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from mysimbdp import MySimBdp_CommonTool  # provided by mysimbdp



configuration = {
    "tenant_id": "tenant_2",
    "tables": [
        {
            "table_name": "another_listing",
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
        },
         {
            "table_name": "New_table_2",
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
        table_name = sys.argv[1]  
        file_name = sys.argv[2] 
    else :
        print("Provide table name and file name")
        exit
   
    source_endpoint = {
        "table_name": table_name,
        "file_name": file_name
    }

    client = MySimBdp_CommonTool()
    result = client.initiate_batch_ingest(configuration, source_endpoint)
    print(result)



   
   