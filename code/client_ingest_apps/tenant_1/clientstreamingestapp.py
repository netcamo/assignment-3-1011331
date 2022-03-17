import os
from datetime import datetime
import sys
import csv
import requests
import logging
import pandas as pd
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from mysimbdp import MySimBdp_CommonTool
import json




    
       
class ClienStreamIngestApp():
    

    def stream_ingest(self, table_name, data):
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
                    },
                    {
                        "table_name": "Listing_table_2",
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

        table_n_data= {
        "table_name": table_name,
        "data": json.loads(data)
    }
        client = MySimBdp_CommonTool()

        result = client.initiate_stream_ingest(configuration, table_n_data,)
        print(result)
        return result

        #print ("Table and data are" + table_name ,data)

   