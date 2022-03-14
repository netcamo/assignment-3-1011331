import os
import sys
import csv
import requests
import logging
import pandas as pd
from datetime import datetime
from time import time
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

logging.basicConfig(filename=os.getcwd() + '/../../logs/logs.log',  level=logging.INFO)
logger = logging.getLogger("MYSIMBDP Logger")
logger.info("Started MYSIMBDP Logger")


class MySimBdpClient():
    
    def start_batch_ingest_job(self, tenant_config, ingest_task):
        
        if 'tenant_id' not in tenant_config or not tenant_config['tenant_id']:
            return {
                'status': 'error',
                'msg': 'Parameter tenant_id is missing'
            }

        required_paras = ['file_name', 'table_name']
        for para in required_paras:
            if para not in ingest_task or not ingest_task[para]:
                return {
                    'status': 'error',
                    'msg': 'Parameter {} is missing'.format(para)
                }

        
        table_metadata = None
        for table in tenant_config['tables']:
            if table['table_name'] == ingest_task['table_name']:
                table_metadata = table

        if not table_metadata:
            return {
                'status': 'error',
                'msg': 'Table metadata is missing'
            }

        start_time = time()
        logger.info("Creating the table for  tenant_id={}, file_name={}, table_name={}".format( tenant_config['tenant_id'], ingest_task['file_name'], ingest_task['table_name']))
        
        
        res = requests.post("http://localhost:5000/{}/create_table".format(tenant_config['tenant_id']), json={"table": table_metadata})
        if res.status_code != 200:
            logger.info("Error while creating the Table")
        
            return {
                'status': 'error',
                'msg': 'Error occur when creating table'
            }
   
        
        FILE_PATH = os.getcwd() + '/../../data/client-staging-input-directory/{}/{}/{}'.format(tenant_config['tenant_id'], ingest_task['table_name'], ingest_task['file_name'])
        file_size = os.stat(FILE_PATH).st_size
        count = 0  
        rows = []  

        logger.info("Starting batch Ingest")
        
        print("Opening the file")
        reader = pd.read_csv(FILE_PATH, chunksize=200)

        for chunk_df in reader:
            for row in chunk_df.itertuples():
                rows.append({
                    item['field']: getattr(row, item['field']) if not pd.isna(getattr(row, item['field'])) else None for item in table_metadata['schema']
                })
            
            res = requests.post("http://localhost:5000/{}/batch_ingest".format(tenant_config['tenant_id']), json={'table_name': ingest_task['table_name'], 'rows': rows})
            if res.status_code == 200:
                count += res.json()['rows']
            rows = [] 
       
        end_time = time()
        
        logger.info("Finished the ingestion \n status={}, tenant_id={}, file_name={}, file_size_bytes={}, table_name={}, ingestion_rows={}, total_time_cost={} seconds".format(
            'success',
            tenant_config['tenant_id'], 
            ingest_task['file_name'],
            file_size,
            ingest_task['table_name'],
            count,
            (end_time-start_time)
            )
        )

      
        return {
            'status': 'success',
            'tenant_id': tenant_config['tenant_id'],
            'file_name': ingest_task['file_name'],
            'file_size_bytes': file_size,
            'table_name': ingest_task['table_name'],
            'ingestion_rows': count,
            'total_time_cost_seconds': (end_time-start_time)
        }


