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


def setup_logger(name, log_file, level=logging.INFO):
    

    handler = logging.FileHandler(log_file)        
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


class MySimBdp_CommonTool():
    
    def initiate_batch_ingest(self, tenant_config, source_endpoint):
        
        table = None
        for temp_table in tenant_config['tables']:
            if temp_table['table_name'] == source_endpoint['table_name']:
                table= temp_table

        
        logger_tenant = setup_logger(str(tenant_config['tenant_id'])+'Logger:', os.getcwd() + '/../../logs/'+str(tenant_config['tenant_id'])+'logs.log')
        logger_tenant.info('Tenant Common tool initiated')
        start_time = time()
        logger.info("Creating the table for  tenant_id={}, file_name={}, table_name={}".format( tenant_config['tenant_id'], source_endpoint['file_name'], source_endpoint['table_name']))
        logger_tenant.info("Creating the table for  tenant_id={}, file_name={}, table_name={}".format( tenant_config['tenant_id'], source_endpoint['file_name'], source_endpoint['table_name']))
        
        
        res = requests.post("http://localhost:5000/{}/create_table".format(tenant_config['tenant_id']), json={"table": table})
        if res.status_code != 200:
            logger.info("Error while creating the Table")
            logger_tenant.info("Creating the table for  tenant_id={}, file_name={}, table_name={}".format( tenant_config['tenant_id'], source_endpoint['file_name'], source_endpoint['table_name']))
        
            return "Error while creating the Table"
   
        
        FILE_PATH = os.getcwd() + '/../../data/client-staging-input-directory/{}/{}/{}'.format(tenant_config['tenant_id'], source_endpoint['table_name'], source_endpoint['file_name'])
        file_size = os.stat(FILE_PATH).st_size
        
        count = 0  
        rows = []  

        logger.info("Starting batch Ingest")
        logger_tenant.info("Starting batch Ingest")
        
        print("Opening the file")
        reader = pd.read_csv(FILE_PATH, chunksize=200)

        for chunk_df in reader:
            for row in chunk_df.itertuples():
                rows.append({
                    item['field']: getattr(row, item['field']) if not pd.isna(getattr(row, item['field'])) else None for item in table['schema']
                })
            
            res = requests.post("http://localhost:5000/{}/batch_ingest".format(tenant_config['tenant_id']), json={'table_name': source_endpoint['table_name'], 'rows': rows})
            if res.status_code == 200:
                count += res.json()['rows']
            rows = [] 
       
        end_time = time()
        
        logger.info("Finished the ingestion \n status={}, tenant_id={}, file_name={}, file_size_bytes={}, table_name={}, ingestion_rows={}, total_time_cost={} seconds".format
        (
            'success',
            tenant_config['tenant_id'], 
            source_endpoint['file_name'],
            file_size,
            source_endpoint['table_name'],
            count,
            (end_time-start_time)
            
        )
                )
                
        logger_tenant.info("Finished the ingestion \n status={}, tenant_id={}, file_name={}, file_size_bytes={}, table_name={}, ingestion_rows={}, total_time_cost={} seconds".format
        (
            'success',
            tenant_config['tenant_id'], 
            source_endpoint['file_name'],
            file_size,
            source_endpoint['table_name'],
            count,
            (end_time-start_time)
            
        )
                )        

      
        return {
            'tenant_id': tenant_config['tenant_id'],
            'file_name': source_endpoint['file_name'],
            'file_size_bytes': file_size,
            'table_name': source_endpoint['table_name'],
            'ingestion_rows': count,
            'total_time_cost_seconds': (end_time-start_time)
        }


