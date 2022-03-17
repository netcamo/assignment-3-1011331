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

    def start_stream_ingest_job(self, tenant_config, table_n_data):

        # examine tenant_id
        if 'tenant_id' not in tenant_config or not tenant_config['tenant_id']:
            return {
                'status': 'error',
                'msg': 'Parameter tenant_id is missing'
            }

        # examine ingest task
        required_paras = ['data', 'table_name']
        for para in required_paras:
            if para not in table_n_data or not table_n_data[para]:
                return {
                    'status': 'error',
                    'msg': 'Parameter {} is missing'.format(para)
                }

        # examine if table metadata exists
        table_metadata = None
        for table in tenant_config['tables']:
            if table['table_name'] == table_n_data['table_name']:
                table_metadata = table

        if not table_metadata:
            return {
                'status': 'error',
                'msg': 'Table metadata is missing'
            }

        logger_tenant = setup_logger(str(tenant_config['tenant_id'])+'Stream Logger:', os.getcwd() + '/../../logs/'+str(tenant_config['tenant_id'])+'_stream_logs.log')
        logger_tenant.info('Tenant Common tool initiated')
        
        logger.info("Creating the table for  tenant_id={},  table_name={}".format( tenant_config['tenant_id'], table_n_data['table_name']))
        logger_tenant.info("Creating the table for  tenant_id={},  table_name={}".format( tenant_config['tenant_id'], table_n_data['table_name']))
        
        
        print("requesting creation of table " + tenant_config['tenant_id']+" "+table_n_data["table_name"])
        print("http://localhost:5000/{}/create_table".format(tenant_config['tenant_id']))
        res = requests.post("http://localhost:5000/{}/create_table".format(tenant_config['tenant_id']), json={"table":table_metadata})

        if res.status_code != 200:
            logger.info("Error while creating the Table")
            logger_tenant.info("Creating the table for  tenant_id={}, table_name={}".format( tenant_config['tenant_id'], table_n_data['table_name']))

            return "Error while creating the Table"
   


        start_time = time()
        
        res = requests.post("http://localhost:5000/{}/stream_ingest".format(tenant_config['tenant_id']), json={'table_name': table_n_data['table_name'], 'data': table_n_data['data']})
        end_time = time()

        # log and return ingestion result
        if res.status_code == 200:

            logger_tenant.info("[Stream ingestion] status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
                'success',
                tenant_config['tenant_id'],
                table_n_data['table_name'],
                start_time,
                end_time,
                (end_time-start_time)
                )
            )
            logger.info("[Stream ingestion] status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
                'success',
                tenant_config['tenant_id'],
                table_n_data['table_name'],
                start_time,
                end_time,
                (end_time-start_time)
                )
            )
            return {
                'status': 'success',
                'tenant_id': tenant_config['tenant_id'],
                'table_name': table_n_data['table_name'],
                'start_time': start_time,
                'end_time': end_time,
                'total_time_cost_seconds': (end_time-start_time)
            }
        else:
            logger_tenant.info("[Stream ingestion] status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
                'failed',
                tenant_config['tenant_id'],
                table_n_data['table_name'],
                start_time,
                end_time,
                (end_time-start_time)
                )
            )
            logger.info("[Stream ingestion] status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
                'failed',
                tenant_config['tenant_id'],
                table_n_data['table_name'],
                start_time,
                end_time,
                (end_time-start_time)
                )
            )
            return {
                'status': 'failed',
                'tenant_id': tenant_config['tenant_id'],
                'table_name': table_n_data['table_name'],
                'start_time': start_time,
                'end_time': end_time,
                'total_time_cost_seconds': (end_time-start_time)
            }



