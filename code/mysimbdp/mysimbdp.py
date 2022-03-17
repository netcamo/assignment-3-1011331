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
def log_maker(add_text):
    with open("../../logs/stream_logs.log", "a") as external_file:
        external_file.write(add_text+"\n")
        external_file.close()
                   


class MySimBdp_CommonTool():
    
    def initiate_batch_ingest(self, tenant_config, source_endpoint):
        
        table = None
        for temp_table in tenant_config['tables']:
            if temp_table['table_name'] == source_endpoint['table_name']:
                table= temp_table

        
        logger_tenant = setup_logger(str(tenant_config['tenant_id'])+'Logger:', os.getcwd() + '/../../logs/'+str(tenant_config['tenant_id'])+'logs.log')
        logger_tenant.info(tenant_config['tenant_id']+ ' Common tool initiated')
        start_time = time()
        #logger.info("Creating the table for  tenant_id={}, file_name={}, table_name={}".\
        #format( tenant_config['tenant_id'], source_endpoint['file_name'], source_endpoint['table_name']))
        logger_tenant.info("Creating the table for  tenant_id={}, file_name={}, table_name={}".format( tenant_config['tenant_id'], source_endpoint['file_name'], source_endpoint['table_name']))
        
        
        res = requests.post("http://localhost:5000/{}/create_table".format(tenant_config['tenant_id']), json={"table": table})
        if res.status_code != 200:
            logger_tenant.info("Error while creating the Table for  tenant_id={}, file_name={}, table_name={}".format( tenant_config['tenant_id'], source_endpoint['file_name'], source_endpoint['table_name']))
        
            return "Error while creating the Table"
   
        
        FILE_PATH = os.getcwd() + '/../../data/client-staging-input-directory/{}/{}/{}'.format(tenant_config['tenant_id'], source_endpoint['table_name'], source_endpoint['file_name'])
        file_size = os.stat(FILE_PATH).st_size
        
        count = 0  
        rows = []  

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

    def initiate_stream_ingest(self, tenant_config, table_n_data):

        
        table = None
        for table_tmp in tenant_config['tables']:
            if table_tmp['table_name'] == table_n_data['table_name']:
                table = table_tmp

        if not table:
            return {
                'status': 'error',
                'msg': 'Table is missing'
            }

        #logger_tenant = setup_logger(str(tenant_config['tenant_id'])+'Stream Logger:', os.getcwd() + '/../../logs/'+str(tenant_config['tenant_id'])+'_stream_logs.log')
        #logger_tenant.info(tenant_config['tenant_id']+ ' Common tool initiated')
        log_maker(tenant_config['tenant_id']+ ' Common tool initiated')
        
        log_maker(("Creating the table for  tenant_id={},  table_name={}".format( tenant_config['tenant_id'], table_n_data['table_name'])))
        #logger_tenant.info("Creating the table for  tenant_id={},  table_name={}".format( tenant_config['tenant_id'], table_n_data['table_name']))
        

        res = requests.post("http://localhost:5000/{}/create_table".format(tenant_config['tenant_id']), json={"table":table})

        if res.status_code != 200:
            #logger_tenant.info("Error while creating the Table for  tenant_id={}, table_name={}".format( tenant_config['tenant_id'], table_n_data['table_name']))
            log_maker("Error while creating the Table for  tenant_id={}, table_name={}".format( tenant_config['tenant_id'], table_n_data['table_name']))

            return "Error while creating the Table"
   

        #logger_tenant.info("Starting the stream ingest")
        log_maker("Starting the stream ingest")
        
        start_time = time()
        
        res = requests.post("http://localhost:5000/{}/stream_ingest".format(tenant_config['tenant_id']), json={'table_name': table_n_data['table_name'], 'data': table_n_data['data']})
        end_time = time()

        if res.status_code == 200:

            # logger_tenant.info("Finished Stream Ingest \n status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
            #     'success',
            #     tenant_config['tenant_id'],
            #     table_n_data['table_name'],
            #     start_time,
            #     end_time,
            #     (end_time-start_time)
            #     )
            # )
            log_maker("Finished Stream Ingest \n status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
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
            # logger_tenant.info("Finished Stream Ingest \n status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
            #     'failed',
            #     tenant_config['tenant_id'],
            #     table_n_data['table_name'],
            #     start_time,
            #     end_time,
            #     (end_time-start_time)
            #     )
            # )

            log_maker("Finished Stream Ingest \n status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
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



