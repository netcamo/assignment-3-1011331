import os
from time import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler
import json
import logging
import pika, sys, os,  requests
import threading
import json
import importlib


CRENDENTIALS = pika.PlainCredentials('mysimbdp', 'mysimbdp')
CURRENT_DIRECTORY = os.getcwd()




class ConsumerThread(threading.Thread):

    def __init__(self,  tenant, *args, **kwargs):
        super(ConsumerThread, self).__init__(*args, **kwargs)

        self._host = '0.0.0.0'
        self._tenant = tenant
        self._count=0
        self._start=time()
        self._total_size=0
        self._predef_time=5
        self._total_ingestion_time=0
        

    def run(self):
        headers = {
            'content-type': 'application/json',
        }
        
        response = requests.put('http://localhost:15672/api/vhosts/{}'.format(self._tenant['tenant_id']), headers=headers, auth=('mysimbdp', 'mysimbdp'))

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host, virtual_host=self._tenant['tenant_id'], credentials=CRENDENTIALS))
        channel = connection.channel()

        
        channel.exchange_declare(exchange='default', exchange_type='direct')

        def setup_logger(name, log_file, level=logging.INFO):
            handler = logging.FileHandler(log_file)        
            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.addHandler(handler)
            return logger
        
        
        for table in self._tenant['tables']:
           
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
           
            channel.queue_bind(exchange='default', queue=queue_name, routing_key=table)
            
            

            def callback(ch, method, properties, body):
                print(" [mysimbdp-streamingestmanager] <Tenant={} Table={}> Received msg".format(self._tenant['tenant_id'], table, body.decode()))
                current_directory = os.getcwd()
                if(self._count==0):
                    self._start=time()
        
                self._count+=1
                
                sys.path.insert(0, CURRENT_DIRECTORY+'/../client_ingest_apps/{}'.format(self._tenant['tenant_id']))
                from clientstreamingestapp  import ClienStreamIngestApp
                
                cliStreamApp=ClienStreamIngestApp()
                result=cliStreamApp.stream_ingest(table,body.decode())
                print("\n{}\n".format(self._count))
                size=sys.getsizeof (body)
                self._total_size+=size
                self._total_ingestion_time+=result['total_time_cost_seconds']
                if(self._count % self._predef_time ==0):
                    with open("../../logs/streaming_monitor.csv", "a") as external_file:
                        add_text = "{},{},{},{}\n".format( str( self._count/ (time()-self._start) ) ,str(self._total_ingestion_time/self._count)  ,str(self._total_size),str (self._count) ) 
                        external_file.write(add_text)
                        external_file.close()
                    #lg = setup_logger(str(self._tenant['tenant_id'])+'MonitorLog', os.getcwd() + '/../../logs/'+str(self._tenant['tenant_id'])+'_monitor_stream_logs.log')
                    #lg.info("{},{},{},{}".format ( str( self._count/ (time()-self._start) ) ,str(self._total_ingestion_time/self._count)  ,str(self._total_size),str (self._count) ) )
        
                
              
            channel.basic_consume(queue=queue_name, on_message_callback=callback,auto_ack=True)

        print('[mysimbdp-streamingestmanager] Start consuming vhost: {}'.format(self._tenant['tenant_id']))
        channel.start_consuming()


def main():

    tenants = [
        {
            'tenant_id': 'tenant_1',
            'tables': ['listings']
         }#,
        #  {
        #      'tenant_id': 'tenant_2',
        #     'tables': ['another_listing']
        #  }
    ]
    threads = [ConsumerThread( tenant=tenant) for tenant in tenants]
    for thread in threads:
        thread.start()


if __name__ == '__main__':
    main()
