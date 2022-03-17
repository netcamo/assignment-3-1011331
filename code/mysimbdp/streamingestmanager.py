import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler
import json
import logging
import pika, sys, os, time, requests
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

    def run(self):
       
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host, virtual_host=self._tenant['tenant_id'], credentials=CRENDENTIALS))
        channel = connection.channel()

        # declare exchange
        channel.exchange_declare(exchange='default', exchange_type='direct')

        # declare queues and bind it to exchange (one table has one queue)
        for table in self._tenant['tables']:
            # declare queue
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            # bind queue to exchange
            channel.queue_bind(exchange='default', queue=queue_name, routing_key=table)
            # define what to do when accept data from rabbitmq
            print("Before call back " +table)

            def callback(ch, method, properties, body):
                print(" [mysimbdp-streamingestmanager] <Tenant={} Table={}> Received msg".format(self._tenant['tenant_id'], table, body.decode()))
                current_directory = os.getcwd()
                # invoke customer's clientstreamingestapp
                self._count+=1
                print(self._count)

                #os.system("python3 {current}/../client_ingest_apps/{tenant_id}/clientstreamingestapp.py {table_name} {file_name}".format(current=CURRENT_DIRECTORY, tenant_id=self._tenant['tenant_id'], table_name=table, file_name=body.decode()))
                #print("Trying to upload module")
                #print (CURRENT_DIRECTORY)
                sys.path.insert(0, CURRENT_DIRECTORY+'/../client_ingest_apps/{}'.format(self._tenant['tenant_id']))
                from clientstreamingestapp  import ClienStreamIngestApp
                #clientstreamingestapp = importlib.import_module("...client_ingest_apps.{}.clientstreamingestapp".format(self._tenant['tenant_id']),package='ClientStreamIngestApp')
                #clientstreamingestapp.stream_ingest(table,body.decode())
                cliStreamApp=ClienStreamIngestApp()
                cliStreamApp.stream_ingest(table,body.decode())
                
                 # ack the message
                #ch.basic_ack(delivery_tag = method.delivery_tag)
            # consume the queue
            channel.basic_consume(queue=queue_name, on_message_callback=callback,auto_ack=True)

        print('[mysimbdp-streamingestmanager] Start consuming vhost: {}'.format(self._tenant['tenant_id']))
        channel.start_consuming()


def main():

    tenants = [
        {
            'tenant_id': 'tenant_1',
            'tables': ['listings']
         }#,
        # {
        #     'tenant_id': 'tenant_2',
        #     'tables': ['another_listing']
        # }
    ]
    threads = [ConsumerThread( tenant=tenant) for tenant in tenants]
    for thread in threads:
        thread.start()


if __name__ == '__main__':
    main()
