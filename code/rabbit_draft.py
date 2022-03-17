import pandas as pd
import pika
import json
import os
import time
import requests


tenants = {
    'tenant_1': {
        'file_path': os.getcwd() + '/../data/client-staging-input-directory/tenant_1/listings/data_small.csv',
        'table_metadata': {
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
    }
    
}

# Creating virtual host
headers = {
    'content-type': 'application/json',
}

response = requests.put('http://localhost:15672/api/vhosts/tenant_1', headers=headers, auth=('mysimbdp', 'mysimbdp'))

# connect to rabbitmq
CRENDENTIALS = pika.PlainCredentials('mysimbdp', 'mysimbdp')
connection = pika.BlockingConnection(pika.ConnectionParameters(host="0.0.0.0", virtual_host='tenant_1', credentials=CRENDENTIALS))
channel = connection.channel()


# declare exchange
channel.exchange_declare(exchange='default', exchange_type='direct')

# read csv and publish to RabbitMQ
table=tenants['tenant_1']['table_metadata']
            # declare queue
result = channel.queue_declare(queue='', exclusive=True)

#QUEUE CREATION AND BINDING
queue_name = result.method.queue
        # bind queue to exchange
channel.queue_bind(exchange='default', queue=queue_name, routing_key=table['table_name'])

reader = pd.read_csv(tenants['tenant_1']['file_path'], chunksize=30)

#for consuming
def callback(ch, method, properties, body):
                print(" [mysimbdp-streamingestmanager] <Tenant={} Table={}> Received msg {}".format("tenant_1", table['table_name'], body.decode()))
                print("che cazz")
                #print(ch['virtual_host'])
                #print(ch.virtual)
                print(method.routing_key)
                
                ch.basic_ack(delivery_tag = method.delivery_tag)
            # consume the queue

channel.basic_consume(queue=queue_name, on_message_callback=callback,auto_ack=True)
      
for chunk_df in reader:
    for row in chunk_df.itertuples():
        data = {
            item['field']: getattr(row, item['field']) if not pd.isna(getattr(row, item['field'])) else None for item in tenants['tenant_1']['table_metadata']['schema']
        }
        # produce to mq
        print("start")
        channel.basic_publish(exchange='default', routing_key=tenants['tenant_1']['table_metadata']['table_name'], body=json.dumps(data))
        print("middle")
        
        print("stop")

        #consuming start
        channel.start_consuming()
        time.sleep(2)
        print("continue")
        


