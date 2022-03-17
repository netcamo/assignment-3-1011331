from flask import Flask
from flask import request, jsonify
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import sys
import json


app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024

@app.route('/check', methods=['GET'])
def check():
 
  return jsonify({'msg': 'success'}), 200


@app.route('/<tenant_id>/create_table', methods=['POST'])
def create_table(tenant_id):
  
  if request.method != 'POST':
    return jsonify({'msg': 'Incorrect request method'}), 400

 
  table = request.json.get('table', None)
  if not table:
    return jsonify({'msg': 'Missing parameter'}), 400

  
  columns = ','.join(['{} {}'.format(item['field'], item['type']) for item in table['schema']])+ ', PRIMARY KEY ({})'.format(','.join(item for item in table['primary_key']))
  print("yeeey")
  
  cluster = Cluster(['0.0.0.0'],port=9042)
  session = cluster.connect()
  with open('filename.txt', 'w') as f:
    print('This message will be written to a file.', file=f)
  print("Have connected")
  print("Tenant_id is "+tenant_id)      
  try:
       
        session.execute("CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{ \'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }}".format(tenant_id) )

        print("Keyspace {} has been created".format(tenant_id))

        create_stmt = session.prepare("CREATE TABLE IF NOT EXISTS {}.{} ({}) ;".format(tenant_id, table['table_name'], columns))
        session.execute(create_stmt)
  except Exception as e:
                print(e)



  return jsonify({'msg': 'success'}), 200


@app.route('/<tenant_id>/batch_ingest', methods=['POST'])
def batch_ingest(tenant_id):
  
  if request.method != 'POST':
    return jsonify({'msg': 'Incorrect request method'}), 400
  if not request.is_json:
    return jsonify({"msg": "Missing JSON in request"}), 400

  
  table_name = request.json.get('table_name', None)
  rows = request.json.get('rows', None)

  
  cluster = Cluster(['0.0.0.0'],port=9042)
  session = cluster.connect()

  batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

  count = 0

  for row in rows:
    keys = row.keys()
    fields = ",".join([item for item in keys])
    insert_stmt ="INSERT INTO {}.{} ({}) VALUES ({})".format(tenant_id, table_name, fields, ",".join(["%s" for item in keys]))
    batch.add(insert_stmt, [row[item] for item in keys])
    count+=1
  session.execute(batch)
  batch.clear()
  return jsonify({'msg': 'success', "rows": count}), 200


@app.route('/<tenant_id>/stream_ingest', methods=['POST'])
def stream_ingest(tenant_id):
  print("Stream Ingest Called")  

  if request.method != 'POST':
    return jsonify({'msg': 'Incorrect request method'}), 400
  if not request.is_json:
    return jsonify({"msg": "Missing JSON in request"}), 400

  
  table_name = request.json.get('table_name', None)
  data = request.json.get('data', None)

  
  cluster = Cluster(['0.0.0.0'],port=9042)
  session = cluster.connect()
  batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

  keys = data.keys()
  fields = ",".join([item for item in keys])
  insert_stmt ="INSERT INTO {}.{} ({}) VALUES ({})".format(tenant_id, table_name, fields, ",".join(["%s" for item in keys]))
  session.execute(insert_stmt, [data[item] for item in keys])
  return  jsonify({'msg': 'success'}), 200




if __name__ == '__main__':
  app.run(port=5000)
