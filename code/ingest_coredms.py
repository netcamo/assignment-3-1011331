import datetime
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
KEYSPACE = "mysimbdp"

import os
import csv
import sys
import coredms
from cassandra.query import BatchStatement

from time import time
import argparse
def ingestListings(datafile, session, ):
    listings={}
    insertions = 0
    with open(datafile) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        next(csv_file)

        count = 0
        for row in reader:
            
            try:
            
                listings["id"]=int(row[0])
                listings["host_id"]=int(row[1])
                listings["host_name"]=row[2]
                listings["neighbourhood"]=row[3]
                listings["latitude"]=float(row[4])
                listings["longitude"]=float(row[5])
                listings["room_type"]=row[6]
                listings["price"]=int(row[7])
                listings["availability_365"]=int(row[8])
                    
            except Exception as e:
                print('The cassandra error: {}'.format(e))

            count += 1
           
           
            
            try:
                coredms.addListings(listings,session)
                insertions += count
                count = 0
            except Exception as e:
                print('The cassandra error: {}'.format(e))

           
    return insertions


if __name__ == "__main__":
    cluster = Cluster(['0.0.0.0'],port=9042)
    session = cluster.connect()

    table = sys.argv[1]
    datafile = sys.argv[2]

    start=time()
    if table == "listings":
	    insertions = ingestListings(datafile, session)

    stop=time()

    print("Inserted", insertions, "in", stop-start, "s", sep=" ")
