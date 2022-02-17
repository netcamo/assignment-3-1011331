
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import sys
from time import time
KEYSPACE="mysimbdp"

if __name__ == "__main__":
   
    if (sys.argv[1])=="setup_cassandra":
   
        for i in range(0,3):
            
            cluster = Cluster(['0.0.0.0'],port=9042+i)
            
            try:
                session = cluster.connect()
                session.execute("""
                            CREATE KEYSPACE IF NOT EXISTS mysimbdp
                            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
                            """ )
                session.set_keyspace("mysimbdp")

                session.execute(""" DROP TABLE IF EXISTS listings """)

                session.execute("""
                            CREATE TABLE IF NOT EXISTS listings (
                                id int,
                                host_id int,
                                host_name text,
                                neighbourhood text,
                                latitude float,
                                longitude float, 
                                room_type text,
                                price int,
                                availability_365 int,
                                PRIMARY KEY (host_id, id)
                            )
                            """)
                print("Keyspace have been created!")
            except Exception as e:
                print(e)
    

    elif (sys.argv[1]=="get_listings"):
        

        cluster = Cluster(['0.0.0.0'],port=9042)
        session = cluster.connect("mysimbdp", wait_for_all_pools=True)
        session.execute('USE %s' % KEYSPACE)
        start=time()
        rows = session.execute('SELECT * FROM listings')
        end=time()
        
       
        #print(list(rows))
        print("Got Listings",len(list(rows))  ,"in", end-start, "s", sep=" ")
            