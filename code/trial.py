import datetime
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
KEYSPACE = "mysimbdp"


if __name__ == "__main__":
    cluster = Cluster(['0.0.0.0'],port=9042)
    session = cluster.connect("mysimbdp", wait_for_all_pools=True)
    query = 'SELECT * FROM listings LIMIT 100;'
    rows = session.execute(query)
    print(list(rows))