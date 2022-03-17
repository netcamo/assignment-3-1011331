 docker run -d --hostname mysimbdp-rabbitmq --name bdp_rabbitmq -e RABBITMQ_DEFAULT_USER="mysimbdp" -e RABBITMQ_DEFAULT_PASS="mysimbdp" -p 5672:5672 -p 15672:15672  -p 55672:55672 rabbitmq:3-management



# This is a deployment/installation guide

First one needs to go to the ***code*** directory.

Then we should launch the cassandra containers: 

        docker-compose up

After waiting for approximately 2 minutes (to docker containers getting up) we should create cassandra keyspace. For this, while in code directory:

        python3  coredms.py setup_cassandra


If it has failed wait for a minute and try again since failure is because of the docker containers not being ready.


next step would be to try  dataingest.py function.  dataingest function takes as argument data source file and  write consistency level (ONE, QUORUM, ALL).

For example :

        python3 dataingest.py  ../data/data.csv ONE


***One note**: if you want to check GIGA data then you need to increase heap size in docker-compose file. My machine wouldn't support that huge heap so I haven't allocated it in my docker compose file.*

To use many ingest processes concurrently use  *&* :

    python3 dataingest.py  ../data/data.csv ONE & python3 dataingest.py  ../data/data.csv ONE &

Performance times will be shown automatically after process finish . 
