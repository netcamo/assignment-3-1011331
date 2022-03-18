 docker run -d --hostname mysimbdp-rabbitmq --name bdp_rabbitmq -e RABBITMQ_DEFAULT_USER="rbmq" -e RABBITMQ_DEFAULT_PASS="rbmq" -p 5672:5672 -p 15672:15672  -p 55672:55672 rabbitmq:3-management



# This is a deployment/installation guide




### CODE STRUCTURE 

Code directory contains all the codes. In the root of code folder there is *data_producer.py* which is used to produce data into messaging queue from file.

inside client_ingest_apps folder there is a folder for each tenant. inside tenants folder there are clientingestapps for batch and stream ingest. 

inside mysimbdp folder there are following:

* docker-compose.yml -> docker compose file for creating cassandra containers.
* mysimbdp_daas.py -> Flask API DAAS
* mysimbdp.py -> there is Mysimbdp_CommonTool  class that is provided to tenants to use in their client_ingest_apps.
* batchingestmanager -> manager for batch ingest
* streamingestmanager -> manager for stream ingest
*  constraint_ingestion.JSON -> implementation of constraints 
  
 inside data folder there is a folder for each tenant and each tenant folder contains a folder for each table . inside table folders there will be files. At the root of data folder there are 3 example data files. data and data_2 are same data , data_small is smaller version of this data. 

 Logs folder contains logs and statistics .


 ## 1st Part

First one needs to go to the ***code/mysimbdp*** directory.

Then we should launch the cassandra containers: 

        docker-compose up

After waiting for approximately 2 minutes (to docker containers getting up) we  launch the flask api in another terminal 

        python3  mysimbdp_daas.py

now first we have to delete all the data files from inside the folders *data/client-staging-input-directory/<tenant_id>/<table_name>/* . Be careful! only remove csv files and not folders.

Now we have to run the batchingestmanager . Open a new terminal and go to code/mysimbdp directory again :

        python3 batchingestmanager.py

Now take any data file you want and put to any of tenant/table folder.  Everything will work as it is supposed to work and you can find logs inside logs folder logs.log  and <tenant_id>_logs.log files.


If it has failed wait for a minute and try again since failure is because of the docker containers not being ready.




## PART 2

*Optional* Remove all cassandra docker containers and start thnem again.

go to code/mysimbdp folder  and then execute following commmand to create rabbitmq container:

         docker run -d --hostname mysimbdp-rabbitmq --name bdp_rabbitmq -e RABBITMQ_DEFAULT_USER="rbmq" -e RABBITMQ_DEFAULT_PASS="rbmq" -p 5672:5672 -p 15672:15672  -p 55672:55672 rabbitmq:3-management

launch the flask api in another terminal 

        python3  mysimbdp_daas.py

Now we have to run the streamingestmanager . Open a new terminal and go to code/mysimbdp directory again :

        python3 streamingestmanager.py

Open another terminal and go to code folder to run data producer :

        python3 data_producer.py

You will find logs and statistics inside logs folder in stream_logs.log and streaming_monitor.csv files.  


        
