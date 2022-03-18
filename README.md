# Assignment 2  1011331

ode directory contains all the codes. In the root of code folder there is *data_producer.py* which is used to produce data into messaging queue from file.

inside client_ingest_apps folder there is a folder for each tenant. inside tenants folder there are clientingestapps for batch and stream ingest. 

inside mysimbdp folder there are following:

* docker-compose.yml -> docker compose file for creating cassandra containers.
* mysimbdp_daas.py -> Flask API DAAS
* mysimbdp.py -> there is Mysimbdp_CommonTool  class that is provided to tenants to use in their client_ingest_apps.
* batchingestmanager -> manager for batch ingest
* streamingestmanager -> manager for stream ingest
*  constraint_ingestion.JSON -> implementation of constraints 
  
 
 Logs folder contains logs and statistics .

 Inside Data folder there is a folder for each tenant and each tenant folder contains a folder for each table . inside table folders there will be files. At the root of data folder there are 3 example data files. data and data_2 are same data , data_small is smaller version of this data. 


Reports Folder contains all report and also instruction on Deployment.





