
### *ALL THE INSTRUCTIONS ON HOW TO TEST THE IMPLEMENTATIONS ARE EXPLAINED DETAILED IN DEPLOYMENT.MD*


## Part 1 - Batch data ingestion

1. *The ingestion will be applied to files of data. Design a set of constraints for files that mysimbdp will support for ingestion.
Design a set of constraints for the tenant service profile w.r.t. ingestion (e.g., maximum number of files and amount of data).
Explain why you as a platform provider decide such constraints. Implement these constraints into simple configuration files
and provide examples (e.g., JSON or YAML).* 
    
    Since we will have many tenants, I have designed constraints to be individual for every tenant so that we can limit accesses and control everything easily. I have decided to use 1 constraint_ingestion.JSON file which will contain all the tenant's constraints according to their unique tenant_id. There are many constraints that we can put but for ease of implementation I decided to use following constraints set:


    ```javascript
    [
        {
            "valid_file_format": "csv",
            "max_batch_size_rows" : 200,
            "max_file_size_MB": 20,
            "tenant_id": "tenant_1",
            "max_file_number": 4,
            "consistency" : 1
        },
        {
            
            "valid_file_format": "csv",
            "max_batch_size_rows": 200,
            "max_file_size_MB": 20,
            "tenant_id": "tenant_2",
            "max_file_number": 4,
            "consistency" : 1
        }
    ]
    ```  
    * valid_file_format -> The file formats that can be used as a valid input
    * max_batch__size_rows -> we can limit batch sizes  according to our platform's possibilities and also tenant's access level
    * max_file_size_MB -> We can limit maximum file size that can be used as an input 
    * tenant_id -> this field is to identify tenants according to their unique id's. 
    * max_file_number -> We can limit number of files that tenant can have
    * consistency -> We can configure consistency level for individual tenants (1: Quroum , 2: ALL)
  
   
2. *Each tenant will put the tenant's data files to be ingested into a directory, client-staging-input-directory within mysimbdp.
Each tenant provides ingestion programs/pipelines, clientbatchingestapp, which will take the tenant's files as input, in
client-staging-input-directory, and ingest the files into mysimbdp-coredms. Any clientbatchingestapp must perform at
least one type of data wrangling. As a tenant, explain the design of clientbatchingestapp and provide one implementation.
Note that clientbatchingestapp follows the guideline of mysimbdp given in the next Point 3.* 

    We will havbe many tenants and they will have many tables so in order to ease managing table schemas for the platform then tenant itself provides its configuration inside their clientbatchingestapp. for example: 


    ```javascript
    {
    "tenant_id": "tenant_1",
    "tables": [
        {
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
        },
        {
            "table_name": "Listing_table_2",
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
    ]
    }          
    ```  

    I have designed a common tool that will be provided to each tenant and they can import it in their clientingestapp.  This CommonTool accesses to tenant configuration and also gets the source_endpoint (source file to be ingested and table in the DB that data should be ingested) . It performs constraint compliances and handles everything to DAAS API to ingest the data. Thus clientingestapp provides tenant's configuration and source_endpoint to initiate the ingestion process.

    ![Batch Ingest Diagram](batchingestdiag.png "Batch Ingest Diagram")

   


3. *As the mysimbdp provider, design and implement a component mysimbdp-batchingestmanager that invokes tenant's
clientbatchingestapp to perform the ingestion for available files in client-staging-input-directory. mysimbdp imposes the
model that clientbatchingestapp has to follow but clientbatchingestapp is, in principle, a blackbox to mysimbdpbatchingestmanager. Explain how mysimbdp-batchingestmanager decides/schedules the execution of
clientbatchingestapp for tenants.* 

    I have decided to use file/directory kind of system for my tenant's input datas. Every tenant has a unique id so inside client-staging-input-directory there is a folder for each tenant whose name is tenant's id. Since tenants can have multiple tables, the folders inside tenant folder shows the tables. Each folder's name is table's name. Tenant puts it's input file inside this table folders to start the ingestion. THis way whenever the file is moved to the table folder we perform ingestion to that tenant's specific table.

    I have implemented it using Watchdog python module. Batchingestmanager monitors the client-staging-input-directory and whenever it detects a new file first it checks the constraints compliance (since we have constraints JSON file) and if everything is good then it proceeds to execute appropriate tenant's clientingestapp with arguments of the table name and file source. it gets these arguments from folder's names that new file has been moved to. This way the implementation and overall execution becomes very simple.




4. *Explain your design for the multi-tenancy model in mysimbdp: which parts of mysimbdp will be shared for all tenants,
which parts will be dedicated for individual tenants so that you as a platform provider can add and remove tenants based on
the principle of pay-per-use. Develop test programs (clientbatchingestapp), test data, and test constraints of files, and test
service profiles for tenants according your deployment. Show the performance of ingestion tests, including failures and
exceptions, for at least 2 different tenants in your test environment and constraints. What is the maximum amount of data
per second you can ingest in your tests?*
 
    From tools  I have chosen to share the CommonTool which takes care of connection with DAAS and handles the configuration from clientingestapps. My databse will be shared between tenants. Since I am using Cassandra DB I can use Keyspaces for isolatig the different tenant's data and tables this allows perfect way to handle them differently but at the same time using the same database. I have chosen to create Keyspace with name of tenant_id  to be able to differentiate them easily easily. It also helps with configuration part since in input directory the root folder will be <tenant_id> folder so it will preserve the file/directory tree system that I was using for input files and tables.


    ![DB design](design.png "DB design")


* Test Implementation

    I have implemented everything as designed and answered to previous questions. I have chosen same data schema for both tenants to ease the implementation. First we run docker compose to create 3 nodes of cassandra . then we run our Flask api mysimbdp_daas.py . Then we start our batchingestmanager.py which starts to monitor the input folder as soon as it notices new file in any of folders it checks compliance , then runs the appropriate client's ingest app. I have created following tables and configured them in clientingest apps :
     
     * tenant_1 has tables named : listings , Listing_table_2. 
     * tenant_2 has tables named : another_listing , New_table_2

    I have tested with 3 data files one is 5556 rows and the other is 237 rows. 3rd one called data_2 is copy of 1st one.
    The results are :

     ![Stat](Stat.JPG "stat")

     According to our test maximum speed i got was 5062 bytes/second. This is because my local implementation limits. We can speed it up by increasing batch rows size from 200. Because of my local implementation this was the max batch size I could put. We could almost double the speed if we could ingest using more than 300 rows of batch size.



5. *Implement and provide logging features for capturing successful/failed ingestion as well as metrics about ingestion time,
data size, etc., for files which have been ingested into mysimbdp. Logging information must be stored in separate files,
databases or a monitoring system for analytics of ingestion. Show and explain simple statistical data extracted from logs for
individual tenants and for the whole platform with your tests.* 

    I have used the logging module of python to provide logging features. I used it mainly inside the common tool since that's where most of action is going on and I also use it in batchingestmanager because it is the first point of interaction with inputs. I have one logs.log file where overall all logs about system goes and then I have <tenant_id>logs.log where logs of specific tenants go.  
    Example of logs.log :

        INFO:BatchIngestManagerLogger:Started Batch Ingest Manager
        INFO:BatchIngestManagerLogger:New file detected: tenant_1/Listing_table_2/data_small.csv
        INFO:MYSIMBDP Logger:Started MYSIMBDP Logger
        INFO:tenant_1Logger::Tenant Common tool initiated
        INFO:MYSIMBDP Logger:Creating the table for  tenant_id=tenant_1, file_name=data_small.csv, table_name=Listing_table_2
        INFO:tenant_1Logger::Creating the table for  tenant_id=tenant_1, file_name=data_small.csv, table_name=Listing_table_2
        INFO:MYSIMBDP Logger:Starting batch Ingest
        INFO:tenant_1Logger::Starting batch Ingest
        INFO:MYSIMBDP Logger:Finished the ingestion 
        status=success, tenant_id=tenant_1, file_name=data_small.csv, file_size_bytes=19477, table_name=Listing_table_2, ingestion_rows=237, total_time_cost=5.690535545349121 seconds
        INFO:tenant_1Logger::Finished the ingestion 
        status=success, tenant_id=tenant_1, file_name=data_small.csv, file_size_bytes=19477, table_name=Listing_table_2, ingestion_rows=237, total_time_cost=5.690535545349121 seconds
        INFO:BatchIngestManagerLogger:New file detected: tenant_1/listings/data_small.csv
        INFO:MYSIMBDP Logger:Started MYSIMBDP Logger
        INFO:tenant_1Logger::Tenant Common tool initiated
        INFO:MYSIMBDP Logger:Creating the table for  tenant_id=tenant_1, file_name=data_small.csv, table_name=listings
        INFO:tenant_1Logger::Creating the table for  tenant_id=tenant_1, file_name=data_small.csv, table_name=listings
        INFO:MYSIMBDP Logger:Starting batch Ingest
        INFO:tenant_1Logger::Starting batch Ingest
        INFO:MYSIMBDP Logger:Finished the ingestion 
        status=success, tenant_id=tenant_1, file_name=data_small.csv, file_size_bytes=19477, table_name=listings, ingestion_rows=237, total_time_cost=7.287861585617065 seconds
        INFO:tenant_1Logger::Finished the ingestion 
        status=success, tenant_id=tenant_1, file_name=data_small.csv, file_size_bytes=19477, table_name=listings, ingestion_rows=237, total_time_cost=7.287861585617065 seconds
        INFO:BatchIngestManagerLogger:New file detected: tenant_2/another_listing/data_small.csv

    Example of tenant_1logs.log:

            Tenant Common tool initiated
            Creating the table for  tenant_id=tenant_1, file_name=data_small.csv, table_name=Listing_table_2
            Starting batch Ingest
            Finished the ingestion 
            status=success, tenant_id=tenant_1, file_name=data_small.csv, file_size_bytes=19477, table_name=Listing_table_2, ingestion_rows=237, total_time_cost=5.690535545349121 seconds
            Tenant Common tool initiated
            Creating the table for  tenant_id=tenant_1, file_name=data_small.csv, table_name=listings
            Starting batch Ingest
            Finished the ingestion 
            status=success, tenant_id=tenant_1, file_name=data_small.csv, file_size_bytes=19477, table_name=listings, ingestion_rows=237, total_time_cost=7.287861585617065 seconds
            Tenant Common tool initiated
            Creating the table for  tenant_id=tenant_1, file_name=data.csv, table_name=Listing_table_2
            Starting batch Ingest
            Finished the ingestion 
            status=success, tenant_id=tenant_1, file_name=data.csv, file_size_bytes=470514, table_name=Listing_table_2, ingestion_rows=5556, total_time_cost=60.319260120391846 seconds
            Tenant Common tool initiated
            Creating the table for  tenant_id=tenant_1, file_name=data.csv, table_name=listings
            Starting batch Ingest
            Finished the ingestion 
            status=success, tenant_id=tenant_1, file_name=data.csv, file_size_bytes=470514, table_name=listings, ingestion_rows=5556, total_time_cost=68.4549822807312 seconds

Logs contain creation of new tables, new input data files, Ingestion results and some statistics about ingestion.


## Part 2 - Near-realtime data ingestion.
1. *Tenants will put their data into messages and send the messages to a messaging system, mysimbdp-messagingsystem (provisioned by mysimbdp) and tenants will develop ingestion programs, clientstreamingestapp, which read data from the
messaging system and ingest the data into mysimbdp-coredms. For near-realtime ingestion, explain your design for the
multi-tenancy model in mysimbdp: which parts of the mysimbdp will be shared for all tenants, which parts will be dedicated
for individual tenants so that mysimbdp can add and remove tenants based on the principle of pay-per-use. Design and
explain a set of constraints for the tenant service profile w.r.t. data ingestion.*



     As a Messagging system I have decided to use RabbitMQ since it is a widely used too lfor such systems and I had prior basic knowdledge about it. As we have leveraged  CassandraDB's features like keyspaces to isolate different tenant's data and operations we can do a similar isolation using vhost feature in RabbitMQ. Thus all tenants will share same server of RabbitMQ but still they will have different vhosts to differentiate between their message queues. We can also leverage  Routing keys in every vhost  to use as our message queues. This way all data producer needs to do is to send its message with approppriate routing key  to start ingestion to the right table.

    As in a previous part our mysimbdp can have constraint JSON file which contains all the tenant's and their individual specific constraints. Thus our platform can control all the constraint and accesses through this unique file but at the same  time it would still be able to define everything for individual tenants. We can have different constraint json files for streaming and ingestion or we can also have 2 different files to further isolate batch and stream ingestion proceses. Constraints can have limits for tables to limit number of queues, we can limit size of a queue or size of a message.

    As before we as a platform provider provide CommonTool for clientingestapps to provide stream ingestiona features. Design will be similar to batch ingestion at mysimbdp side but a bit different on data input and handling side. This will be discussed more in details in next questions but overall Design looks like :



    ![Stream Design](stream_design.png "Stream Design")


2. *Design and implement a component mysimbdp-streamingestmanager, which can start and stop clientstreamingestapp instances on-demand. mysimbdp imposes the model that clientstreamingestapp has to follow so that mysimbdpstreamingestmanager can invoke clientstreamingestapp as a blackbox, explain the model.* 

    My Streamingestmanager is designed to create threads for each tenant so that they can consume from their RabbitMQ vhost s parallelly.  Every thread  starts consuming from their approppriate queues and when there is a new message in the queue they create an instance of ClientStreamIngestApp Class provided by our tenant  and use its function to initiate the ingest process with consumed message. Please notice that since each thread is isolated from each other then we can also provide analytics features for each tenant thus I also do some simple statistics and analysis in each thread. Thread gets ingestion results from ClientStreamIngest and writes statistics to  a file. More about it in following questions...

    ClientStreamIngestApp follows the model imposed by platform provider. I have imposed it to be a callable class so that it can be easily imported to streamingestmanager thread and its function can be called whenever there is a message.






3. *Develop test ingestion programs (clientstreamingestapp), test data, and test service profiles for tenants. Show the
performance of ingestion tests, including failures and exceptions, for at least 2 different tenants in your test environment.
What is the maximum throughput of the ingestion in your tests?*



    I have developed clientstreamingestapp as I ahve designed above and my streamingestmanager iss running threads paralelly to check the queues and start ingestion. I am testing a small file which has 237 rows and during my test none of the ingestions got failed and all of them have been successfully ingested. 


    ![Test](Capture.JPG "Capture")


    I have got some performance issues due to some implementation difficulties. In my implementation due to some initial mistakes I had to create table evrytime I get a new message. Cassandra supports multiple queries for creation of same table and doesn't fail if there is already such a table existent so initially this was not a problem. As first goal was making a working platform and I didn't have access to full statistics , I learned only at the end that because I am querying twice daas for every message I am getting decreased performance. Average ingestion time over 230 rows of data was 2.75 seconds per row and it was in total 55414 bytes of data being processed. It makes 87 bytes per second which seems quite low. For some reason my rabbitMQ was also slow (maybe because I was running all containers locally) . But anyway I couldn't improve it further since that would complicate my implementation very much and possibly break at least slowly but fully working system.

4. *clientstreamingestapp decides to report the its processing rate, including average ingestion time, total ingestion data size,
and number of messages to mysimbdp-streamingestmonitor within a pre-defined period of time. Design the report format
and explain possible components, flows and the mechanism for reporting.* 

    clientstreamingestapp returns ingestion time and sizes messages and overall data is available from streamingestmanager. Thus I have designed and implemented such statistics feature inside streamingestmanager (since its threads basically import clientingestapp class and runs it ). At every 5th (we can change this to any number) message it outputs following report to a file called streaming_monitor.csv .

    |processing_rate            |average_ingestion_time|total_ingestion_data_size|number_of_messages|
    |---------------------------|----------------------|-------------------------|------------------|
    |        0.1418022124085645 |3.076353359222412     |1198                     |5                 |
    |        0.17878187008215257|2.066713047027588     |2405                     |10                |
    |        0.19597108478829345|2.0658872922261557    |3606                     |15                |
    |        0.19590889070555706|2.062541353702545     |4793                     |20                |
    |        0.2124623040219599 |1.8617733669281007    |5996                     |25                |
    |        0.20244775251215838|2.226233418782552     |7209                     |30                |
    |        0.19569575202096975|2.345375667299543     |8433                     |35                |
    |        0.19094800733097061|2.310488724708557     |9640                     |40                |
    |        0.1835569398192949 |2.2828392611609565    |10801                    |45                |
    |        0.18808822492492897|2.161802306175232     |11976                    |50                |
    |        0.17946450662856783|2.3340127988295123    |13188                    |55                |
    |        0.17538122773139897|2.477855396270752     |14423                    |60                |
    |        0.17430404389995863|2.5245310563307544    |15631                    |65                |




5. *Implement a feature in mysimbdp-streamingestmonitor to receive the report from clientstreamingestapp. Based on the
report from clientstreamingestapp and the tenant profile, when the performance is below a threshold, e.g., average
ingestion time is too low, mysimbdp-streamingestmonitor decides to inform mysimbdp-streamingestmanager about the
situation. Implementation a feature in mysimbdp-streamingestmanager to receive information informed by mysimbdpstreamingestmonitor.*

    Since we already have constantly updated performance statistics in streaming_monitor.csv, streamingestmonitor can constantly read the file and  get if performance is below threshold.it could sned signals to streamingestmanager  (unix process signals) or it is possible to change the implementation of ingestmanager to add such feature using reading files. But it was already splitting to threads and subscribing to consume the queues so I wasn't able to modify this component more by implementing this feature. 


### *ALL THE INSTRUCTIONS ON HOW TO TEST THE IMPLEMENTATIONS ARE EXPLAINED DETAILED IN DEPLOYMENT.MD*

## Part 3 - Integration and Extension


1. *Produce an integrated architecture for the logging and monitoring of both batch and near-realtime ingestion features (Part 1,
Point 5 and Part 2, Points 4-5) so that you as a platform provider could know the amount of data ingested and existing
errors/performance for individual tenants.* 
2. *In the stream ingestion pipeline, assume that a tenant has to ingest the same data but to different sinks, e.g., mybdpcoredms for storage and a new mybdp-streamdataprocessing component, what features/solutions you can provide and
recommend to your tenant?*
3. *The tenant wants to protect the data during the ingestion using some encryption mechanisms, e.g., clientbatchingestapp
and clientstreamingestapp have to deal with encrypted data. Which features/solutions you recommend the tenants and
which services you might support them for this goal?*

4. *In the case of batch ingestion, we want to (i) detect the quality of data to allow ingestion only for data with a pre-defined
quality of data condition and (ii) store metadata, including detected quality, into the platform, how you, as a platform provider,
and your tenants can work together?*
5. *If a tenant has multiple clientbatchingestapp and clientstreamingestapp, each is suitable for a type of data and has
different workloads (e.g., different CPUs, memory consumption and execution time), how would you extend your design and
implementation in Parts 1 & 2 (only explain the concept/design) to support this requirement?*

