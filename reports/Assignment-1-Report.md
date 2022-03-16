

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

4. *Explain your design for the multi-tenancy model in mysimbdp: which parts of mysimbdp will be shared for all tenants,
which parts will be dedicated for individual tenants so that you as a platform provider can add and remove tenants based on
the principle of pay-per-use. Develop test programs (clientbatchingestapp), test data, and test constraints of files, and test
service profiles for tenants according your deployment. Show the performance of ingestion tests, including failures and
exceptions, for at least 2 different tenants in your test environment and constraints. What is the maximum amount of data
per second you can ingest in your tests?*


5. *Implement and provide logging features for capturing successful/failed ingestion as well as metrics about ingestion time,
data size, etc., for files which have been ingested into mysimbdp. Logging information must be stored in separate files,
databases or a monitoring system for analytics of ingestion. Show and explain simple statistical data extracted from logs for
individual tenants and for the whole platform with your tests.* 



## Part 2 - Near-realtime data ingestion




   

  
    ![Design Schema](Design_Schema.jpg "Design Schema")

    





| ID      | HostId | Host_name      | neighbourhood | Latitude      | Longitude | Room_type      | Price | Availability_365  | 
| ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- |
| Listing Id -  Integer      |  Integer    |text    | text    | float    | Float    |  text    | Integer    |Availability - Integer   |

