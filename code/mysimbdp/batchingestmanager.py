import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler
import json
import logging
CURRENT_DIRECTORY = os.getcwd()
WATCH_PATH = CURRENT_DIRECTORY + '/../../data/client-staging-input-directory'

logging.basicConfig(filename=os.getcwd() + '/../../logs/logs.log',  level=logging.INFO)
logger = logging.getLogger("BatchIngestManagerLogger")
logger.info("Started Batch Ingest Manager")

class Handler(FileSystemEventHandler):

    def __init__(self, **kwargs):
        super(Handler, self).__init__(**kwargs)
    
    def on_created(self, event):

        if event.is_directory:
           
            if event.src_path.split('/')[-2] == 'client-staging-input-directory':
                new_tenant = event.src_path.split('/')[-1]
                logger.info("New tenant detected: %s" % new_tenant)
                print("[BatchIngestManager] New tenant detected: %s" % new_tenant)
            if event.src_path.split('/')[-3] == 'client-staging-input-directory':
                new_table = event.src_path.split('/')[-1]
                tenant = event.src_path.split('/')[-2]
                logger.info("New table %s detected for %s" % (new_table, tenant))
                
                print("[BatchIngestManager] New table %s detected for   %s" % (new_table, tenant))    

        else:  
            print("New file detected")
            if event.src_path.split('/')[-4] == 'client-staging-input-directory':
                tenant_id = event.src_path.split('/')[-3]
                table_name = event.src_path.split('/')[-2]
                file_name = event.src_path.split('/')[-1]
                logger.info("New file detected: {}/{}/{}".format(tenant_id, table_name, file_name))
                 
                print("[BatchIngestManager] New file detected: {}/{}/{}".format(tenant_id, table_name, file_name))
                print("Event source is"+event.src_path)
                print(os.path.getsize(event.src_path))
                with open('constraint_ingestion.JSON') as json_file:
                        constraints = json.load(json_file)
                        constraint=None
                        for constraint in constraints:
                            if tenant_id == constraint['tenant_id']:
                                break
                        if(constraint==None):
                            print("Tenant doesn't exist or have no access!")
                            logger.info("Tenant doesn't exist or have no access!")

                        elif ( event.src_path.endswith(constraint["valid_file_format"])) and (os.path.getsize(event.src_path) <= int(constraint["max_file_size_MB"])*1024*1024):

                            os.system("python3 {current}/../client_ingest_apps/{tenant_id}/clientbatchingestapp.py {table_name} {file_name}".format(current=CURRENT_DIRECTORY, tenant_id=tenant_id, table_name=table_name, file_name=file_name))
                        else:
                            print("New tenant file is not consistent with its constraints!")
                            logger.info("New tenant file is not consistent with its constraints!")


if __name__ == '__main__':
    event_handler = Handler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_PATH, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:  # pree ctrl+c to end this program
        observer.stop()
    observer.join()
