import os
import sys
import csv
import requests
import logging
import pandas as pd
from datetime import datetime
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel


 
import logging
logging.basicConfig(filename=os.getcwd() + '/logs.log',  level=logging.INFO)
logger = logging.getLogger("NameOfLogger")
logger.info("Hello, world")
       
        