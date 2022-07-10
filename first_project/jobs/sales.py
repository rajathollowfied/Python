# importing modules
from asyncio.log import logger
import logging
import json, os, re, sys
from typing import Callable,Optional
from unicodedata import name
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

# setting up environment variables and logger information
proj_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
job_name = os.path.basename(__file__)
LOG_FILE = f"{proj_dir}/logs/job-{job_name}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('py4j')

# including class directory containing custom modules
sys.path.insert(1, proj_dir)
from classes import class_pyspark

def main():
    class_pyspark.Sparkclass({"first":"testing..."}).sparkStart()

if __name__ == '__main__':
    main()

