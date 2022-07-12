# importing modules
import logging
import json, os, re, sys
from typing import Callable,Optional
from pyspark import SparkStageInfo
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

def main(proj_dir:str) -> None:
    #calling function to open and read the file(s) and return contents as dict type
    config = openFile(f"{proj_dir}/config_json/sales.json")
    #starts the spark job
    start = sparkStart(config)
    #stop the spark job
    sparkStop(start)
    
def openFile(jsonFile:str) -> dict:
    def openJson(jsonFile:str) -> dict:
        if isinstance(jsonFile,str) and os.path.exists(jsonFile):
            with open(jsonFile, "r") as f:
                data = json.load(f)
            return data
    return (openJson(jsonFile))

def sparkStart(config:dict) -> SparkSession:
    if isinstance(config,dict):
        class_pyspark.Sparkclass(strdict={}).sparkStart(config)
       
def sparkStop(spark:SparkSession) -> None:
    if isinstance(spark,SparkSession):
        spark.stop()    

if __name__ == '__main__':
    main(proj_dir)