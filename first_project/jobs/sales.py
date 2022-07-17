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
#LOG_FILE = f"{proj_dir}/logs/job-{job_name}.log"
#LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
#logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
#logger = logging.getLogger('py4j')

# including class directory containing custom modules
sys.path.insert(1, proj_dir)
from classes import class_pyspark

def main(proj_dir:str) -> None:
    
    #calling function to open and read the file(s) and return contents as dict type
    jsonConfig = openConfigFile(f"{proj_dir}/config_json/sales.json")
    
    #starts the spark job
    start = sparkStart(jsonConfig)
    
    #import json files from the specified directory
    transactionsDf = importData(start, f"{proj_dir}/test_data/sales/transactions",".json$")
    customerDf = importData(start, f"{proj_dir}/test_data/sales/customers.csv")
    productsDf = importData(start, f"{proj_dir}/test_data/sales/products.csv")

    transformData(start,transactionsDf,customerDf,productsDf)
    
    sparkStop(start)    #stop the spark job
    
def openConfigFile(jsonFile:str) -> dict:
    def openJson(jsonFile:str) -> dict:
        if isinstance(jsonFile,str) and os.path.exists(jsonFile):
            with open(jsonFile, "r") as filedata:
                data = json.load(filedata)
            return data
    return (openJson(jsonFile))

def sparkStart(jsonConfig:dict) -> SparkSession:
    if isinstance(jsonConfig,dict):
         return class_pyspark.Sparkclass(strdict={}).sparkStart(jsonConfig)
       
def sparkStop(start:SparkSession) -> None:
    if isinstance(start,SparkSession):
        start.stop() #pre-built stop function in SparkSession   

def importData(start:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
    if isinstance(start, SparkSession):
        return class_pyspark.Sparkclass(strdict={}).importData(start, datapath, pattern)

def showMySchema(df:DataFrame) -> None:
    if isinstance(df, DataFrame):
        df.show()
        df.printSchema()
        print("Total number of rows - ",df.count())

def transformData(start:SparkSession,transactionsDf:DataFrame,customerDf:DataFrame,productsDf:DataFrame) -> DataFrame:
    print(f"I AM TRANSFORMING --  \n{transactionsDf}\n SCHEMA --{showMySchema(transactionsDf)}")
    print(f"\n{customerDf}\n SCHEMA --{showMySchema(customerDf)}")
    print(f"\n{productsDf}\n SCHEMA --{showMySchema(productsDf)}")
       

if __name__ == '__main__':
    main(proj_dir)