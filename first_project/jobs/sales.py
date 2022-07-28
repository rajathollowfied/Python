# importing modules
import logging
import json, os, re, sys
from typing import Callable,Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
import findspark
findspark.init()

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

def main() -> None:
    
    #calling function to open and read the file(s) and return contents as dict type
    jsonConfig = openConfigFile(f"{proj_dir}/config_json/sales.json")
    
    #starts the spark job
    start = sparkStart(jsonConfig)
    
    #import json files from the specified directory
    transactionsDf = importData(start, f"{proj_dir}/test_data/sales/transactions",".json$")
    customerDf = importData(start, f"{proj_dir}/test_data/sales/customers.csv")
    productsDf = importData(start, f"{proj_dir}/test_data/sales/products.csv")

    transformData(start,transactionsDf,customerDf,productsDf, f"{proj_dir}/test-delta/sales")
    
    sparkStop(start)    #stop the spark job
    
def openConfigFile(filepath:str) -> dict:
    if isinstance(filepath,str) and os.path.exists(filepath):
        return class_pyspark.Sparkclass(strdict={}).openJson(filepath)

def sparkStart(jsonConfig:dict) -> SparkSession:
    if isinstance(jsonConfig,dict):
         return class_pyspark.Sparkclass(strdict={}).sparkStart(jsonConfig)
       
def sparkStop(start:SparkSession) -> None:
    if isinstance(start,SparkSession):
        start.stop() #pre-built stop function in SparkSession   

def importData(start:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
    if isinstance(start, SparkSession):
        return class_pyspark.Sparkclass(strdict={}).importData(start, datapath, pattern)

def showMySchema(df:DataFrame, filename:str) -> None:
    if isinstance(df, DataFrame):
        class_pyspark.Sparkclass(strdict={}).debugDataFrames(df, filename)

def transformData(start:SparkSession,transactionsDf:DataFrame,customerDf:DataFrame,productsDf:DataFrame, path:str ) -> DataFrame:
    
    #createTables(start,[(cleanTransactions(transactionsDf),"transactions_table"),(cleanCustomers(customerDf),"customers_table"),(cleanProducts(productsDf),"products_table")])
     exportTables([
        (start,cleanTransactions(transactionsDf), {"format":"delta", "path":f"{path}/transactions", "key":"date_of_purchase"}),
        (start,cleanCustomers(customerDf),{"format":"delta", "path":f"{path}/customers", "key":"customer_id"}),
        (start,cleanProducts(productsDf),{"format":"delta", "path":f"{path}/products", "key":"product_id"})
    ])

    

def cleanTransactions(df:DataFrame) -> DataFrame:
                            
    if isinstance(df, DataFrame):

        df1 = df.withColumn("basket_explode", explode(col("basket"))).drop("basket")
        df2 = df1.select(col("customer_id"), \
                        col("date_of_purchase"), \
                        col("basket_explode.*"))    \
                            .withColumn("date", col("date_of_purchase").cast("Date"))   \
                                .withColumn("price", col("price").cast("Integer"))
        showMySchema(df2,"transactionsDf")
        return df2

def cleanCustomers(df:DataFrame) -> DataFrame:
    if isinstance(df, DataFrame):
        df1 = df.withColumn("loyalty_score", col("loyalty_score").cast("Integer"))
        showMySchema(df1,"customersDf")
        return df1

def cleanProducts(df:DataFrame) -> DataFrame:
    if isinstance(df, DataFrame):
       showMySchema(df,"productsDf")
       return df

def createTables(start:SparkSession,listOfDf:list):
    t = [ ( lambda x: class_pyspark.Sparkclass(strdict={}).createTempTables(x) ) (x) for x in listOfDf ]
    d = [ ( lambda x: class_pyspark.Sparkclass(strdict={}).debugTables(x) ) (x) for x in start.catalog.listTables() ]

def exportTables(listOfDf:list):
    t = [ ( lambda x: class_pyspark.Sparkclass(strdict={}).exportDf(x) ) (x) for x in listOfDf ]
    
if __name__ == '__main__':
    main()