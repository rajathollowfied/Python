# importing modules
import json, os, re, sys
from typing import Callable,Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext

class Sparkclass:

    def __init__(self, strdict):    #blank dict is passed from sales job
        self.strdict = strdict
    
    def sparkStart(self, kwargs:dict):
        MASTER = kwargs['spark_conf']['master']
        APP_NAME = kwargs['spark_conf']['app_name']
        #LOG_LEVEL = kwargs['log']['level']
        
        def createSession(master:Optional[str]="local[*]",app_name:Optional[str]="myapp"):
            #creating a spark session
            spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
            print(f"Session Created !! - {spark}")
            return spark

        def setLogging():
            pass

        def getSettings(spark:SparkSession) -> None:
            #show spark settings
            
            print(spark.sparkContext.getConf().getAll())
            
        spark = createSession(MASTER, APP_NAME)
        #getSettings(spark)

        return spark

    def importData(self, spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
        
        def fileOrDirectory(datapath:str) -> str:
            if isinstance(datapath,str) and os.path.exists(datapath):
                if os.path.isdir(datapath):
                    return "dir"
                elif os.path.isfile(datapath):
                    return "file"
        pathType = fileOrDirectory(datapath)
