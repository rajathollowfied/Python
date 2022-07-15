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
        MASTER = kwargs['spark_conf']['master']     #passing config values for spark session
        APP_NAME = kwargs['spark_conf']['app_name']
        #LOG_LEVEL = kwargs['log']['level']
        
        def createSession(master:Optional[str]="local[*]",app_name:Optional[str]="myapp"):
            spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()     #creating a spark session
            print(f"Session Created !! - {spark}")
            return spark

        def setLogging():
            pass

        def getSettings(spark:SparkSession) -> None:
            print(spark.sparkContext.getConf().getAll())    #show spark settings
            
        spark = createSession(MASTER, APP_NAME) #Sparksession variable

        return spark

    def importData(self, spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
        
        def fileOrDirectory(datapath:str) -> str:                       #determining whether it's a file or a directory
            if isinstance(datapath,str) and os.path.exists(datapath):
                if os.path.isdir(datapath):
                    return "dir"
                elif os.path.isfile(datapath):
                    return "file"

        def openDirectory(getUniqueFileExtensions:Callable,datapath:str, pattern:Optional[str]=None) -> list:                            #opening the directory
            if isinstance(datapath, str) and os.path.exists(datapath):
                filelist = Sparkclass(self.strdict).listDirectory(datapath, pattern)
                filetype = getUniqueFileExtensions(filelist)
                print(filetype)
        
        def openFile(filepath:str):
            if isinstance(filepath, str) and os.path.exists(filepath):
                filelist = [filepath]


        def getUniqueFileExtensions(filelist:list) -> list:
            if isinstance(filelist, list) and len(filelist) > 0:
                extts = list(set([os.path.splitext(f)[1] for f in filelist]))
                return extts[0] if len(extts) == 1 else None


        pathtype = fileOrDirectory(datapath)                       #str type dir/file
        
        if pathtype == "dir":
            openDirectory(getUniqueFileExtensions,datapath,pattern)
        else:
            None
        
    def listDirectory(self, directory:str, pattern:Optional[str]=None) -> list:     #list contents in the directory

        def recursiveFilelist(directory:str) -> list:               #go through the contents of the directory recursively, return a list in the end
            filelist = []
            for dirpath, dirname, filenames in os.walk(directory):
                for filename in filenames:
                    filelist.append(f"{dirpath}/{filename}")
            return filelist
        
        def filterFiles(filelist:list, pattern:str):                #filtering contents in the directory and appending contents to the list in case pattern is matched
            newfilelist = []
            for contents in filelist:
                if re.search(rf"{pattern}",contents):
                    newfilelist.append(contents)
            return newfilelist
                    
        filelist = recursiveFilelist(directory)
        

        return filelist  if pattern == None else filterFiles(filelist, pattern) if pattern == ".json$" else print ("ffs")

def createDataFrame(self,spark:SparkSession,filelist:list,filetype:str) -> DataFrame:
    
    def dfFromCSV(filelist:list) -> DataFrame:
        pass


    def dfFromJSON(filelist:list) -> DataFrame:
        pass