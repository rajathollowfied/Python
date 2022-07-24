# importing modules
import json, os, re, sys
from typing import Any, Callable,Optional, Tuple
from venv import create
from delta import configure_spark_with_delta_pip
from numpy import isin
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext
from jobs import sales
from delta import configure_spark_with_delta_pip
import findspark
findspark.init()

class Sparkclass:

    def __init__(self, strdict):    #blank dict is passed from sales job
        self.strdict = strdict
        self.debug_dir = f"{sales.proj_dir}/logs"
    
    def sparkStart(self, kwargs:dict):
               
        def createBuilder(master:str,app_name:str, config:dict) -> SparkSession.Builder:
            
            builder = SparkSession.builder.appName(app_name).master(master)
            
            return configDeltaLake(builder, config)

        def configDeltaLake(builder:SparkSession.Builder, config:dict):
            
            if isinstance(builder, SparkSession.Builder) and config.get('deltalake') == True:
                
                builder \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                
                return configure_spark_with_delta_pip(builder)
            
            else:
                return builder

        def createSparkSession(builder:SparkSession.Builder) -> SparkSession:
            
            if isinstance(builder, SparkSession.Builder):
                    
                return builder.getOrCreate()     #creating a spark session

        def setLogging():
            pass

        def getSettings(spark:SparkSession) -> None:
            """show spark settings"""
            c ={}
            c['spark.version'] = spark.version
            c['spark.sparkContext'] = spark.sparkContext.getConf().getAll()
            content = json.dumps(c, sort_keys = False, indent = 4, default = str)
            Sparkclass(self.strdict).debugcreateContext((f"{self.debug_dir}",f"{self.debug_dir}/SparkSession.json"), content)

        MASTER = kwargs.get('spark_conf', {}).get('master', 'local[*]')     #passing config values for spark session
        APP_NAME = kwargs.get('spark_conf').get('app_name', 'sales_app')
        CONFIG = kwargs.get('config')

        #LOG_LEVEL = kwargs.get('log').get('level')
        
        builder = createBuilder(MASTER, APP_NAME, CONFIG)
        spark = createSparkSession(builder) #Sparksession variable
        getSettings(spark)

        return spark
        
    
    def openJson(self, filepath:str) -> dict:
        if isinstance(filepath,str) and os.path.exists(filepath):
            with open(filepath, "r") as filedata:
                data = json.load(filedata)
            return data
        

    def importData(self, spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
        
        def fileOrDirectory(spark:SparkSession,datapath:str,pattern:Optional[str]=None) -> str:                       #determining whether it's a file or a directory
            if isinstance(datapath,str) and os.path.exists(datapath):
                if os.path.isdir(datapath):
                    return openDirectory(spark,datapath,pattern)
                elif os.path.isfile(datapath):
                    return openFile(spark, datapath)

        def openDirectory(spark:SparkSession,datapath:str, pattern:Optional[str]=None) -> DataFrame:                            #opening the directory
            if isinstance(datapath, str) and os.path.exists(datapath):
                filelist = Sparkclass(self.strdict).listDirectory(datapath, pattern)
                filetype = getUniqueFileExtensions(filelist)
                if filetype:
                    return Sparkclass(self.strdict).createDataFrame(spark, filelist, filetype)
        
        def openFile(spark:SparkSession,filepath:str):
            if isinstance(filepath, str) and os.path.exists(filepath):
                filelist = [filepath]
                filetype = Sparkclass(self.strdict).getFileExtension(filepath)
                return Sparkclass(self.strdict).createDataFrame(spark, filelist, filetype)

        def getUniqueFileExtensions(filelist:list) -> list:     # unique extentions in a path
            if isinstance(filelist, list) and len(filelist) > 0:
                extts = list(set([os.path.splitext(f)[1] for f in filelist]))
                return extts[0][1:] if len(extts) == 1 else None


        return fileOrDirectory(spark,datapath,pattern)                       
    
    def getFileExtension(self, file:str) -> str: #file extension of a single file
        if isinstance(file, str) and os.path.exists(file):
            filename, file_extension = os.path.splitext(file)
            return file_extension[1:] if file_extension else None


    def listDirectory(self, directory:str, pattern:Optional[str]=None) -> list:     #list contents in the directory

        def recursiveFilelist(directory:str) -> list:               #go through the contents of the directory recursively, return a list in the end
            filelist = []
            for dirpath, dirname, filenames in os.walk(directory):
                for filename in filenames:
                    filelist.append(f"{dirpath}/{filename}")
            return filelist
        
        def filterFiles(filelist:list, pattern:str):                #filtering contents in the directory and appending contents to the list in case pattern is matched
            newfilelist = []
            if isinstance(pattern,str):
                for contents in filelist:
                    if re.search(rf"{pattern}",contents):
                        newfilelist.append(contents)
                return newfilelist
            else:
                return filelist
                    
        filelist = recursiveFilelist(directory)
        return filterFiles(filelist,pattern)               

    def createDataFrame(self,spark:SparkSession,filelist:list,filetype:str) -> DataFrame:
        
        def dfFromCSV(filelist:list) -> DataFrame:
            
            if isinstance(filelist, list) and len(filelist) > 0:
                df = spark.read.format("csv") \
                    .option("header","true") \
                    .option("mode", "DROPMALFORMED") \
                    .load(filelist)
            return df              

        def dfFromJSON(filelist:list) -> DataFrame:
            
            if isinstance(filelist, list) and len(filelist) > 0:
                df = spark.read.format("json") \
                    .option("mode","PERMISSIVE") \
                    .option("primitiveAsString", "true") \
                    .load(filelist)
            return df
        
        def makeDf(filelist:list, filetype: str) -> DataFrame:
            return dfFromCSV(filelist) if filetype == "csv" else dfFromJSON(filelist) if filetype == "json" else None
        
        return makeDf(filelist, filetype)

    def createTempTables(self,tupleDf:tuple):
        if isinstance(tupleDf,tuple) and len(tupleDf) == 2:
            tupleDf[0].createOrReplaceTempView(tupleDf[1])
            return tupleDf

    def exportDf(self,tupleDf:tuple):
        if isinstance(tupleDf,tuple) and len(tupleDf) == 2 and self.strdict.get("export"):
            path = f"{self.strdict.get('export')}\{tupleDf[1]}"
            tupleDf[0].write.format("delta").mode("overwrite").save(path)

    def debugcreateContext(self, paths:tuple, content:dict) -> None:
        
        def makeDirectory(directory:str) -> None:
            if not os.path.exists(directory):
                os.makedirs(directory)
        
        def createFile(filepath:str, content:Any) -> None:
            with open(filepath, 'a') as f:
                f.write(content)
            f.close()

        def deleteFiles(filepath:str) -> None:
            if os.path.exists(filepath):
                os.remove(filepath)
        
        directory = paths[0]
        filepath = paths[1]
        
        makeDirectory(directory)
        deleteFiles(filepath)
        createFile(filepath,content)
    
    def debugDataFrames(self,df:DataFrame, filename:str) -> None:

        def createFilePath(directory:str, filename:str) -> str:
            d = f"{directory}/dataframes"
            return d, f"{d}/{filename}.json"

        def createContent(df:DataFrame) -> dict:
            content = {}
            content['count'] = df.count()
            content['schema'] = json.loads(df.schema.json())
            return json.dumps(content, sort_keys=False, indent = 4, default = str)

        path = createFilePath(self.debug_dir, filename)
        Sparkclass(self.strdict).debugcreateContext(path, createContent(df))
    
    def debugTables(self, table) -> None:
        
        def createFilePath(directory:str, filename:str) -> str:
            d = f"{directory}/tables"
            return d, f"{d}/{filename}.json"

        def createTables(table) -> dict:
            content = {}
            content['table'] = table._asdict()
            content['dir.table'] = dir(table)
            return json.dumps(content, sort_keys=False, indent = 4, default = str)

        path = createFilePath(self.debug_dir, table.name)
        Sparkclass(self.strdict).debugcreateContext(path, createTables(table))