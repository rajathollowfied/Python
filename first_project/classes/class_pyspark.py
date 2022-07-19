# importing modules
import json, os, re, sys
from typing import Any, Callable,Optional
from venv import create
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext
from jobs import sales

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
                    return openFile(Sparkclass(self.strdict).getFileExtension, datapath)

        def openDirectory(spark:SparkSession,datapath:str, pattern:Optional[str]=None) -> DataFrame:                            #opening the directory
            if isinstance(datapath, str) and os.path.exists(datapath):
                filelist = Sparkclass(self.strdict).listDirectory(datapath, pattern)
                filetype = getUniqueFileExtensions(filelist)
                if filetype:
                    return Sparkclass(self.strdict).createDataFrame(spark, filelist, filetype)
        
        def openFile(getFileExtension:callable,filepath:str):
            if isinstance(filepath, str) and os.path.exists(filepath):
                filelist = [filepath]
                filetype = getFileExtension(filepath)
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

    def debugDf(self, df:DataFrame, filename:str) -> None:
        
        def makeDirectory() -> None:
            if not os.path.exists(f"{sales.proj_dir}/logs/"):
                os.makedirs(f"{sales.proj_dir}/logs/")

        def createFilepath(filename:str) -> str:
            return f"{sales.proj_dir}/logs/{filename}.json"
        
        def dfToString(df:DataFrame) -> str:
            return df._jdf.schema().treeString()

        def createContent(df:DataFrame) -> dict:
            content = {}
            content['count'] = df.count()
            content['schema'] = json.loads(df.schema.json())
            return json.dumps(content, sort_keys=False, indent = 4, default = str)
        
        def createFile(filepath:str, content:Any) -> None:
            with open(filepath, 'a') as f:
                f.write("{}\n".format(content))
            f.close()

        makeDirectory()
        filepath = createFilepath(filename)
        #content_log = dfToString(df)
        createFile(filepath,createContent(df))