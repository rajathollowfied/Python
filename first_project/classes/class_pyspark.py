# importing modules
import json, os, re, sys
from typing import Callable,Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

class Sparkclass:

    def __init__(self, strdict):
        self.strdict = strdict
    
    def sparkStart(self, kwargs:dict):
        print(kwargs)