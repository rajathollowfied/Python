shell = True

import pyspark
import findspark

findspark.init('C:\spark\spark-3.3.2-bin-hadoop3')

sc = pyspark.SparkContext(appName="myAppName")
#spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

sc.stop()
