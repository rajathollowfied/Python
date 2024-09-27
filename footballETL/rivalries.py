import findspark
findspark.init()
from pyspark.sql import SparkSession, functions as F
import logging

logging.basicConfig(filename='D:\Learning\GIT\Python//footballETL\logs\etl.log',format='%(asctime)s - %(levelname)s - %(message)s')

spark = SparkSession.builder.appName("test").getOrCreate()

#pdf= pd.read_csv('D://Learning//GIT//datasets//football//Match_Results.csv')
#pdf = pdf.apply(lambda col: col.str.replace(r',(?![^"]*")', '',regex=True).str.strip() if col.dtype == 'object' else col)

sdf = spark.read.csv('D://Learning//GIT//datasets//football//Match_Results.csv',header = True, inferSchema=True)
columnTrans = sdf.columns

print(sdf.dtypes)


if(sdf.dtypes=='string'):
    for field in columnTrans:
        sdf = sdf.withColumn(
            field,
            F.when(
                F.regexp_replace(F.trim(F.col(field)),r',(?![^"]*")', '')
            )
        )
print(sdf.dtypes)

sdf.createOrReplaceTempView("table")

result = spark.sql("SELECT * FROM table")
print(result.show())

spark.stop()