# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example35").getOrCreate()

# COMMAND ----------

data=[(1,'piyush',30000),(2,'nishant',35000)]
schema=['id','name','salary']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,transform()
from pyspark.sql.functions import upper
def lowerToUpper(df):
    return df.withColumn('name',upper(df.name))
def doubleSalary(df):
    return df.withColumn('salary',df.salary*2)
df1=df.transform(lowerToUpper).transform(doubleSalary)
df1.show()
