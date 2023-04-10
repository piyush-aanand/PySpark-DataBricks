# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example18").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush Anand','M',50000),(2,'Shalini Sharma','F',35000),(3,'Oswald','',60000)]
df=spark.createDataFrame(data,['id','name','gender','salary'])
df.show(truncate=0)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Applying when() and otherwise()
from pyspark.sql.functions import when
df1=df.select(df.id,\
              df.name,\
              when(df.gender=='M','Male').\
              when(df.gender=='F','Female').\
              otherwise('Otherwise').alias('Gender'))
df1.show(truncate=0)
