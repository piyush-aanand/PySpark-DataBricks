# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example6").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush'),(2,'Anand')]
schema=['id','name']
df=spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

# DBTITLE 1,Writing DataFrame to json in DBFS
df.write.json('dbfs:/FileStore/jsonData')

# COMMAND ----------

# DBTITLE 1,Reading the stored json from DBFS
df1=spark.read.json('dbfs:/FileStore/jsonData/')
display(df1)
