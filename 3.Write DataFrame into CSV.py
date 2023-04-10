# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Exercise3").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush'),(2,'Anand')]
schema=['id','name']
df=spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

# Writing to a csv file inside DBFS

# COMMAND ----------

df.write.csv(path='dbfs:/FileStore/Demo1/',header=True)

# COMMAND ----------

df2=spark.read.csv(path='dbfs:/FileStore/Demo1/',header=True)
df2.show()

# COMMAND ----------


