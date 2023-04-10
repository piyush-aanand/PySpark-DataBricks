# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example8").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush'),(2,'Anand')]
schema=['id','name']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,Writing DataFrame into Parquet file
df.write.parquet('dbfs:/FileStore/ParquetData')

# COMMAND ----------

# DBTITLE 1,Reading parquet file stored in DBFS
df1=spark.read.parquet('dbfs:/FileStore/ParquetData/')
display(df1)

# COMMAND ----------


