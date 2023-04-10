# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example7").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Reading parquet file from DBFS
df=spark.read.parquet("dbfs:/FileStore/tables/userdata1.parquet")
display(df)
df.count()

# COMMAND ----------

# DBTITLE 1,Reading from multiple parquet files 
df=spark.read.parquet("dbfs:/FileStore/tables/*.parquet")
display(df)
print("The count of data rows present in dataframe is: ",df.count())
