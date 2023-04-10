# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Example4').getOrCreate()

# COMMAND ----------

# DBTITLE 1,JSON without multi-line
df = spark.read.json(path="dbfs:/FileStore/tables/emps.json")
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,JSON with multi-line
df1=spark.read.json(path="dbfs:/FileStore/tables/empsMline.json",multiLine=True)
df1.show()
df1.printSchema()

# COMMAND ----------


