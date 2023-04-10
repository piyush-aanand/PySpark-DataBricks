# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example2").getOrCreate()

# COMMAND ----------

# Case-1: Only one csv file
# Method-1

# COMMAND ----------

df=spark.read.csv(path="/FileStore/tables/Employees1.csv",header=True)
df.show()
df.printSchema()

# COMMAND ----------

#Method-2

# COMMAND ----------

df=spark.read.format('csv').option(key='header',value='True').load(path="/FileStore/tables/Employees1.csv")
df.show()
df.printSchema()

# COMMAND ----------

#Case-2: Two or more csv files at once in form of list

# COMMAND ----------

df=spark.read.csv(path=["/FileStore/tables/Employees1.csv","/FileStore/tables/Employees2.csv"],header=True)
df.show()
df.printSchema()

# COMMAND ----------

#Case-3: Considering the schema of csv file too

# COMMAND ----------

df=spark.read.csv(path=["/FileStore/tables/Employees1.csv","/FileStore/tables/Employees2.csv"],header=True,inferSchema=True)
df.show()
df.printSchema()

# COMMAND ----------


