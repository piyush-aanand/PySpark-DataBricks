# Databricks notebook source
# Creating DataFrame from hardcoded values

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Example1').getOrCreate()

# COMMAND ----------

# Method-1

# COMMAND ----------

data=[(1,'Piyush'),(2,'Anand')]
schema=['id','name']
df=spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

# Method-2

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
data=[(1,'Piyush'),(2,'Anand')]
schema=StructType([StructField(name='id',dataType=IntegerType()),
                   StructField(name='name',dataType=StringType())])
df=spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

# Method-3

# COMMAND ----------

data=[{'id':1,'name':'Piyush'},{'id':2,'name':'Anand'}]
df=spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------


