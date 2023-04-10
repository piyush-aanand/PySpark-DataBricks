# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example30").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush','male','IT'),\
      (2,'Anand','male','HR'),\
      (3,'Ayushi','female','IT'),\
      (4,'Nishant','male','IT'),\
      (5,'Shalini','female','HR'),\
      (6,'Aanantha','female','HR'),\
      (7,'Sapna','female','IT'),\
      (8,'Ankit','male','IT')]
schema=['id','name','gender','dept']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,GroupBy
df.groupBy('dept','gender').count().show()

# COMMAND ----------

# DBTITLE 1,pivot()
df.groupBy('dept').pivot('gender').count().show()

# COMMAND ----------

#For specific values of pivot column
df.groupBy('dept').pivot('gender',['female']).count().show()
