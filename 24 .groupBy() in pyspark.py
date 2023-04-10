# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example24").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush','M',5000,'IT'),\
       (2,'Shalini','F',6000,'IT'),\
       (3,'Aayushi','F',2500,'Payroll'),\
       (4,'Nishant','M',4000,'HR'),\
       (5,'Priyaranjan','M',2000,'HR'),\
       (6,'Manisha','F',2000,'Payroll'),\
       (7,'Ayush','M',3000,'IT')]
schema=['id','name','gender','salary','dept']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,groupBy()
df.groupBy('dept').count().show()

# COMMAND ----------

df.groupBy('dept').min('salary').show()

# COMMAND ----------

df.groupBy('dept','gender').count().show()
