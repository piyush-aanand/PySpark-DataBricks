# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example22").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush','male',2000,'IT'),(2,'Shalini','female',3000,'HR'),(3,'Anand','male',4000,'Payroll'),(4,'Aayushi','female',5000,'HR')]
schema=['id','name','gender','salary','dept']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,sort()
#Method-1
df.sort(df.dept.desc()).show()
#Method-2 (in form of sql expressions)
df.sort('dept').show()

# COMMAND ----------

# DBTITLE 1,orderBy()
#Method-1
df.orderBy(df.dept.desc(),df.id.desc()).show()
#Method-2 (in form of sql expressions)
df.orderBy('dept','id').show()
