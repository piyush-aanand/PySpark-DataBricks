# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example32").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush','male',30000,None),(2,'Ayushi','female',None,'IT'),(3,'abcd',None,20000,'HR')]
schema=['id','name','gender','salary','dept']

df=spark.createDataFrame(data,schema)
df.show()

df.printSchema()

# COMMAND ----------

# DBTITLE 1,fillna()- replaces null with string literal
df.fillna('unknown',['dept']).show()

# COMMAND ----------

# DBTITLE 1,na.fill()- replaces null with string literal
df.na.fill('unknown').show()

# COMMAND ----------

# DBTITLE 1,Replacing null with Integer literal
df.fillna(0,['salary']).show()
