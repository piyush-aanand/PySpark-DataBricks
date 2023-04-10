# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example23").getOrCreate()

# COMMAND ----------

data1=[(1,'Piyush','male'),(2,'Shalini','female')]
data2=[(2,'Shalini','female'),(3,'Anand','male'),(4,'Aayushi','female')]
schema=['id','name','gender']
df1=spark.createDataFrame(data1,schema)
df2=spark.createDataFrame(data2,schema)
df1.show()
df2.show()

# COMMAND ----------

# DBTITLE 1,union()/ unionAll()
newDf=df1.union(df2)
newDf.show()
newdDf1=df1.unionAll(df2)
newdDf1.show()
#Both union and unionAll will give same result
