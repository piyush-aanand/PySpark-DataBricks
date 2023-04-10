# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example26").getOrCreate()

# COMMAND ----------

data1=[(1,'Piyush','male')]
sc1=['id','name','gender']
data2=[(1,'Piyush',2000)]
sc2=['id','name','salary']

df1=spark.createDataFrame(data1,sc1)
df2=spark.createDataFrame(data2,sc2)

df1.union(df2).show()

df1.unionByName(allowMissingColumns=True,other=df2).show()
