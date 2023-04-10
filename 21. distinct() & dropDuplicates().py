# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example21").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush','male',2000),(2,'Anand','male',3000),(3,'Aayushi','female',3500),(2,'Anand','male',3000)]
sc=['id','name','gender','salary']
df=spark.createDataFrame(data,sc)
df.show()

# COMMAND ----------

# DBTITLE 1,distinct()
df.distinct().show()

# COMMAND ----------

# DBTITLE 1,dropDuplicates()
df.dropDuplicates(['gender']).show()

# COMMAND ----------

df.dropDuplicates(['gender','salary']).show()
