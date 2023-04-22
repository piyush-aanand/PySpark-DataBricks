# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example42").getOrCreate()

# COMMAND ----------

data=['Piyush Anand','Manish Kumar Sharma']
rdd=spark.sparkContext.parallelize(data)
for item in rdd.collect():
    print(item)

# COMMAND ----------

# DBTITLE 1,Using map()
rdd1=rdd.map(lambda x: x.split(' '))
for item in rdd1.collect():
    print(item)

# COMMAND ----------

# DBTITLE 1,Using flatMap()
rdd2=rdd.flatMap(lambda x: x.split(' '))
for item in rdd2.collect():
    print(item.upper())

# COMMAND ----------


