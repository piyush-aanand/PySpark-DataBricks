# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example40").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Parallelize()
data=[(1,'Piyush'),(2,'Anand'),(3,'Nishant')]
rdd=spark.sparkContext.parallelize(data)
print(type(rdd))
print(rdd.collect())

# COMMAND ----------

# DBTITLE 1,Conversion to DF
df=rdd.toDF(schema=['id','name'])
df.show()
