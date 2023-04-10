# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example20").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush','male',50000),(2,'Anand','male',40000),(3,'Aayushi','female',30000)]
sc=['id','name','gender','salary']
df=spark.createDataFrame(data,sc)
df.show()

# COMMAND ----------

# DBTITLE 1,filter()
#Method-1
df.filter(df.gender=='female').show()
#Method-2 (passing condition like sql statement)
df.filter("gender=='male'").show()

# COMMAND ----------

# DBTITLE 1,where()
#Method-1
df.where("gender=='male' and salary==40000").show()
#Method-2
df.where((df.gender=='male')&(df.salary==40000)).show()
