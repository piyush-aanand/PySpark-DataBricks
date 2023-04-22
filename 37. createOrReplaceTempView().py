# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example37").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush',35000),(2,'Anand',40000)]
schema=['id','name','salary']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,createOrReplaceTempView()
# Method-1
df.createOrReplaceTempView('employees')
df1=spark.sql("select * from employees")
df1.show()

# COMMAND ----------

# DBTITLE 1,Method-2
# MAGIC %sql
# MAGIC SELECT id, upper(name) as Name from employees
