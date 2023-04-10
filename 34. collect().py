# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example34").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush',30000),(2,'Anand',35000),(3,'Nishant',40000)]
schema=['id','name','salary']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,using collect()
listRows=df.collect()
print(listRows)

# COMMAND ----------

#For getting 2nd row entry
print(listRows[1])

# COMMAND ----------

# For getting Anand salary
print(listRows[1][2])
