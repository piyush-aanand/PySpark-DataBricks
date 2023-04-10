# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example27").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush','Male',30000)]
schema=['id','name','gender','salary']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,select() ways
# First
df.select('id','name','salary').show()

# COMMAND ----------

#Second
df.select(df.id,df.salary).show()

# COMMAND ----------

#Third
df.select(['id','name','salary']).show()

# COMMAND ----------

#Fourth-(To see all the columns and rows)
df.select('*').show()

# COMMAND ----------

#Fifth-(Columns fetched dynamically)
df.select([col for col in df.columns]).show()
