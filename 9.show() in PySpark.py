# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example9").getOrCreate()

# COMMAND ----------

data=[(1,'abcdefghijklmnopqrstuvwxyz'),
      (2,'abcdefghijklmnopqrstuvwxyz'),
      (3,'abcdefghijklmnopqrstuvwxyz'),
      (4,'abcdefghijklmnopqrstuvwxyz'),
      (5,'abcdefghijklmnopqrstuvwxyz')]
sc=['Id','Name']
df=spark.createDataFrame(data=data,schema=sc)
df.show()

# COMMAND ----------

# DBTITLE 1,Parameter-1
df.show(n=3) # Here n means number of rows to be displayed

# COMMAND ----------

# DBTITLE 1,Parameter-2
# Truncate=(False or 0) will let the entire column content to be displayed
df.show(truncate=False)
df.show(n=2,truncate=0) 

# COMMAND ----------

# DBTITLE 1,Parameter-3
df.show(n=3,truncate=False,vertical=True) # Vertical=True will show the result in vertical format

# COMMAND ----------


