# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example31").getOrCreate()

# COMMAND ----------

data=[('IT',8,5),('Payroll',3,2),('HR',2,4)]
schema=['dept','male','female']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,unpivot using stack()
from pyspark.sql.functions import expr
unpivotDF=df.select('dept',expr("stack(2,'M',male,'F',female) as (gender,count)"))
unpivotDF.show()
