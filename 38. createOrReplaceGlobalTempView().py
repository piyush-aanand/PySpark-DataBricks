# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example38").getOrCreate()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark

# COMMAND ----------

data=[(1,'Piyush',35000),(2,'Anand',40000)]
schema=['id','name','salary']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.createOrReplaceGlobalTempView('employees')

# COMMAND ----------

# DBTITLE 1,global_temp.<tablename>
# MAGIC %sql
# MAGIC select id,name from global_temp.employees

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables("global_temp")

# COMMAND ----------

# DBTITLE 1,Delete global_temp table
spark.catalog.dropGlobalTempView('employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees
