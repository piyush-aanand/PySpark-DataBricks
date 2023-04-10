# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example19").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush',20000),(2,'Anand',30000),(3,'Mishra',40000)]
schema=['id','name','salary']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,alias(): Gives alternate name to column headers
df.select(df.id.alias('emp_Id'),df.name.alias('emp_Name'),df.salary.alias('emp_Salary')).show()

# COMMAND ----------

# DBTITLE 1,asc()/ desc(): asc() is by default
df.sort(df.salary.desc()).show()

# COMMAND ----------

# DBTITLE 1,cast(): To convert data type of column 
df.printSchema()
df1=df.select(df.salary.cast('int'))
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,like(): To match pattern 
df.filter(df.name.like('A%')).show()
