# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example45").getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import row_number, rank, dense_rank
from pyspark.sql.window import Window
data=[('Piyush','IT',40000),\
      ('Anand','IT',45000),\
      ('Asi','HR',30000),\
      ('Annu','Payroll',35000),\
      ('Shakti','IT',25000),\
      ('Pradeep','IT',28000),\
      ('Kranti','Payroll',29850),\
      ('Himanshu','IT',78000),\
      ('Bhargava','HR',60000),\
      ('Shalini','HR',27000)]
schema=['name','dept','salary']
df=spark.createDataFrame(data,schema)
df.sort('dept').show()

# COMMAND ----------

# DBTITLE 1,Window.partitionBy() and row_number()
window=Window.partitionBy('dept').orderBy('salary')
df.withColumn('RowNumbering',row_number().over(window)).show()

# COMMAND ----------

# DBTITLE 1,rank()
data1=[('Piyush','IT',4000),\
      ('Anand','IT',3000),\
      ('Asi','HR',1500),\
      ('Annu','Payroll',2000),\
      ('Shakti','IT',3000),\
      ('Pradeep','IT',2500),\
      ('Kranti','Payroll',3500),\
      ('Himanshu','IT',2000),\
      ('Bhargava','HR',2000),\
      ('Shalini','HR',2000)]
schema1=['name','dept','salary']
df1=spark.createDataFrame(data1,schema1)
#df1.sort('dept').show()
df1.withColumn('RowNumbering',row_number().over(window)).withColumn('Rank',rank().over(window)).show()

# COMMAND ----------

# DBTITLE 1,dense_rank()
df1.withColumn('RowNumbering',row_number().over(window)).\
    withColumn('Rank',rank().over(window)).\
    withColumn('DenserRank',dense_rank().over(window)).show()
