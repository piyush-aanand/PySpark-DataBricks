# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example10").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush Anand','50000'),(2,'Nishant Mishra','70000')]
col=['Id','Name','Salary']
df=spark.createDataFrame(data=data,schema=col)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Changing datatype of column
from pyspark.sql.functions import col
df1=df.withColumn(colName='Salary',col=col('Salary').cast('Integer'))
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,Arithmetic operation on a column
df2=df1.withColumn('Salary',col('Salary')*2)
df2.show()

# COMMAND ----------

# DBTITLE 1,Adding a new column to dataframe
from pyspark.sql.functions import lit
df3=df2.withColumn('Country',lit('India')) # lit() is used to add same values in entire column rows
df3.show()

# COMMAND ----------

# DBTITLE 1,withColumnRename() use case
df3.withColumnRenamed('Name','Full_Name').show()
