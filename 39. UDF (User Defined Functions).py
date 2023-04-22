# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example39").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush',35000,5000),(2,'Anand',40000,4600)]
schema=['id','name','salary','bonus']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,UDF (Method-1)
def totalPay(s,b):
    return s+b

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
TotalPayment=udf (lambda s,b:totalPay(s,b),IntegerType())
df.withColumn('totPay',TotalPayment(df.salary,df.bonus)).show()

# COMMAND ----------

# DBTITLE 1,Method-2 (Using Annotation)
@udf(returnType=IntegerType())
def totalPay(s,b):
    return s+b

df.select('*',totalPay(df.salary,df.bonus).alias('Total_Pay')).show()

# COMMAND ----------

# DBTITLE 1,Method-3: Register udf for using in SQL queries
df.createOrReplaceTempView('emps')
def totPay(s,b):
    return s+b

spark.udf.register(name='TotalPay',f=totPay,returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,TotalPay(salary,bonus) as totPay from emps
