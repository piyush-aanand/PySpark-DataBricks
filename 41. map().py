# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example41").getOrCreate()

# COMMAND ----------

data=[('Piyush','Anand'),('Nishant','Mishra')]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------

# DBTITLE 1,rdd then map()
rdd1=rdd.map(lambda x: x + (x[0]+' '+x[1],))
print(rdd1.collect())

# COMMAND ----------

# DBTITLE 1,DataFrame to rdd then map()
df=spark.createDataFrame(data,['fn','ln'])
rdd2=df.rdd.map(lambda x:  x + (x[0]+' '+x[1],))
df1=rdd2.toDF(['fn','ln','Fullname'])
df1.show()

# COMMAND ----------

# DBTITLE 1,Using function
def fullName(x):
    return x+(x[0]+' '+x[1],)
df2=spark.createDataFrame(data,['fn','ln'])
rdd3=df.rdd.map(lambda x: fullName(x))
df3=rdd3.toDF(['fn','ln','Fullname'])
df3.show()
