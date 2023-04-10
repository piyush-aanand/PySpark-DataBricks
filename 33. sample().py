# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example33").getOrCreate()

# COMMAND ----------

# DBTITLE 1,spark.range() - To generate series
df=spark.range(start=0,end=1000,step=10)
display(df)

# COMMAND ----------

# DBTITLE 1,sample(fraction= , seed= )- Generates fixed set of random numbers from a dataset
df1=df.sample(fraction=0.1,seed=12)
display(df1)

df2=df.sample(fraction=0.1,seed=12)
display(df2)
