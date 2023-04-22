# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example44").getOrCreate()

# COMMAND ----------

df=spark.range(2)
from pyspark.sql.functions import date_format,current_date,to_date
df1=df.withColumn('CurrentDate',current_date())
df1.show()

df2=df1.withColumn('CurrentDate',date_format(df1.CurrentDate,'yyyy.MM.dd'))
df2.show()
df2.printSchema()

# COMMAND ----------

df3=df2.withColumn('CurrentDate',to_date(df2.CurrentDate,'yyyy.MM.dd'))
df3.show()
df3.printSchema()
