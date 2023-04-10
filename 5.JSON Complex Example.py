# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("ex").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Example-1 (Multi-Line json file)
data_in=['{"name":"Michael","education":[{"Qualification":"BE","year":2010}]},{"name":"Clarke","education":[{"Qualification":"BE","year":2011},{"Qualification":"ME","year":2013}]}']
inter=spark.sparkContext.parallelize(data_in)
s_check=spark.read.json(inter)
s_check.show(truncate=0)

# COMMAND ----------

s_check_crr=spark.read.json("dbfs:/FileStore/tables/example.json",multiLine=True)
display(s_check_crr)

# COMMAND ----------

s_check_crr.show(truncate=0)

# COMMAND ----------

from pyspark.sql.functions import explode
flat=s_check_crr.select('name',explode('education').alias('flat_education'))
flat.show()

# COMMAND ----------

out_df=flat.select('name','flat_education.Qualification','flat_education.year')
out_df.show()

# COMMAND ----------

# DBTITLE 1,Example-2 (Multi-line json file from DBFS)
sample=spark.read.json("dbfs:/FileStore/tables/sample5.json",multiLine=True)
display(sample)

# COMMAND ----------

sample1=sample.select(explode("users").alias("user_info"))
sample1.show(truncate=0)

# COMMAND ----------

sample2=sample1.select("user_info.emailAddress","user_info.firstName","user_info.lastName","user_info.phoneNumber")
sample2.show(truncate=0)

# COMMAND ----------


