# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example14").getOrCreate()

# COMMAND ----------

data=[('Piyush',{'hair':'black','eyes':'brown'}),('Anand',{'hair':'yellow','eyes':'blue'})]
sc=['name','properties']
df=spark.createDataFrame(data,sc)
df.show(truncate=0)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,MapType()
from pyspark.sql.types import StructType,StructField,StringType,MapType
schema=StructType([StructField('name',StringType()),
                   StructField('properties',MapType(StringType(),StringType()))])
df1=spark.createDataFrame(data,schema)
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,Accessing elements from MapType()
df2=df1.withColumn('hair',df1.properties['hair'])
df2.show(truncate=0)
