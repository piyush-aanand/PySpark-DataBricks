# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example15").getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,MapType

data=[('Piyush',{'hair':'black','eyes':'brown'}),('Anand',{'hair':'yellow','eyes':'blue'})]
schema=StructType([StructField('name',StringType()),
                   StructField('properties',MapType(StringType(),StringType()))])
df1=spark.createDataFrame(data,schema)
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,explode()
from pyspark.sql.functions import explode
df1.select('name','properties',explode(df1.properties)).show(truncate=0)

# COMMAND ----------

# DBTITLE 1,map_keys()
from pyspark.sql.functions import map_keys
df1.withColumn('keys',map_keys(df1.properties)).show(truncate=0)

# COMMAND ----------

# DBTITLE 1,map_values()
from pyspark.sql.functions import map_values
df1.withColumn('values',map_values(df1.properties)).show(truncate=0)
