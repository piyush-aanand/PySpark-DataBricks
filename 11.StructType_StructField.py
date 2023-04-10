# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example11").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Example-1 (Simple Schema)
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

data=[(1,'Piyush',1000),(2,'Anand',3000)]
schema=StructType([StructField(name='id',dataType=IntegerType()),\
                   StructField(name='name',dataType=StringType()),
                   StructField(name='salary',dataType=IntegerType())])
df=spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Example-2 (NestedSchema)
data=[(1,('Piyush','Anand'),1000),(2,('Nishant','Mishra'),3000)]

structName=StructType([StructField('FirstName',StringType()),\
                       StructField('LastName',StringType())])
schema=StructType([StructField(name='id',dataType=IntegerType()),\
                   StructField(name='name',dataType=structName),
                   StructField(name='salary',dataType=IntegerType())])
df=spark.createDataFrame(data,schema)
df.show()
df.printSchema()
