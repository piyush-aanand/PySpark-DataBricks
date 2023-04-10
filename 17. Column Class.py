# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example17").getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import lit
col1=lit('abcd')
print(type(col1))

# COMMAND ----------

data=[(1,'Piyush',80000),(2,'Mishra',75000)]
schema=['id','name','salary']
df=spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Using Column class-lit() function
df.withColumn('newCol',lit('newColValue')).show(truncate=0)

# COMMAND ----------

# DBTITLE 1,Multiple ways to access columns from dataframe
#Method-1
df.select(df.name).show()

# COMMAND ----------

#Method-2
df.select(df['name']).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('name')).show()

# COMMAND ----------

# DBTITLE 1,For StructType data
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data=[(1,'Piyush',80000,('Brown','black')),(2,'Mishra',75000,('black','blue'))]
propType=StructType([StructField('hair',StringType()),
                     StructField('eyes',StringType())])
schema=StructType([StructField('id',IntegerType()),
                   StructField('name',StringType()),
                   StructField('salary',IntegerType()),
                   StructField('prop',propType)])
df1=spark.createDataFrame(data,schema)
df1.show(truncate=0)
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,Accessing columns from StructType data
df1.select(df1.prop.hair).show()

# COMMAND ----------

from pyspark.sql.functions import col
df1.select(col('prop.eyes')).show()

# COMMAND ----------


