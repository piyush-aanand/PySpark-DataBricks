# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example12").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Without ArrayType
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

data=[('abc',[1,2]),('def',[3,4]),('ghi',[5,6])]
schema=['id','numbers']
df=spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,With ArrayType
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

data=[('abc',[1,2]),('def',[3,4]),('ghi',[5,6])]
schema=StructType([StructField('id',StringType()),StructField('numbers',ArrayType(IntegerType()))])
df1=spark.createDataFrame(data,schema)
df1.show()
df1.printSchema()
display(df1)

# COMMAND ----------

# DBTITLE 1,Accessing array elements separately using indexing
df1.withColumn('firstNumber',df1.numbers[0]).show()

# COMMAND ----------

# DBTITLE 1,Combine columns to form Array
from pyspark.sql.functions import col,array

df2=spark.createDataFrame([(22,33),(66,44)],['num1','num2'])
df2.show()
df2.printSchema()

# COMMAND ----------

df3=df2.withColumn('combined_num',array(col('num1'),col('num2')))
df3.show()
df3.printSchema()
