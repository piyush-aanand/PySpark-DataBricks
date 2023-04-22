# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example36").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush',['AWS','.Net']),(2,'Anand',['Azure','Java'])]
schema=['id','name','skills']
df=spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,#Method-1
from pyspark.sql.functions import transform,upper
df.select('id','name',transform('skills',lambda x:upper(x)).alias("skills")).show()

# COMMAND ----------

# DBTITLE 1,#Method-2
def convertToUpper(x):
    return upper(x)
df.select('id','name',transform('skills',convertToUpper).alias("Skills")).show()
