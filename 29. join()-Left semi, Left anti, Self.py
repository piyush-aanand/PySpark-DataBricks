# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example29").getOrCreate()

# COMMAND ----------

data1=[(1,'Piyush',30000,4),(2,'Anand',29000,1),(3,'Nishant',40000,2),(4,'ABCD',50000,3)]
schema1=['id','name','salary','dept']

data2=[(1,'HR'),(2,'Payroll'),(4,'IT'),(5,'Manager')]
schema2=['dept','name']

empDf=spark.createDataFrame(data1,schema1)
empDf.show()

depDf=spark.createDataFrame(data2,schema2)
depDf.show()

# COMMAND ----------

# DBTITLE 1,Left-semi
empDf.join(depDf,empDf.dept==depDf.dept,'inner').show()
empDf.join(depDf,empDf.dept==depDf.dept,'leftsemi').show()

# COMMAND ----------

# DBTITLE 1,Left-anti
empDf.join(depDf,empDf.dept==depDf.dept,'leftanti').show()

# COMMAND ----------

# DBTITLE 1,Self
data=[(1,'Piyush',0),(2,'Anand',1),(3,'Sharma',2)]
schema=['id','name','mgrId']

df=spark.createDataFrame(data,schema)
df.show() 

from pyspark.sql.functions import col

df1=df.alias('empData').join(df.alias('mgrData'),\
                    col('empData.mgrId')==col('mgrData.id'),'full')
df1.show()

# COMMAND ----------

df2=df.alias('empData').join(df.alias('mgrData'),\
                    col('empData.mgrId')==col('mgrData.id'),'left')\
                    .select(col('empData.name').alias('empName'),col('mgrData.name').alias('mgrName'))
df2.show()
