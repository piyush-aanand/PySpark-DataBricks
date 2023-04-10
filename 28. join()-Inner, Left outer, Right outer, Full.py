# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example28").getOrCreate()

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

# DBTITLE 1,inner join
empDf.join(depDf,empDf.dept==depDf.dept,'inner').show()

# COMMAND ----------

# DBTITLE 1,Left outer
empDf.join(depDf,empDf.dept==depDf.dept,'left').show()

# COMMAND ----------

# DBTITLE 1,Right outer
empDf.join(depDf,empDf.dept==depDf.dept,'right').show()

# COMMAND ----------

# DBTITLE 1,Full join
empDf.join(depDf,empDf.dept==depDf.dept,'full').show()
