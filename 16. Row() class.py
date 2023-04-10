# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example16").getOrCreate()

# COMMAND ----------

#Method-1
from pyspark.sql import Row
row=Row('Piyush Anand',30000)
print('Name is '+row[0]+' and salary is '+str(row[1]))

# COMMAND ----------

#Method-2
row=Row(name='Piyush Anand',salary=30000)
print('Name is '+row.name+' and salary is '+str(row.salary))

# COMMAND ----------

# DBTITLE 1,Creating DataFrame from available Row
row1=Row(name='Piyush',salary=50000)
row2=Row(name='Mishra',salary=40000)
data=[row1,row2]
df=spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Creating row like class
Person=Row('name','salary')
p1=Person('Piyush',50000)
p2=Person('Anand',85000)
print(p1.name+" "+p2.name)

# COMMAND ----------

# DBTITLE 1,Nested Row()
data=[Row(name='Piyush',properties=Row(age=25,gender='Male')),
      Row(name='Shalini',properties=Row(age=23,gender='Female'))]
df1=spark.createDataFrame(data)
df1.show()
df1.printSchema()
