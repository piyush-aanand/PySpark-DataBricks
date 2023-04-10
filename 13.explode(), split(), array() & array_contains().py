# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Example13").getOrCreate()

# COMMAND ----------

data=[(1,'Piyush',['dotNet','Azure']),(2,'Anand',['Java','AWS'])]
sc=['id','name','skills']
df=spark.createDataFrame(data,sc)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,explode()
from pyspark.sql.functions import explode,col
df1=df.withColumn('Skill',explode(col('skills')))
df1.show()

# COMMAND ----------

# DBTITLE 1,split()
from pyspark.sql.functions import split
data=[(1,'Piyush','.Net,Azure,SQL'),(2,'Anand','Java,AWS,Oracle')]
sc=['id','name','skills']
df2=spark.createDataFrame(data,sc)
df2.show()
df2.withColumn('SkillSet',split(col('skills'),',')).show()

# COMMAND ----------

# DBTITLE 1,array()
from pyspark.sql.functions import array
data=[(1,'Piyush','.Net','Azure','SQL'),(2,'Anand','Java','AWS','Oracle')]
sc=['id','name','f_skill','s_skill','t_skill']
df3=spark.createDataFrame(data,sc)
df3.show()
df3.withColumn('skillArray',array(col('f_skill'),col('s_skill'),col('t_skill'))).show()

# COMMAND ----------

# DBTITLE 1,array_contains()
from pyspark.sql.functions import array_contains
data=[(1,'Piyush',['dotNet','Azure']),(2,'Anand',['Java','AWS'])]
sc=['id','name','skills']
df4=spark.createDataFrame(data,sc)
df4.withColumn('presentOrNot',array_contains(col('skills'),'AWS')).show()
