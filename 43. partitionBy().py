# Databricks notebook source
data=[(1,'Piyush','male','IT'),(2,'Utkarsh','male','HR'),(3,'Supritha','female','IT')]
schema=['id','name','gender','dept']
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,Partitioning details and saving into parquet file
df.write.parquet("/FileStore/Employees",mode="overwrite",partitionBy='dept')

# COMMAND ----------

# DBTITLE 1,Reading from parquet file
spark.read.parquet('/FileStore/Employees').show()

# COMMAND ----------

spark.read.parquet('/FileStore/Employees/dept=IT').show()
