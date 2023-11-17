# Databricks notebook source
users=[(1,'a',30),(2,'b',32)]

# COMMAND ----------

sampledf=spark.createDataFrame(users)

# COMMAND ----------

sampledf.show()

# COMMAND ----------

display(sampledf)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

users_schema_str= " id int, name string, age int"
df1=spark.createDataFrame(users,users_schema_str)
df1.display()

# COMMAND ----------


