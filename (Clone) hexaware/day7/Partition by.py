# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/asadlsad/processeddata/raw/Baby_Names.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("Year").count().orderBy("Year").show()

# COMMAND ----------


