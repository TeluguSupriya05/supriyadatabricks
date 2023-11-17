# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/circuts.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1/circuits.csv

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### load to table

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create schema formula1;
# MAGIC use formula1

# COMMAND ----------

df.write.saveAsTable("formula1.circuits")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.circuits

# COMMAND ----------


