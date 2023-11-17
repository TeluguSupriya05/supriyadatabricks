# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/constructors.json

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df.withcolumn("ingestiondate",current_date()).dsiplay()

# COMMAND ----------


from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("ingestiondate",current_date()).display()

# COMMAND ----------

df.withColumn("file_path",input_file_name()).display()

# COMMAND ----------

df_final=(df.withColumn("ingestiondate",current_date())
.withColumn("path",input_file_name())
.drop("url"))
df_final.write.saveAsTable("formula1.constructors")

# COMMAND ----------

df.write.saveAsTable("formula1.constructors")

# COMMAND ----------


