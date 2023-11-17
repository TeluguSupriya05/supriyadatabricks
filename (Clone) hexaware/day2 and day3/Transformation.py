# Databricks notebook source

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/formula1/circuits.csv")

df.select("circuitId","circuitRef","name").display()

# COMMAND ----------


from pyspark.sql.functions import col

df.select(col("circuitId").alias("circuit_id")).display()

df.select(col("circuitId"),"circuitRef",df.name,df["location"]).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


df.select(concat("location","country").alias("new column")).display()

df.select("*", concat("location",lit(" "),"country").alias("new column")).display()

# COMMAND ----------

help(withColumnRead)

# COMMAND ----------

help(df.withColumnRead)

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------


df.select(col("circuitId").alias("circuit_id")).display()

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------


df.columns


newcolumns=['circuit_id',
 'circuit_ref',
 'firstname',
 'location',
 'country',
 'lat',
 'lng',
 'alt',
 'url']

# COMMAND ----------


df.toDF(newcolumns)



# COMMAND ----------

df.toDF(*newcolumns).display()

# COMMAND ----------

df.withColumn("NewDate",lit(current_date())).display()

# COMMAND ----------

df.withColumn("ingentiondate",current_timestamp()).display()

# COMMAND ----------


df.where("circuitID=1").display()



# COMMAND ----------

df.filter(col("circuitID")==1).display()

# COMMAND ----------


df.filter("circuitID > 10 and country ='UK'").display()

df.filter((col("circuitID") > 10) & (col("country") =='UK')).display()

# COMMAND ----------

df.sort("country").display()

# COMMAND ----------

df.sort("country","location").select("country")

# COMMAND ----------


df.orderBy(col("circuitID").desc()).display()

df.orderBy(desc("circuitID")).display()


# COMMAND ----------


