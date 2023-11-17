# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/inputstream/csv/

# COMMAND ----------

inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"

# COMMAND ----------

from pyspark.sql.types import *
users_sch=StructType([StructField("Id",IntegerType()),
                      StructField("Name",StringType()),
                      StructField("Gender",StringType()),
                      StructField("Salary",IntegerType()),
                      StructField("Country",StringType()),
                      StructField("Date",StringType()),
])

# COMMAND ----------

df=spark.readStream.option("header",True).schema(users_sch).csv(f"{inputpath}")

# COMMAND ----------

display(df)

# COMMAND ----------


from pyspark.sql.functions import *

df1=df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create schema tsstream;

# COMMAND ----------

outputpath="dbfs:/mnt/asadlsad/processeddata/outputstream"

# COMMAND ----------

df1.writeStream.option("path",f"{outputpath}/supriya/teststream/files").option("checkpointLocation",f"{outputpath}/supriya/teststream/checkpoint").toTable("tsstream.teststream")

# COMMAND ----------

df1.writeStream.option("path",f"{outputpath}/supriya/teststream/files").option("checkpointLocation",f"{outputpath}/supriya/teststream/checkpoint").trigger(processingTime="1 minute").toTable("tsstream.teststream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tsstream.teststream;

# COMMAND ----------

for i in spark.streams.active:
    i.stop()

# COMMAND ----------


