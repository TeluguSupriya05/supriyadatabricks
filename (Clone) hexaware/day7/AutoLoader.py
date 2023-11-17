# Databricks notebook source
inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"
outputpath="dbfs:/mnt/asadlsad/processeddata/outputautoloader"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/supriya/schemalocation")
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/supriya/checkpoint")
.option("path",f"{outputpath}/supriya/files")
.table("tsstream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from tsstream.firstauto

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table tsstream.firstauto;

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/supriya/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/supriya/checkpoint")
.option("path",f"{outputpath}/supriya/files")
.table("tsstream.firstauto")
)

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/supriya/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/supriya/checkpoint")
.option("path",f"{outputpath}/supriya/files")
.option("mergeSchema",True)
.table("tsstream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tsstream.firstauto;

# COMMAND ----------


