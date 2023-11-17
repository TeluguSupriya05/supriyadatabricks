# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.<tsadlshexaware>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<tsadlshexaware>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<tsadlshexaware>.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=c&sp=rlyx&se=2023-11-10T17:06:54Z&st=2023-11-10T09:06:54Z&spr=https&sig=1aZFNlr2uy19z352eP4tYqACrKq7EVDdn5JYftkZPvs%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://processeddata@tsadlshexaware.dfs.core.windows.net/processeddata/")

# COMMAND ----------


