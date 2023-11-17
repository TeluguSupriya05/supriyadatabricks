# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists iotdata;
# MAGIC use iotdata
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC Create table iotdata.sample as
# MAGIC (select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storageaccountname>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storageaccountname>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storageaccountname>.dfs.core.windows.net", "SAS Token")
