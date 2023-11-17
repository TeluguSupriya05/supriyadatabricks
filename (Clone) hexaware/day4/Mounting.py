# Databricks notebook source
# MAGIC %md
# MAGIC ####mount
# MAGIC 1. Notebook authentication
# MAGIC 2. Cluster

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://testblobcontainer@blobadhex.blob.core.windows.net",
  mount_point = "/mnt/blobadhex/testblobcontainer",
  extra_configs = {"fs.azure.account.key.blobadhex.blob.core.windows.net":"q5I8uK1/Z4PL9+1OsIPWik5gHWWliO8M4RO7iVIYcf2uE+sJCXKTNd5tdcPa/U99JDmpkT7Zbphl+AStgc+3LQ=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/blobadhex/testblobcontainer/

# COMMAND ----------

dbutils.fs.unmount("/mnt/blobadhex/testblobcontainer")

# COMMAND ----------

dbutils.fs.unmount("/mnt/asadlsad/processeddata")

# COMMAND ----------


