-- Databricks notebook source
use project;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df=spark.read.table("bronze")
-- MAGIC df_final=df.withColumn("products",explode("products"))\
-- MAGIC .withColumn("price",col("products.price"))\
-- MAGIC .withColumn("product_id",col("products.product_id"))\
-- MAGIC .withColumn("product_name",col("products.product_name"))\
-- MAGIC .withColumn("quantity",col("products.quantity"))\
-- MAGIC .withColumn("timestamp",col("timestamp").cast("timestamp"))\
-- MAGIC .drop("products")
-- MAGIC df_final.write.mode("overwrite").saveAsTable("project.silver")

-- COMMAND ----------

select * from silver;

-- COMMAND ----------


