-- Databricks notebook source
select * from samples;

-- COMMAND ----------

use iotdata

-- COMMAND ----------

select * from sample;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### standard view

-- COMMAND ----------

create view tempabove25 as (select * from sample where temperature >25)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Temporaray view

-- COMMAND ----------

create temp view tempabove25 as (select * from sample where temperature >25)

-- COMMAND ----------

show views

-- COMMAND ----------

create  global temp view tempabove25 as (select * from sample where temperature >25)

-- COMMAND ----------

show views

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/asadlsad/processeddata/raw/Baby_Names.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("namestemp")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceGlobalTempView("namesglobal")

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from global_temp.namesglobal
