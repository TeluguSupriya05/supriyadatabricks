-- Databricks notebook source
delta
parquet  files
+
delta logs 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS tdelta.people20m (
-- MAGIC   id INT,
-- MAGIC   firstName STRING,
-- MAGIC   middleName STRING,
-- MAGIC   lastName STRING,
-- MAGIC   gender STRING,
-- MAGIC   birthDate TIMESTAMP,
-- MAGIC   ssn STRING,
-- MAGIC   salary INT
-- MAGIC ) LOCATION 'dbfs:/mnt/tsadlshexaware/processeddata/delta/supriya'

-- COMMAND ----------

insert into tdelta.people20m values(1,'Virat','R','K','M',2023-11-14,"123",1500)

-- COMMAND ----------

insert into tdelta.people20m values(2,'samrat','R','K','M',2023-11-14,"123",1500)

-- COMMAND ----------

select * from tdelta.people20m;

-- COMMAND ----------

 create table  tdelta.peoplev3 as (select * from tdelta.people20m);

-- COMMAND ----------


select * from tdelta.people20m timestamp as of '2023-11-14T05:58:40Z'



-- COMMAND ----------

select * from tdelta.people20m version as of 2

-- COMMAND ----------

2
