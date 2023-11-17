-- Databricks notebook source
-- MAGIC %sql
-- MAGIC create schema tdelta;
-- MAGIC use tdelta

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS tdelta.people10m (  id INT,  firstName STRING,  middleName STRING,  lastName STRING,  gender STRING,  birthDate TIMESTAMP,  ssn STRING,  salary INT) USING DELTA

-- COMMAND ----------


describe extended tdelta.people10m

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS tdelta.people20m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) LOCATION 'dbfs:/mnt/tsaldshexaware/processeddata/delta/naval'

-- COMMAND ----------

describe history tdelta.people10m

-- COMMAND ----------


