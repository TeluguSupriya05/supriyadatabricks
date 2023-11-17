# Databricks notebook source
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]
 
schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.filter("department='Sales'").display()

# COMMAND ----------

df.groupBy("department").sum("salary").display()

# COMMAND ----------


from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df.groupBy("department").agg(
    col("department"),
    col("salary").min().alias("min_salary"),
    col("salary").max().alias("max_salary"),
    col("salary").avg().alias("avg_salary"),
    col("salary").count().alias("salary_count")
).show()

# COMMAND ----------

df.groupBy("department").agg(sum("salary"),min("salary"),max("salary")).display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/tsadlshexaware//processeddata/pdata/

# COMMAND ----------

df_drivers=spark.read.json("dbfs:/mnt/tsadlshexaware/processeddata/pdata/drivers.json")
df_pitstop=spark.read.option("multiline",True).json("dbfs:/mnt/tsadlshexaware/processeddata/pdata/pit_stops.json")

# COMMAND ----------

df_drivers.join(df_pitstop).display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId").display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId","left").select("driverId","code","lap").display()

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/asadlsad/processeddata/raw/Nulls.csv",header=True,inferSchema=True)

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

df1.dropna("all",subset="id").display()

# COMMAND ----------

df1.fillna({"id":0,"name":"unknown","Marks":49,"placed":False}).display()

# COMMAND ----------


