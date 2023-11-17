# Databricks notebook source
# MAGIC %sql
# MAGIC upsert:(update and insert)

# COMMAND ----------


employee = [(1, "Scott", "Tiger", 1000.0,
                      "united states"
                     )]
df = spark. \
    createDataFrame(employee,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employee
# MAGIC (
# MAGIC   employee_id INT, 
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC    salary FLOAT,
# MAGIC     nationality STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employee as target
# MAGIC USING source_view as source
# MAGIC ON target.employee_id = source.employee_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC    target.first_name=source.first_name,
# MAGIC    target.last_name=source.last_name,
# MAGIC    target.salary=source.salary,
# MAGIC   target.nationality=source.nationality
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee;

# COMMAND ----------

employee = [(1, "Scott", "Tiger", 1000.0,
                      "India"
                     ),
             (2,"John","Clair",2000.0,"UK")]
df = spark. \
    createDataFrame(employee,
employees = [(1, "Scott", "Tiger", 1000.0,
                      "India"
                     ),
             (2,"John","Clair",2000.0,"UK")]
df = spark. \
    createDataFrame(employee,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
df.createOrReplaceTempView("source_view")

# COMMAND ----------

employee = [(1, "Scott", "Tiger", 1000.0,
                      "India"
                     ),
             (2,"John","Clair",2000.0,"UK")]
df = spark. \
    createDataFrame(employee,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view;

# COMMAND ----------


