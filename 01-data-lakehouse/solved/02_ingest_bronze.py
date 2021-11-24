# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Setup

# COMMAND ----------

username = "your_name_here"
spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse_{username}")
spark.sql(f"USE lakehouse_{username}")
health_tracker = f"/lakehouse/{username}/healthtracker"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Load data into DataFrame

# COMMAND ----------

file_path = health_tracker + "/raw/health_tracker_data_2020_1.json"
health_tracker_data_2020_1_df = spark.read.format("json").load(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Remove `health_tracker_bronze` tables if exist

# COMMAND ----------

dbutils.fs.rm(f"{health_tracker}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_bronze
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Write DataFrame to storage using Delta Lake format 

# COMMAND ----------

(
  health_tracker_data_2020_1_df
  .write
  .mode("overwrite")
  .format("delta")
  .save(f"{health_tracker}/bronze")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Register the table into the metastore. 
# MAGIC 
# MAGIC This creates a pointer to the delta lake files. 

# COMMAND ----------

spark.sql(
f"""
  CREATE TABLE health_tracker_bronze
  USING DELTA
  LOCATION "{health_tracker}/bronze"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Check the table in the metastore by: 
# MAGIC 1. Go to Data 
# MAGIC 2. Select your database name 
# MAGIC 3. Select `health_tracker_bronze`
