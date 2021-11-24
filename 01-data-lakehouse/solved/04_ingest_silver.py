# Databricks notebook source
# MAGIC %md
# MAGIC Config

# COMMAND ----------

username = "your_name_here"
spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse_{username}")
spark.sql(f"USE lakehouse_{username}")
health_tracker = f"/lakehouse/{username}/healthtracker"

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Run this cleanup only if needed 

# COMMAND ----------

# clean up if needed 
dbutils.fs.rm(f"{health_tracker}/silver", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_silver
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Read the bronze dataset

# COMMAND ----------

health_tracker_bronze_df = spark.read.format("delta").load(f"{health_tracker}/bronze")
display(health_tracker_bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform the cleaning/filtering/enriching to generate the silver dataset

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime

health_tracker_silver_df = health_tracker_bronze_df.select(
  from_unixtime("time").cast("date").alias("dte"),
  from_unixtime("time").cast("timestamp").alias("time"),
  "heartrate",
  "name",
  col("device_id").cast("integer").alias("p_device_id"),
)

display(health_tracker_silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the silver dataset and add to metadata store 

# COMMAND ----------

(
    health_tracker_silver_df.write.mode("overwrite")
    .format("delta")
    .partitionBy("p_device_id")
    .save(f"{health_tracker}/silver")
)

spark.sql(
  f"""
    CREATE TABLE IF NOT EXISTS health_tracker_silver
    USING DELTA
    LOCATION "{health_tracker}/silver"
  """
)
