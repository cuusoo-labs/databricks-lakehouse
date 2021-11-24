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
dbutils.fs.rm(f"{health_tracker}/gold", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_gold
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Read the silver dataset

# COMMAND ----------

health_tracker_silver_df = spark.read.format("delta").load(f"{health_tracker}/silver")
display(health_tracker_silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform the cleaning/filtering/enriching to generate the silver dataset

# COMMAND ----------

from pyspark.sql.functions import col, avg, max, stddev

health_tracker_gold_df = (
  health_tracker_silver_df
  .groupby("p_device_id")
  .agg(avg(col("heartrate")).alias("avg_heartrate"),
       max(col("heartrate")).alias("max_heartrate"),
       stddev(col("heartrate")).alias("stddev_heartrate"))
)

display(health_tracker_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the gold dataset and add to metadata store 

# COMMAND ----------

(
    health_tracker_gold_df.write.mode("overwrite")
    .format("delta")
    .save(f"{health_tracker}/gold")
)

spark.sql(
  f"""
    CREATE TABLE IF NOT EXISTS health_tracker_gold
    USING DELTA
    LOCATION "{health_tracker}/gold"
  """
)
