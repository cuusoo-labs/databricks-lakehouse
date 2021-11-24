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


# COMMAND ----------

# MAGIC %md
# MAGIC Perform the cleaning/filtering/enriching to generate the silver dataset

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC Write the gold dataset and add to metadata store 

# COMMAND ----------
