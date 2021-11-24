# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse hands-on lab using Delta Lake
# MAGIC 
# MAGIC Credit to [Databricks Acacdemy Repos](https://github.com/orgs/databricks-academy/repositories)
# MAGIC 
# MAGIC The files you will be using for this labs are located at: 
# MAGIC - https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_1.json
# MAGIC - https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2.json
# MAGIC - https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_3.json

# COMMAND ----------

# MAGIC %md
# MAGIC Download the files to the driver node's disk. 
# MAGIC 
# MAGIC We use the `%sh` command to execute shell commands directly in the notebook. 

# COMMAND ----------

username = "jneo"
spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse_{username}")
spark.sql(f"USE lakehouse_{username}")
health_tracker = f"/lakehouse/{username}/healthtracker/"

# COMMAND ----------

dbutils.fs.rm(f"{health_tracker}/bronze", True)
dbutils.fs.rm(f"{health_tracker}/silver", True)
dbutils.fs.rm(f"{health_tracker}/gold", True)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_bronze
"""
)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_silver
"""
)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_gold
"""
)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_1.json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We run `%sh ls` to verify the files have been downloaded. 

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %md
# MAGIC Set up: 
# MAGIC - Variables used to store the file path
# MAGIC - "database"

# COMMAND ----------

# MAGIC %md
# MAGIC Move files from driver node to the raw directory 

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_1.json", 
              health_tracker + "raw/health_tracker_data_2020_1.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC Load the data into a Spark DataFrame

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_1.json"
 
health_tracker_data_2020_1_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Display the data using `display()` and visualise the data with the following visualisation configuration: 
# MAGIC 
# MAGIC ```
# MAGIC Keys: time
# MAGIC Series groupings: device_id
# MAGIC Values: heartrate
# MAGIC Aggregation: SUM
# MAGIC Display Type: Line chart
# MAGIC ```

# COMMAND ----------

display(health_tracker_data_2020_1_df)
