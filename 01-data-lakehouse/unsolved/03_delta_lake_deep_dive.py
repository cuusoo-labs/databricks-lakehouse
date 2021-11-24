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
# MAGIC Add additional metadata comments to the table columns to provide more description

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Lets look at the table metadata 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Display the delta lake file

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC Display the `_delta_log`

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Take a peek at the `_delta_log` json files

# COMMAND ----------

# the first operation performed


# COMMAND ----------

# metadata about the table size


# COMMAND ----------

# the second operation performed


# COMMAND ----------

# metadata about the table size


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Make a modification to the delta table 

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC Read the delta table 

# COMMAND ----------



# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's take a look at the delta table again 
# MAGIC 
# MAGIC We notice two files now, instead of 1. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see what changes have been made to the `_delta_log` folder
# MAGIC 
# MAGIC We should see `00000000000000000002.json` added. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's take a look at the newest transaction log and see what has changed. 
# MAGIC 
# MAGIC What's changed: 
# MAGIC - A new file reference has been added to the delta table 
# MAGIC - The previous file reference has been removed 
# MAGIC 
# MAGIC But **NOTE**, the physical files have not been removed. Only the references. 

# COMMAND ----------

# see what operation has been performed 


# COMMAND ----------

# see the new table stats


# COMMAND ----------

# what is was before 


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC View history in a more readable format

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Time travel 
# MAGIC 
# MAGIC Since the parquet files have not been removed, but we have captured the transaction in the `_delta_log` table, we are able to perform time travel. 
# MAGIC 
# MAGIC Docs: https://docs.delta.io/latest/quick-start.html#read-older-versions-of-data-using-time-travel

# COMMAND ----------


# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's perform a couple more operations on our table 

# COMMAND ----------

from pyspark.sql.functions import col
import random

health_tracker_data_2020_1_df = spark.read.format("delta").load(f"{health_tracker}/bronze/")

for i in range(0,10): 
  df_update = health_tracker_data_2020_1_df.filter(col("device_id").isin([random.randint(1,5)]))
  df_update.write.format("delta").mode("overwrite").save(f"{health_tracker}/bronze")

# COMMAND ----------

# we now see a whole bunch of parquet files, one per change 


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Notice a new addition of the `00000000000000000010.checkpoint.parquet` file. 

# COMMAND ----------

# what is interesting is a new file has appeared - checkpoint.parquet


# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake creates a checkpoint file in Parquet format on every 10th commit. 
# MAGIC 
# MAGIC The checkpoint files save the **entire** state of the table at a point in time – in native Parquet format that is quick and easy for Spark to read. 
# MAGIC 
# MAGIC This provides the Spark reader with a "shortcut” to fully reproducing a table’s state without reprocessing what could be thousands of tiny, inefficient JSON files. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Oh no! I made a mistake, how do I revert back to a previous copy? 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- restore version 1 of metastore defined Delta table
# MAGIC RESTORE TABLE health_tracker_bronze TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We're now back to the original dataset

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We've now produced a lot of files in order to maintain history and time travel capabilities. 
# MAGIC 
# MAGIC These files are only going to keep on increasing and increasing in quantity. 
# MAGIC 
# MAGIC How do we clean up these files if they exceed a certain time window? 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To perform clean-ups, we need to perform it in two places: 
# MAGIC 
# MAGIC 1. Log files - files that are in the `_delta_log` folder 
# MAGIC 2. Data files - parquet data files that are at the root of the delta table folder 
# MAGIC 
# MAGIC **Log files** 
# MAGIC 
# MAGIC Log files are deleted automatically and asynchronously after checkpoint operations. The default retention period of log files is 30 days, configurable through the `delta.logRetentionDuration` property which you set with the ALTER TABLE SET TBLPROPERTIES SQL method. If you set this config to a large enough value, many log entries are retained. This should not impact performance as operations against the log are constant time. Operations on history are parallel but will become more expensive as the log size increases. See [table property overrides](https://docs.databricks.com/delta/delta-utility.html#table-property-overrides). 
# MAGIC 
# MAGIC **Data files** 
# MAGIC 
# MAGIC - `delta.deletedFileRetentionDuration = "interval <interval>"`: controls how long ago a file must have been deleted before being a candidate for VACUUM. The default is interval 7 days.
# MAGIC - `deltaTable.vacuum()`: vacuum files not required by versions older than the `deletedFileRetentionDuration` period

# COMMAND ----------



# COMMAND ----------


# COMMAND ----------

# no change in number of parquet files since we haven't exceeded the deletedFileRetentionDuration yet. 


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Warning** 
# MAGIC 
# MAGIC We do not recommend that you set a retention interval shorter than 7 days, because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table. If vacuum cleans up active files, concurrent readers can fail or, worse, tables can be corrupted when vacuum deletes files that have not yet been committed.
# MAGIC 
# MAGIC Delta Lake has a safety check to prevent you from running a dangerous vacuum command. If you are certain that there are no operations being performed on this table that take longer than the retention interval you plan to specify, you can turn off this safety check by setting the Apache Spark configuration property spark.databricks.delta.retentionDurationCheck.enabled to false. You must choose an interval that is longer than the longest running concurrent transaction and the longest period that any stream can lag behind the most recent update to the table.
