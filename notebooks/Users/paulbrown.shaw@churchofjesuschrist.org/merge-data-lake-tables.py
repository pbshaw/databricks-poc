# Databricks notebook source
# MAGIC %md
# MAGIC # Merge Data Lake Tables
# MAGIC 
# MAGIC Reads a staging or source tble and applies inserts/ updates (deletes inserts) to a target table based on a comparison of the source and target. Currently supported formats are json and parquet
# MAGIC 
# MAGIC This noteook is a wrapper around the data_lake_merge library where most of the logic is contained

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get fullpath of configration file 
# MAGIC 
# MAGIC This is expected to be passedas job paramter when running notebook as a job

# COMMAND ----------

dbutils.widgets.text("config_file_location", "s3://pbs-databricks-data-lake/scripts/python/resources/merge-game-databricks.yml")
config_file_widget = dbutils.widgets.get("config_file_location")

# COMMAND ----------

# MAGIC %md
# MAGIC ### "import" functions from "local library"

# COMMAND ----------

# MAGIC %run ./local-lib/config-files

# COMMAND ----------

import data_lake_merge

# COMMAND ----------

config = get_config_file(config_file_widget)
print(config)

# COMMAND ----------

data_lake_merge.merge(spark, config)

# COMMAND ----------

