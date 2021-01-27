# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Snowflake Dim
# MAGIC 
# MAGIC 
# MAGIC This noteook is a wrapper around the snowflake_transform library where most of the logic is contained

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get fullpath of configration file 
# MAGIC 
# MAGIC This is expected to be passedas job paramter when running notebook as a job

# COMMAND ----------

dbutils.widgets.text("config_file_location", "s3://pbs-databricks-data-lake/scripts/python/resources/team-dim-transform-databricks.yml")
config_file_widget = dbutils.widgets.get("config_file_location")

# COMMAND ----------

# MAGIC %md
# MAGIC ### "import" functions from "local library"

# COMMAND ----------

# MAGIC %run ./local-lib/config-files

# COMMAND ----------

import data_lake_merge
from py4j.java_gateway import java_import

# COMMAND ----------

config = get_config_file(config_file_widget)
print(config)

# COMMAND ----------

#get_secret(arg_config['SF_URL_SECRET'])['sfUrl']
config['SF_URL_SECRET']

# COMMAND ----------

get_secret(config['SF_URL_SECRET'])['sfUrl']

# COMMAND ----------

arg_config = config
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
Utils = sc._gateway.jvm.Utils
##
sf_options = {
"sfUrl": get_secret(arg_config['SF_URL_SECRET'])['sfUrl'],
"sfUser": get_secret(arg_config['USER_ID_SECRET'])['sfUser'],
"sfPassword": get_secret(arg_config['USER_PW_SECRET'])['sfPassword'],
"sfRole": arg_config['sf_role'],
"sfDatabase": arg_config['sf_db'],
"sfSchema": arg_config['sf_schema'],
"sfWarehouse": arg_config['sf_wh']
}

sf_table_name = arg_config['sf_dim']

current_dim_df = spark.read \
.format("snowflake") \
.options(**sf_options) \
.option("dbtable", sf_table_name) \
.load().cache()

# COMMAND ----------

sf_options

# COMMAND ----------

current_dim_df.show()

# COMMAND ----------

arg_config = config
input_bucket = arg_config['input_bucket']
s3_dim = arg_config['s3_dim']
s3_prefix = arg_config['s3_prefix']
arg_file_format = arg_config['s3_format']
file_path = 's3://{}/{}/{}'.format(input_bucket,s3_prefix,s3_dim)

if arg_file_format == 'json':
  new_dim_df = \
  (spark                              # Our SparkSession & Entry Point
    .read                                          # Our DataFrameReader
    .json(file_path)
        .cache()                                   # Cache the DataFrame
  )  
else:
  new_dim_df = \
  (spark                              # Our SparkSession & Entry Point
    .read                                          # Our DataFrameReader
    .parquet(file_path)
        .cache()                                   # Cache the DataFrame
  )

# COMMAND ----------

new_dim_df.show()


# COMMAND ----------


current_dim_df.createOrReplaceTempView("current_dim")
new_dim_df.createOrReplaceTempView("new_dim")

# COMMAND ----------


surrogate_key = arg_config['surrogate_key']
current_max_key = spark.sql('select coalesce(max({}),0) as max_key from current_dim'.format(surrogate_key)).collect()[0]['max_key']
print(current_max_key)

# COMMAND ----------


staging_sql = arg_config['staging_sql'].format(current_max_key)
staging_df = spark.sql(staging_sql)
staging_df.show()

# COMMAND ----------

## Note Overwirte drops and recreates the table
##      We may want to do an explicit delete then insert in this case
sf_options['sfSchema'] = arg_config['staging_schema']

staging_df.write\
.format(SNOWFLAKE_SOURCE_NAME)\
.options(**sf_options)\
.option("dbtable", arg_config['sf_dim'])\
.mode("Overwrite")\
.save()


# COMMAND ----------

merge_sql = arg_config['merge_sql']
result = Utils.runQuery(sf_options, merge_sql)

# COMMAND ----------

