# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Team Player Dims
# MAGIC ### - Reads team and player (roster) files and wrties reformated data to a staging area 
# MAGIC ### - The output wil be merged with full history files of both teams and players

# COMMAND ----------

# Imports
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import input_file_name, current_timestamp, regexp_extract, col, udf
import boto3
import re
import sys
import logging
import argparse
#import yaml
import os
import glob

# COMMAND ----------

# MAGIC %md
# MAGIC run config files notebook to "import" reusable functions related to configuration files

# COMMAND ----------

dbutils.widgets.text("config_file_location", "default")
config_file_widget = dbutils.widgets.get("config_file_location")
 

# COMMAND ----------

# MAGIC %run ./local-lib/config-files

# COMMAND ----------

# MAGIC %run ./local-lib/s3-search

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function: translate_league_code

# COMMAND ----------

def translate_league_code(arg_league_code):
    
    league_description = arg_league_code
    if arg_league_code == 'A':
        league_description = 'American'
    elif arg_league_code == 'N':
        league_description = 'National'
        
    return league_description
  
udf_translate_league_code = udf(translate_league_code, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function: get_input_roster_dataframe

# COMMAND ----------

def get_input_roster_dataframe(arg_bucket, arg_search_prefix, arg_search_pattern, arg_spark):
    
    files_to_read = get_files_to_read(arg_bucket, arg_search_prefix, arg_search_pattern)   
    logging.info('bucket: {}'.format(arg_bucket))  
    logging.info('search pattern: {}'.format(arg_search_pattern))  
    logging.info('num files to read: {}'.format(len(files_to_read)))
    fields = [StructField('player_id', StringType(), nullable=False),
              StructField('last_name', StringType(), nullable=False),
              StructField('first_name', StringType(), nullable=False),
              StructField('bats', StringType(), nullable=True),
              StructField('throws', StringType(), nullable=True),
              StructField('team_id', StringType(), nullable=True),
              StructField('position', StringType(), nullable=True)]
    input_schema = StructType(fields)

    df = (arg_spark                              # Our SparkSession & Entry Point
    .read                                          # Our DataFrameReader
    .csv(files_to_read, schema=input_schema) # Returns an instance of DataFrame 
            .withColumn("date_ingested", current_timestamp())
            .withColumn("input_file_name", input_file_name())
        .cache()                                   # Cache the DataFrame
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function: get_input_team_dataframe

# COMMAND ----------

def get_input_team_dataframe(arg_bucket, arg_search_prefix, arg_search_pattern, arg_spark):
    
    files_to_read = get_files_to_read(arg_bucket, arg_search_prefix, arg_search_pattern)   

    fields = [StructField('team_id', StringType(), nullable=False),
            StructField('league_id', StringType(), nullable=False),
            StructField('team_city', StringType(), nullable=False),
            StructField('team_name', StringType(), nullable=True),]
    input_schema = StructType(fields)

    df = (arg_spark                              # Our SparkSession & Entry Point
    .read                                          # Our DataFrameReader
    .csv(files_to_read, schema=input_schema) # Returns an instance of DataFrame 
            .withColumn("date_ingested", current_timestamp())
            .withColumn("input_file_name", input_file_name())
        .cache()                                   # Cache the DataFrame
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function: main

# COMMAND ----------


def main(arg_config):
    start_time = datetime.now()

    logging.info('Transform Team Player Dimensions started @ {}'.format(start_time))
    ## DB Change .. Spark Session is created by notebook
    #spark = create_spark_session()

    # Identify root directory for input and output
    input_bucket = arg_config.get('bucketName')
    prefix_filter = arg_config['prefix_filter']
    outputDir = arg_config['stageDir']
    if outputDir.startswith('s3'):
        outputDir = outputDir.format(input_bucket)
    search_year = '\d\d\d\d'

    logging.info('input_bucket: {}'.format(input_bucket))


    ###########################################################################
    #  Read Input files
    ###########################################################################
    # get roster dataframe
    search_pattern = '.*{}\.ROS'.format(search_year)
    df = get_input_roster_dataframe(input_bucket, prefix_filter, search_pattern, spark)

    # get team dataframe
    search_pattern = '.*TEAM{}'.format(search_year)
    team_df = get_input_team_dataframe(input_bucket, prefix_filter, search_pattern, spark)

    ###########################################################################
    # Perform Transformations
    ###########################################################################

    season_pattern = '.*([\d]{4})\.ROS'
    raw_player_dim_df = \
    df.withColumn('roster_year', regexp_extract(col("input_file_name"), season_pattern, 1))
 
    season_pattern = 'TEAM([\d]{4})'
    raw_team_dim_df = \
    team_df.withColumn('season', regexp_extract(col("input_file_name"), season_pattern, 1))

    raw_team_dim_df = raw_team_dim_df.withColumn('league_description', 
                                                udf_translate_league_code(raw_team_dim_df.league_id))

    ###########################################################################
    # Write To Parquet tables
    ###########################################################################
    target_table = 'raw_player_dim'
    if arg_config.get('output_format') == 'json':
        raw_player_dim_df.write.mode('overwrite').json(outputDir+'/'+ target_table)
    else:
        raw_player_dim_df.write.mode('overwrite').parquet(outputDir+'/'+ target_table)


    target_table = 'raw_team_dim'
    if arg_config.get('output_format') == 'json':
        raw_team_dim_df.write.mode('overwrite').json(outputDir+'/'+ target_table)
    else:
        raw_team_dim_df.write.mode('overwrite').parquet(outputDir+'/'+ target_table)

    logging.info('*'*80)
    logging.info('*')
    logging.info('*                   Processing Complete @ {}'.format(datetime.now()))
    logging.info('*')
    logging.info('*  Size of input dataframes:')
    logging.info('*     rosters:  {} x {}'.format(df.count(), len(df.columns)))
    logging.info('*     teams:    {} x {}'.format(team_df.count(), len(team_df.columns)))
    logging.info('*  Size of output dataframes:')
    logging.info('*     raw player_dim: {} x {}'.format(raw_player_dim_df.count(), len(raw_player_dim_df.columns)))
    logging.info('*     raw team dim:   {} x {}'.format(raw_team_dim_df.count(), len(raw_team_dim_df.columns)))
    logging.info('*')
    logging.info('*  Total Execution Time: {}'.format(datetime.now() - start_time))
    logging.info('*')
    logging.info('*'*80)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Script Entry Point

# COMMAND ----------


configfile = get_config_file(config_file_widget)
#spark = create_spark_session()
#log4jLogger = spark._jvm.org.apache.log4j
#logger = log4jLogger.LogManager.getLogger(__name__)
#FORMAT = '%(asctime)-15s %(message)s'
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#logging.basicConfig(format=FORMAT, level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
root = logging.getLogger()
#root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(FORMAT)
handler.setFormatter(formatter)
root.handlers = []
root.addHandler(handler)
main(configfile)