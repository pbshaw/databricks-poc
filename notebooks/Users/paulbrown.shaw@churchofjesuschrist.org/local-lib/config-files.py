# Databricks notebook source
import boto3
import yaml
import os
import re

# COMMAND ----------

def parse_source_location(arg_source_location):
    pattern = 's3:\/\/([^\/]*)\/(.*)'
    # this regex expression with split source llocation into two parts:
    #    (1) bucket name - s3://<all characters until the next forward slash
    #    (2) prefix - all characters after the trailin gforward slash following the bucket name
    re_result = re.match(pattern, arg_source_location)
    bucket_name = re_result.group(1)
    prefix = re_result.group(2)

    return bucket_name, prefix

# COMMAND ----------

# Here we create a configuration dictionary ... Need to determine how to manage configurations
def get_config_file(config_location):
    #s3://pbshaw-emr-exploration/scripts/resources/confirmed-cases-config.yml
    #bucket = "pbshaw-emr-exploration"
    #config_file_key = "scripts/resources/confirmed-cases-config.yml"
    
    if config_location.startswith('s3'):
        bucket, config_file_key = parse_source_location(config_location)
        
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=config_file_key)

        try:
            configfile = yaml.safe_load(response["Body"])
        except yaml.YAMLError as exc:
            return exc
    else:
        print('bad config file: {}'.format(config_location))
        configfile = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.realpath(__file__)), config_location), 'r'))

    return configfile
    
    
    #return {'bucketName': 'pbs-databricks-data-lake', 'stageDir': 's3://{}/output/databricks/transform/baseball_stats', 
    #        'prefix_filter': 'input/retro-stats', 'output_format': 'json'}