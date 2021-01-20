# Databricks notebook source
import boto3
import re
import glob

# COMMAND ----------

# Generic reuse cross module .. Should come from reusable library
def get_files_to_read(arg_bucket, arg_prefix_filter, arg_search_pattern):

    files_to_read = []
    if arg_bucket:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=arg_bucket, Prefix=arg_prefix_filter)

        data_to_read = False
        if response['ResponseMetadata'].get('HTTPStatusCode') == 200:
            if response['KeyCount'] > 0:
                data_to_read = True

        while data_to_read:
            for obj in response['Contents']:
                key = obj['Key']
                if re.search(arg_search_pattern, key):
                    files_to_read.append('s3://'+arg_bucket+'/'+key)

            cont_token = response.get('NextContinuationToken')
            if cont_token:
                response = s3_client.list_objects_v2(Bucket=arg_bucket,Prefix=arg_prefix_filter,
                                            ContinuationToken=cont_token)
            else:
                data_to_read = False
    else:
        for f in glob.glob(arg_prefix_filter+'/**', recursive=True):
            if re.search(arg_search_pattern, f):
                files_to_read.append(f)

    return files_to_read        