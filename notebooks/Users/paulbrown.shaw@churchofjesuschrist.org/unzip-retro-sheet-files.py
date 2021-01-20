# Databricks notebook source
# MAGIC %md
# MAGIC # UnZip Retro Sheets Data Files

# COMMAND ----------

import json
import boto3
from io import BytesIO
import zipfile
import re

# COMMAND ----------

s3_client = boto3.client('s3') 
s3_resource = boto3.resource('s3')

# COMMAND ----------

input_bucket_name = 'pbs-databricks-data-lake'
zip_file_prefix = 'input/zip-files/2010'


response = s3_client.list_objects_v2(
    Bucket=input_bucket_name,
    Prefix=zip_file_prefix)

# COMMAND ----------

if response['ResponseMetadata'].get('HTTPStatusCode') == 200:
    data_to_read = True
s3_object_list = []
while data_to_read:
    for obj in response['Contents']:
        s3_object_list.append(obj)
    cont_token = response.get('NextContinuationToken')
    if cont_token:
        response = s3.list_objects_v2(Bucket=bucket_name,Prefix=prefix_filter,ContinuationToken=cont_token)
    else:
        data_to_read = False
        

# COMMAND ----------

year_pattern = '.*\/(\d\d\d\d)'
for item in s3_object_list:
    key = item['Key']
    if key.endswith('.zip'):
        print(key)
        decade = re.findall(year_pattern, key)[0]
        zip_obj = s3_resource.Object(bucket_name=input_bucket_name, key=key)
        buffer = BytesIO(zip_obj.get()['Body'].read())
        z = zipfile.ZipFile(buffer)
        for filename in z.namelist():
            print(z.getinfo(filename))
            unzip_key = 'input/retro-stats/{}games/{}'.format(decade, filename)
            s3_client.upload_fileobj(z.open(filename), input_bucket_name, unzip_key)


# COMMAND ----------

s3_object_list

# COMMAND ----------

