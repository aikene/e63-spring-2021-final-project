from __future__ import print_function
import json
import urllib
import urllib.request
import os
import boto3
from datetime import date
import uuid
import dateutil

# Uaing python dat function to get today's date
# NOTE: AWS uses GMT time, so that will be th time used
today = date.today()

# URL to get the function to pull data as JSON from API
url = "https://hub.mph.in.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%2239239f34-11ff-4dfc-9b9a-a408b0399458%22"

# Create a file object using urllib
fileobj = urllib.request.urlopen(url)

# Declaration of lambda handler function
def lambda_handler(event, context):

    try:
        # Write full JSON object using json.loads module
        fulljson = json.loads(fileobj.read())
    except:
        print("Unable to read file")
        raise

    try:
        # Create S3 resource
        s3 = boto3.resource('s3')

        # Create a S3 key file with '/' as folder path
        key = f"indiana-schools-covid/{today.year}/{today.month}/{today.day}/{uuid.uuid1()}.json"

        # Create an S3 object
        obj = s3.Object('aws-demo-ingestion-bucket', key)

        # Write results of the object using PUT
        obj.put(Body=json.dumps(fulljson['result']['records']))
    except:
        print("Unable to load file")
        raise

    # Return successful Status Code wi location of file
    return {
        'statusCode': 200,
        'body': 'success',
        's3path': f'aws-demo-ingestion-bucket/{key}',
    }
