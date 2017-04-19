from __future__ import print_function

import json
import urllib
import boto3
import datetime
from math import floor

print('Loading function')

s3 = boto3.client('s3')
client = boto3.client('dynamodb')


def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    
    response = s3.get_object(Bucket=bucket, Key=key)
    print("CONTENT TYPE: " + response['ContentType'])
    open_data = response["Body"]
    print("The file that is being uploaded is: ", key)

    # assinging variables
    dataSet = json.load(open_data)
    expt = dataSet["meta"]["experiment"]
    subject = dataSet["meta"]["subject"]
    startT = str(dataSet["meta"]["start_time"])
    stopT = str(dataSet["meta"]["stop_time"])
    day = str(dataSet["meta"]["day"])
    fileName = str(key)
    
    year = str(subject)
    start = str(dateTimeConvertor(year, day, startT))
    stop = str(dateTimeConvertor(year, day, stopT))

    print("uploading...")
    
    response = client.put_item(
        TableName='behavioral-data',
        Item={
            'file name': {
                'S': fileName,
                },
            'experiment': {
                'S': expt,
                },
            'subject': {
                'S': subject,
                },
            'start time': {
                'S': start,
                },
            'stop time': {
                'S': stop,
                },
            'day': {
                'N': day,
                }
            }
    )

def dateTimeConvertor(year, day, time):
    start_date = year+"-01-"+day
    dateTime = start_date+" "+time
    
    end_date = datetime.datetime.strptime(dateTime, "%Y-%m-%d %H:%M:%S:%f")
    return end_date