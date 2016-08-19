from __future__ import print_function

import json
import urllib
import boto3
import datetime
import pymysql
import logging
import rds_config
import sys
from math import floor

#rds settings
rds_host  = rds_config.rds_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
port = rds_config.port

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig()

print('Loading function')

s3 = boto3.client('s3')

#RDS connection stuff

server_address = (rds_host, port)
try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")

def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key) #gives out dict
        data = response["Body"].read() #gives out string
        text = data.splitlines()[0]
        output = json.loads(text)
        print("Success in loading the object")

        startTime = output["start_time"][0]
        stopTime = startTime + 1
        objectKey = str(key)
        dayID = output["dayCount"]
        patientID = objectKey+'-'+str(dayID)
        channelNumber = output["chunk"]
        sig_start = dateTimeConvertor(startTime,0)
        sig_stop = dateTimeConvertor(stopTime,0)
        print("Successfully read the object into variables")

        with conn.cursor() as cur:
            cur = conn.cursor()
            # cur.execute('insert into edfPatientInfo (PatientID, StartTime, StopTime, DayInfo, ObjectKey, ChannelNumber) values(patientID, "2000-04-12 11:26:00", "2000-04-12 12:26:00", 4, objectKey, 4)')
            cur.execute('INSERT INTO edfPatientInfo (PatientID, StartTime, StopTime, DayInfo, ObjectKey, ChannelNumber) VALUES (%s, %s, %s, %s, %s, %s)',(patientID, sig_start, sig_stop, dayID, objectKey, channelNumber))
            conn.commit()
            print("Transfer completed...")

        print("Patient ID: ", patientID)
        print("Start time: ", sig_start)
        print("Stop time: ", sig_stop)
        print("Day Info: ", dayID)
        print("Object key: ", objectKey)
        print("Channel number: ", channelNumber)

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def dateTimeConvertor(time, day):
    timeHours = time
    timeMinutes = 60 * (timeHours % 1)
    timeSeconds = 60 * (timeMinutes % 1)

    Hours = floor(timeHours)
    Minutes = floor(timeMinutes % 60)
    Seconds = floor(timeSeconds % 60)
    timeq =  ("%d:%02d:%02d" % (Hours, Minutes, Seconds))
    
    start_date = "2000-01-01"
    date_1 = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = date_1 + datetime.timedelta(days = day, hours = Hours, minutes = Minutes, seconds = Seconds)
    return end_date
