import json
import boto3
import requests
import logging
import os
import string
import requests
import time
from urllib.parse import urlencode
from datetime import datetime
from dateutil import tz


# PRD mod3 envrionment flag
PRD_MODE=True
#Message Send mode : OFF=don't send message via telegram
SEND_MSG_MODE=True #ON/OFF


logger = logging.getLogger()
logger.setLevel( logging.INFO)

from_zone = tz.gettz('UTC')
to_zone= tz.gettz('Asia/Seoul')


prd_telegram_info = {
                    "alert":{   "botToken":'xxx',
                                "chat_id":'-xxxx'},
                    "info":{   "botToken":'botxxxxx',
                                "chat_id":'-xxxxxx'},
                    }

dev_telegram_info = {
                        "alert":{  "botToken":'xxxx',
                                    "chat_id":'xxxx'},
                        "info":{   "botToken":'xxxx',
                                    "chat_id":'xxxxx'},
                    }



alert_category_group = { "availability", "deletion", "failover", "failure", "low storage" }


#for test
test_event = {
  "version": "0",
  "id": "844e2571-85d4-695f-b930-0153b71dcb42",
  "detail-type": "RDS DB Cluster Event",
  "source": "aws.rds",
  "account": "123456789012",
  "time": "2018-10-06T12:26:13Z",
  "region": "us-east-1",
  "resources": [
    "arn:aws:rds:us-east-1:123456789012:db:mysql-instance-2018-10-06-12-24"
    ],
  "detail": {
    "EventCategories": [
      "notification"
    ],
    "SourceType": "CLUSTER",
    "SourceArn": "arn:aws:rds:us-east-1:123456789012:db:mysql-instance-2018-10-06-12-24",
    "Date": "2018-10-06T12:26:13.882Z",
    "SourceIdentifier": "rds:mysql-instance-2018-10-06-12-24",
    "Message": "Database cluster has been patched"
  }
}

def sendto_telegram(mode, msg):
    
    if SEND_MSG_MODE == False:
        logger.info(' Send_msg_mode is disabled , so do not  send message to telegram' )
        return
    
    if PRD_MODE == True:
        telegram_info = prd_telegram_info
    else:
        telegram_info = dev_telegram_info
        
    chat_id   = telegram_info[ mode ] [ 'chat_id' ]
    botToken  = telegram_info[ mode ] [ 'botToken']
    params1   = urlencode( {'chat_id':chat_id,'text':msg }).encode()
    r = requests.post('https://api.telegram.org/'+ botToken+'/sendMessage',
            params=params1, json=[])
    result=r.json()
    if result['ok'] == True:
        logger.info('Success to send message.')
        return "ok"
    else:
        logger.info('Failed to send message.')
        return "fail"



def utc2localtime( utcstr ):
    utc  = datetime.strptime( utcstr, '%Y-%m-%dT%H:%M:%S.%fZ')
    utc = utc.replace(tzinfo=from_zone)
    localtime =  utc.astimezone( to_zone)

    return localtime.strftime('%Y-%m-%d %H:%M:%S (KST)'  ) 


def lambda_handler(event, context):
    # TODO implement
    logger.info( event )
    logger.info( context )

    detailtype= event['detail-type']
    time=event['time']
    region=event['region']
    eventcategory=event['detail']['EventCategories'][0]
    eventdate=event['detail']['Date']
    sourcetype=event['detail']['SourceType']
    server=event['detail']['SourceIdentifier']
    message=event['detail']['Message']

    msg='[%s]\n \
EventCategory=%s \n \
Date=%s \n \
SourceType=%s \n \
Server=%s \n \
Message=%s' % ( detailtype, eventcategory, utc2localtime( eventdate ),  sourcetype, server, message )

    logger.info( msg )

    mode='info'
    if eventcategory in alert_category_group:
        mode='alert'

    result = sendto_telegram( mode , msg )

    return {
        'result' : result,
        'message':  json.dumps(' called from my RDS Event logging  lambda function ' )
    }



#print( lambda_handler(test_event, None) )
