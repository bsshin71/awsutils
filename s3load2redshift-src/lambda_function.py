import json
import urllib.parse
import boto3
import uuid
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

print('Loading function')

s3 = boto3.client('s3')
sqs = boto3.resource('sqs')
target_table = 'useractlog'
jsonpath='s3://s3bucket/topics/bos.useractlog/jsonpaths/useractlog_jsonpath.json'
s3_access_id = '*********'
s3_access_key =  '**********'

queuename='s3load.fifo'



def send_message(queue, message_body, messagegroupid, messagededuplicationid=None,  message_attributes=None):
    if not message_attributes:
        message_attributes = {}

    if not messagededuplicationid:
        messagededuplicationid = str(uuid.uuid1())

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageGroupId =messagegroupid,
            MessageAttributes = message_attributes,
            MessageDeduplicationId= messagededuplicationid
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    #key = event['Records'][0]['s3']['object']['key']
    logger.info( 'bucket = ' + bucket )
    logger.info( 'key = ' + key )
    logger.info( 'uuid = ' + str(uuid.uuid1() ))

    try:
        #response = s3.get_object(Bucket=bucket, Key=key)
        queue = sqs.get_queue_by_name(QueueName=queuename)
        body_data= { 'bucket' : bucket , 'key' : key , 'table' : target_table, 'jsonpath' : jsonpath , 's3_access_id' : s3_access_id, 's3_access_key' : s3_access_key }
        msgbody = json.dumps( body_data )
        response = send_message( queue, msgbody, 'mtifilegroup' )    
        return response
    except Exception as e:
        logger.error(e)
        logger.error('Error getting sending {} to sqs  from bucket {}.'.format(key, bucket))
        raise e
