#!/usr/bin/env python3

import boto3
from boto3.session import Session
from datetime import datetime, timedelta

import os
import string
import requests
import json
import time
from urllib.parse import urlencode

import logging
import logging.config
import re
import yaml

CW_WINDOW = 5 # Min
PERIOD = 300 # Secs


# PRD mod3 envrionment flag
PRD_MODE=True

#Message Send mode : OFF=don't send Else Send messave via telegram
SEND_MSG_MODE=True #ON/OFF

# DB instance classes listed on the following url
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
db_classes_mem = {
    'db.m5.24xlarge' :  384,
    'db.m5.16xlarge' :  256,
    'db.m5.12xlarge' :  192,
    'db.m5.8xlarge'  :  128,
    'db.m5.4xlarge'  :  64,
    'db.m5.2xlarge'  :  32,
    'db.m5.xlarge'   :  16,
    'db.m5.large'    :  8,
    'db.m4.16xlarge' :  256,
    'db.m4.10xlarge' :  160,
    'db.m4.4xlarge'  :  64,
    'db.m4.2xlarge'  :  32,
    'db.m4.xlarge'   :  16,
    'db.m4.large'    :  8,
    'db.m3.2xlarge'  : 30,
    'db.m3.xlarge'   : 15,
    'db.m3.large'    : 7.5,
    'db.m3.medium'   : 3.75,
    'db.t3.2xlarge'  : 32,
    'db.t3.xlarge'   : 16,
    'db.t3.large'    : 8,
    'db.t3.medium'   : 4,
    'db.t3.small'    : 2,
    'db.t2.2xlarge'  : 32,
    'db.t2.xlarge'   : 16,
    'db.t2.large'    : 8,
    'db.t2.medium'   : 4,
    }

prd_telegram_info = {
    "alert":{  "botToken":'xxxxxx',
				"chat_id":'-xxxxx'},
    "info":{   "botToken":'xxxx',
				"chat_id":'-3xxxxx'}, 
	} 

dev_telegram_info = {
    "alert":{  "botToken":'bot90737xxxx',
				"chat_id":'xxx'},
    "info":{   "botToken":'bot9582554xxxx',
				"chat_id":'xxxx'}, 
	} 



class RdsMon:
    def __init__(self, profile, region, servermem ):
        self.profile          = profile
        self.region           = region
        self.identifier       = ''
        self.db_classes_mem   = servermem
        self.db_engine_type   = ''
        self.metric_unit      = ''
        self.allocatedStorage = 0

    def setIdentifier( self, identifier):
        self.identifier = identifier
    
    def setDbClassesMem( self, dbclassmem ):
        self.db_classes_mem = db_classmem 

    def setDbClass ( self, dbclass ):
        self.db_class = dbclass 

    def setDbEngineType ( self, dbengine ):
        self.db_engine_type = dbengine

    def setMetricUnit( self, unit ):
        self.metric_unit   = unit

    def setAllocatedStorage( self, storage ):
        self.allocatedStorage = storage

    def get_metric( self, metric,  role=None):
        session = boto3.Session(profile_name = self.profile )
        cloudwatch = session.client('cloudwatch', region_name = self.region )
# Role: WRITER,READER

        if not role:
           dimension = [{u'Name':'DBInstanceIdentifier', u'Value':self.identifier}]
        else:
            if role in ('aurora'):
                #print('......here.....')
                #dimension = [{u'Name':'EngineName',u'Value':'aurora'},{u'Name':'DBClusterIdentifier',u'Value':self.identifier}]
                dimension = [{u'Name':'DBClusterIdentifier',u'Value':self.identifier}]
            else:
                dimension =[{u'Name':'Role',u'Value':role},{u'Name':'DBClusterIdentifier',u'Value':self.identifier}]


        result = cloudwatch.get_metric_statistics(
            Namespace   = 'AWS/RDS',
            MetricName  = metric,
            Dimensions  = dimension, 
            StartTime   = datetime.utcnow() - timedelta(minutes=CW_WINDOW),
            EndTime     = datetime.utcnow(),
            Period      = PERIOD,
            Statistics  = [ 'Average']
            )

        if result['Datapoints']:
            if metric in ( 'ReadLatency','WriteLatency'):
                # Transform into miliseconds
                result = '%.2f' % float( result['Datapoints'][0]['Average'] * 1000 )
            else:
                result = '%.2f' % float( result['Datapoints'][0]['Average'] )

            if metric in ( 'FreeableMemory' ):
                try:                                                                      
                    logger.debug( 'db class = ' + self.db_class )                                    
                    memory = db_classes_mem [ self.db_class ] * 1024 ** 3                     
                except IndexError:                                                         
                    logger.error ('Unknown DB instance class "%s"' % self.db_class)
                    return -1                  
                used_percent = '%.2f' % ( ( float(result) /memory) * 100 )                        
                result = float( used_percent ) 
            elif metric in ( 'FreeStorageSpace' ):
                    if self.metric_unit in ( '%' ):
                        allocsize =   self.allocatedStorage  * 1024 ** 3
                        logger.debug( 'AllocatedStorage Size=%f FeeStorageSize=%f' % ( allocsize , float(result) ) )
                        used_percent = '%.2f' % ( (  float(result)/ float(allocsize) )  * 100 )      
                        result = float( used_percent )
                    else:
                        pass         
        else:
            logger.error('Unable to get RDS statistics ' + metric )
            return -1
        
        return float( result )

        



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
    else:
        logger.info('Failed to send message.')    



def check_if_abnormal_result( value , condition ):
    operator    = condition[2]
    limit_value = condition[3]
    result      = False
    if operator in ( 'lt' ):
        if value <= limit_value:
            return True
        else:
            return False
    if operator in ( 'gt' ):
        if value >= limit_value:
            return True
        else:
            return False
    if operator in ( 'eq' ):
        if value == limit_value :
            return True
        else:
            return False
    return False


def optostr( operator ):
    if operator in ( 'lt' ):
        return "less than"
    elif operator in ( 'gt' ):
        return "greater than"
    elif operator in ( 'eq' ):
        return "equal"

    return ""


if __name__ == "__main__":
    try:
        rds_metrics={}
        rdsmon = RdsMon( profile='default', region='ap-northeast-2', servermem=db_classes_mem ) 
        rds = boto3.client('rds')

        logger = logging.config.fileConfig('logging.conf')
        logger = logging.getLogger('dbmon')

        if os.getenv('PRD_MODE','OFF') in ( 'ON') :
            logger.info(' Start with PRD_MODE=ON ')
            PRD_MODE=True
        else:
            logger.info(' Start with PRD_MODE=OFF ')
            PRD_MODE=False

        if  os.getenv('SEND_MSG_MODE', 'OFF') in ( 'ON'):
            logger.info( ' Start with SEND_MSG_MODE=ON ' )
            SEND_MSG_MODE=True
        else:
            logger.info( ' Start with SEND_MSG_MODE=OFF' )
            SEND_MSG_MODE=False

    #  change  tizme zone for timestamp in logging file
        os.environ['TZ'] = 'Asia/Seoul'
        time.tzset() 

        logger.debug('debug message')
        logger.info('info message')
        logger.warning('warn message')
        logger.error('error message')
        logger.critical('critical message')
  
    # loading metric configuration information to dictionary variables    
        with open('metric.yaml') as f:
            db_metrics = yaml.safe_load( f)

    # get all of the db instances
        dbs = rds.describe_db_instances()
        for db in dbs['DBInstances']:
            db_instance_name     = db[ 'DBInstanceIdentifier' ]
            db_instance_status   = db[ 'DBInstanceStatus'     ]
            db_engine_name       = db[ 'Engine'               ]
            db_classes           = db[ 'DBInstanceClass'      ]
            db_allocated_storage = db[ 'AllocatedStorage'     ] 

            logger.debug(  'db_instance_name     : ' + db_instance_name  )
            logger.debug(  'db_instance_status   : ' + db_instance_status  )
            logger.debug(  'db_engine_name       : ' + db_engine_name  )
            logger.debug(  'db_allocated_storage : ' + str( db_allocated_storage) ) 

            if not db_instance_status in ( 'available', 'backing-up' ) :
                msg = '!!!Alert!!!. server=%s is not available, status=%s' % ( db_instance_name, db_instance_status  )
                logger.warning( msg )
                sendto_telegram( 'alert', msg )
                continue

            if 'aurora' in db_engine_name:
                db_engine_type = 'aurora' 
            else:
                db_engine_type = 'mysql'

            rdsmon.setIdentifier       ( db_instance_name )
            rdsmon.setDbClass          ( db_classes       )
            rdsmon.setDbEngineType     ( db_engine_type   )
            rdsmon.setAllocatedStorage ( db_allocated_storage )

            for key, val in db_metrics.items():
                metric               = re.sub( '\d+.', '', key).strip()  # remove -digit from metric key
                metric_db_type       = val[0]
                metric_instance_name = val[1]
                metric_operator      = val[2]
                metric_limit_value   = val[3]
                metric_alert_type    = val[4]
                metric_unit          = val[5]
                metric_run_onoff     = val[6]

                logger.debug( 'metric key            = ' + key )
                logger.debug( 'metric db type        = ' + metric_db_type )
                logger.debug( 'metric instance_name  = ' + metric_instance_name )
                logger.debug( 'db engine             = ' + db_engine_type )

                rdsmon.setMetricUnit   ( metric_unit )

                if not metric_db_type  == 'all' and not db_engine_type in metric_db_type:
                    logger.info( 'skip ..engine=%s metric_db_type=%s metric=%s' % ( db_engine_type , metric_db_type , metric  ) )
                    continue

                if not metric_instance_name == 'all' and not db_instance_name in metric_instance_name:
                    logger.info( 'skip ..db_instance_name=%s metric_instance_name=%s metric=%s' % ( db_instance_name, metric_instance_name, metric ) )
                    continue

                if not metric_run_onoff in ( 'on'):
                    logger.info( 'skip ..db_instance_name=%s metric =%s is disabled ,  please set it to on for running  ' % (metric_instance_name, metric  ) )
                    continue

                result = rdsmon.get_metric( metric, None ) 
                if result < 0:
                    logger.error( ' server=%s metric=%s  result=%.2f'  % ( db_instance_name, metric, result  ) )
                    continue
                is_abnormal = check_if_abnormal_result( result,  val )
                msg = "server=%s metric=%s  result=%.2f %s %.2f %s" % ( db_instance_name, metric, result, optostr( metric_operator ), metric_limit_value, metric_unit )
                logger.info( 'check if ' + msg)
                if is_abnormal == True:
                    if metric_alert_type in ( 'alert'):
                        msg='!!!Alert!!!.' + msg
                        logger.warning( msg )
                    elif metric_alert_type in ( 'info'):
                        msg='[info] ' + msg
                        logger.info( msg ) 
                    sendto_telegram( metric_alert_type, msg )
    except Exception as error:
        logger.error ('error occured while desc instances : ' + error)
