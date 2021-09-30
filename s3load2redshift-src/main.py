# -*- coding: utf-8 -*-
import logging
import sys
import boto3
from botocore.exceptions import ClientError
import json
import threading,  time
import signal
from pydantic import BaseModel
from postgresql import Databases
from datetime import datetime
import time
import logconfig
from config import Config


class ThreadWorker(threading.Thread):
    def __init__(self, param1):
        threading.Thread.__init__(self)
        self.thread_number = param1
        self.pg =  Databases(config)

        sqsconfig = config.get_jsection('sqs')
        self.sess = boto3.session.Session(
            aws_access_key_id= sqsconfig['access_key_id'],
            aws_secret_access_key=sqsconfig['secret_access_key'],
            region_name=sqsconfig['region_name'],
        )

        self.sqs = self.sess.resource('sqs', endpoint_url=sqsconfig['endpointurl'])
        self.queue = self.sqs.get_queue_by_name(QueueName=sqsconfig['queuename'])
        self.threadname = f'Thread worker #{self.thread_number}'

    # 작업 쓰레드 별 현재 작업상태를 셋팅한다. ( working, free, stopping 중 한가지임)
    def set_workstat(self,
                     thr_state,
                     s3filename,
                     thr_native_id = None
                     ):
        thr_number = self.thread_number
        thr_id     = self.thread_id
        thr_name   = self.threadname
        #thr_native_id = self.native_id

        #self.pg.reconnect()

        if 'working' in thr_state:
            rows = self.pg.retryexecute(" update  loaderstat " \
                                   " set thr_state = %s, wk_starttime=current_timestamp, wk_endtime = null, s3filename= %s " \
                                   " where thr_number = %s ",
                                    5,
                                   (thr_state, s3filename, thr_number ) )
        elif 'free' in thr_state:
            rows = self.pg.retryexecute(" update  loaderstat " \
                                   " set thr_state = %s,  wk_endtime = current_timestamp" \
                                   " where thr_number = %s ",
                                    5,
                                   (thr_state, thr_number))
        elif 'stopping' in thr_state:
            rows = self.pg.execute(" update  loaderstat set thr_state = 'stopping' " )

        self.pg.commit()

    # copy 명령 처리결과를 로깅한다. 처리 s3파일당 한개의 로그가 생성된다.
    def worklogging( self,
                     start_time,
                     end_time,
                     s3filename,
                     tablename,
                     command,
                     rowcount,
                     result,
                     errmsg
                     ):
        thr_number = self.thread_number

        #self.pg.reconnect()

        rows = self.pg.retryexecute(" insert into loaderlog( thr_number, start_tm, end_tm, sfilename, tablename,   " \
                               "  command, rowcount, result, errmsg ) " \
                               " values ( %s, %s, %s, %s, %s, %s, %s, %s, %s ) " ,
                                5,
                               (thr_number,
                                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                end_time.strftime('%Y-%m-%d %H:%M:%S'),
                                s3filename,
                                tablename,
                                command,
                                rowcount, result, errmsg)
                               )

        self.pg.commit()

    # copy
    def copy2redshift(self, s3file, body_json ):
        copysql = 'copy {} from \'{}\' credentials \'aws_access_key_id={};aws_secret_access_key={}\'' \
                    ' timeformat \'auto\' json \'{}\''.format(
                                                        body_json['table'],
                                                        s3file,
                                                        body_json['s3_access_id'],
                                                        body_json['s3_access_key'],
                                                        body_json['jsonpath']
                                                     )
        result = -1
        errmsg = ''
        try :
            rows = self.pg.retryexecute(copysql, 5 )
            self.pg.commit()
            result = 0
        except Exception as ex:
            result = -1
            errmsg = ''.join(ex.args)
            logging.error(ex)
            logging.error(errmsg )

        return copysql, result, errmsg

    def run(self):
        global loopflag

        self.thread_id = threading.get_ident()
        rows = self.pg.execute(
            "INSERT INTO loaderstat(thr_number, thr_id, thr_name, thr_state)" \
            "values( %s, %s, %s, %s) " ,
            (self.thread_number, self.thread_id, self.threadname, 'free' ))

        logging.info('default logstat row inserted ')
        logging.debug('thr_name:'  + self.threadname )
        self.pg.commit()

        while( loopflag ):
            for message in self.queue.receive_messages(MessageAttributeNames=['All'], MaxNumberOfMessages=1,
                                                  VisibilityTimeout=30, WaitTimeSeconds=2):
                try :
                    message.delete()
                    logging.debug(f'thr num={self.thread_number} body = {message.body}')
                    body_json  = json.loads( message.body )
                    s3file = 's3://{}/{}'.format(body_json['bucket'],body_json['key'])
                    logging.info( 's3file : ' + s3file )
                    self.set_workstat('working', s3filename=s3file)
                    starttime = datetime.now()
                    copysql, result, errmsg = self.copy2redshift(s3file, body_json)
                    endtime = datetime.now()
                    elapsedtime = endtime - starttime
                    logging.info('elapsed time {} for file {} '.format(elapsedtime, s3file ) )
                    self.worklogging(starttime, endtime, s3file, body_json['table'], copysql, 0, result, errmsg )
                    self.set_workstat('free', s3filename=None)
                    if result > 0:  # failed to copy to redshift
                        pass
                # to do : the messsage which is not processed can be send to another queue to handle it later
                except Exception as ex:
                    logging.error(ex)
                    logging.error('error while processing : ' + s3file)

        self.set_workstat('stopping', s3filename=None)
        logging.info('Thread ended : ' + threading.current_thread().name )
        sys.stdout.flush()

def exit_gracefully(signum, frame):
    global loopflag
    root_logger.info('stop signal captured ')
    loopflag = False

def reset_stattable():
    pg = Databases(config)
    pg.execute("delete from loaderstat")
    pg.commit()
    root_logger.info('delete loaderstat for resetting while startup')

def main():
    global loopflag
    global config

    logconfig.loadlogconfig()

    # config 파일을 로딩한다.
    configfile = 'conf.json'
    config = Config()
    try:
        config.load_jconfig(configfile)
    except:
        sys.exit(1)

    logging.info("success load config")

    loopflag = True
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    reset_stattable()

    mainonfig = config.get_jsection('main')

    for thr_num in range( mainonfig['threadnum']):
        worker = ThreadWorker( thr_num  )
        worker.setName(name=f'Thread worker #{thr_num}')
        worker.setDaemon(False)
        worker.start()

    time.sleep(1)
    mainThread = threading.current_thread()
    for thread in threading.enumerate():
        if thread is not mainThread:
            thread.join()

if __name__ == '__main__':
    root_logger = logging.getLogger()
    main()
