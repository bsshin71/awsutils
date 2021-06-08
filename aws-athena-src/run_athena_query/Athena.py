import time
import uuid
import boto3
from datetime import date

"""
athena의 json result 셋중에 row value 부분만 발췌
"""
def get_var_char_values(d):
    return [obj['VarCharValue'] for obj in d['Data']]


class Athena:
    """
    athena와 연결
    athena 쿼리 결과 자정경로 구성 : s3 버킷정보  + path 정보
    """
    def __init__(self, config):
        self.athconfig  = config.get_jsection('athena')
        self.s3config = config.get_jsection('s3')
        self.client = boto3.client(
            'athena',
            aws_access_key_id=self.athconfig['access_key_id'],
            aws_secret_access_key=self.athconfig['secret_access_key'],
            region_name=self.athconfig['region_name'],
            endpoint_url=self.athconfig['endpoint_url'],
            # config=my_config
        )
        self.database = self.athconfig['database']
        self.Bucket  = self.s3config['bucket']
        self.OutLoc  = self.athconfig['outpath']
    """
    parameter로 전달된 쿼리를 실행한다.
    """
    def start_query_execution(self, query ):
        keylocation = self.OutLoc + '/' + date.today().strftime("%Y/%m/%d")
        fullocation = 's3://' + self.Bucket + '/' + keylocation
        response = self.client.start_query_execution(
            QueryString=query,
            ClientRequestToken=str(uuid.uuid4()),
            QueryExecutionContext={
                'Database': self.database
            },
            ResultConfiguration={
                'OutputLocation': fullocation
            }
        )
        self.keylocation = keylocation
        self.queryid = response['QueryExecutionId']
        return response
    """
    쿼리를 실행한 직후 실행한다.
    maxwait (초단위) 만큼 기다리며 쿼리의 실행상태를 체크한다.
    지정된 시간동안 쿼리가 완료되지 않은 경루 fail 로 간주한다.
    """
    def wait_until_query_end(self, maxwait):
        status = 'RUNNING'
        while (maxwait > 0):
            maxwait = maxwait - 1
            self.querystatus_info = self.client.get_query_execution(
                QueryExecutionId=self.queryid
            )
            status = self.querystatus_info['QueryExecution']['Status']['State']
            print('querystatus:' + status)
            if (status == 'FAILED') or (status == 'CANCELLED'):
                return False
            elif status == 'SUCCEEDED':
                return True
            time.sleep(1)

        return False

    """
    쿼리 결과가 저장된 full path 경로(파일명포함 ) 를 얻는다.
    """
    def get_query_fulllocation(self):
        location = self.querystatus_info['QueryExecution']['ResultConfiguration']['OutputLocation']
        self.querystatus_info = self.client.get_query_execution(
            QueryExecutionId=self.queryid
        )
        status = self.querystatus_info['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            return location, True
        else:
            return None, False

    """
    쿼리 결과가 저장된 csv 파일 정보를 얻는다.
    s3 버킷정보를 제외한 나머지경로+파일명  
    """
    def get_query_keylocationCsv(self):
       return self.keylocation + '/' + self.queryid + '.csv'

    """
    쿼리 결과를  dictionary 형태로 얻는다.  dict = array of { 칼럼명,칼럼value } 
    """
    def get_query_result_dict(self):
        location = self.querystatus_info['QueryExecution']['ResultConfiguration']['OutputLocation']
        response_query_result = self.client.get_query_results(
            QueryExecutionId=self.queryid
        )
        result_data = response_query_result['ResultSet']
        if len(response_query_result['ResultSet']['Rows']) > 1:
            header = response_query_result['ResultSet']['Rows'][0]
            rows = response_query_result['ResultSet']['Rows'][1:]
            header = [obj['VarCharValue'] for obj in header['Data']]
            result = [dict(zip(header, get_var_char_values(row))) for row in rows]
            # print(json.dumps(result, indent=2))
            return location, result
        else:
            return location, None