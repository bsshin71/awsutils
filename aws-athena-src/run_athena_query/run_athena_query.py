import pandas as pd
from Athena import Athena
from io import StringIO
import csv
from db_con import DBConn
from config import Config
from s3     import S3
from time import time

"""
dataframe으로 로딩된 csv객체를  stdin 를 입력으로 하는 psql COPY 구문으로 실행해서 
bulk 로  database에 insert 한다.
"""
def psql_insert_copy( table, conn, keys, data_iter):
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows ( data_iter )
        s_buf.seek(0)
        columns = ','.join('"{}"'.format(k) for k in keys )
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name )
        else:
            table_name = table.name
        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format( table_name, columns )
        print(sql)
        cur.copy_expert(sql=sql, file=s_buf)


"""
입력으로 전달된 string 쿼리를 실행한다
쿼리를 지정한 시간동안 polling하면서 실행 종류를 대기한다.
쿼리 결과를  pandas dataframe으로 로딩한다.
dataframe 을  postgresql 에 copy 구문으로  bulk로 write한다.
"""
def exe_query_and_save_to_pg( dbconn, athena, s3, query, maxwait, table):
    queryexecinfo = athena.start_query_execution(query)
    print(queryexecinfo)
    waitresult = athena.wait_until_query_end(maxwait)
    if (waitresult == True):
        keylocationCsv  = athena.get_query_keylocationCsv()
        print( 'keylocationCsv:' + keylocationCsv )
        result= s3.get_object( keylocationCsv)
        print(result['Body'])
        df = pd.read_csv( result['Body'])
        print(df)
        conn = dbconn.get_conn()
        df.to_sql(name=table, if_exists='append', index=False, con=conn, method=psql_insert_copy)
        dbconn.close()


if __name__ == '__main__':

    # config 파일을 로딩한다.
    config = Config()
    config.load_jconfig("conf.json")
    config.load_xconfig("query.xml")

    """
    db , athena, s3   객체를 생성하고  각각의 connection 을 생성한다.
    """
    dbconn = DBConn( config )
    athena = Athena( config )
    s3     = S3( config )

    """
    query xml 파일에 있는  query element 갯수만큼 반복하면서  쿼리를 실행하고 쿼리결과를 db에 insert 하는 작업을 수행한다.
    """
    for query in config.get_xquerydict(Nodename='query'):
        print(query.find('sql').text)
        print(query.find('maxwait').text)
        print(query.find('table').text)
        print('querynum = ' + query.attrib['num'])
        onoff   = query.find('onoff').text
        sql     = query.find('sql').text
        maxwait = int( query.find('maxwait').text )
        table   = query.find('table').text
        querynum = query.attrib['num']
        if onoff.upper() == "OFF".upper():
            print("this query disabled , so skip to execute")
            continue
        start_time =  time()
        exe_query_and_save_to_pg(dbconn, athena, s3, query=sql, maxwait=maxwait, table=table)
        end_time   = time()
        elapsed = end_time - start_time
        print('Elapsed time for query num=%s is %f seconds. ' % (querynum, elapsed ) )


    # queryexecinfo = athena.start_query_execution(QUERY, queryLocation)
    # print(queryexecinfo)
    #
    # waitresult = athena.wait_until_query_end(30)
    # if (waitresult == True):
    #     (location, rows) = athena.get_query_result_dict()
    #     if (rows != None):
    #         print('location: ' + location)
    #         # print(json.dumps(rows, indent=2))
    #         for row in rows:
    #             print(row)
    # print(athena)
