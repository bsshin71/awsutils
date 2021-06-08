import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
import boto3 
from awsglue.job import Job
from pytz import timezone

## @params: [JOB_NAME]
# Glue 에서 실행시
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#sc = SparkContext().getOrCreate()
#glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# docker 에서 실행시
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

#glueContext = GlueContext(SparkContext.getOrCreate()) 
s3 = boto3.resource('s3')

# To void creation of SUCCES files , it is quite bothersome
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

#configure
bucketname="bos-dev-log-analysis" # s3://bos-dev-log-analysis
targetlist = [ { "src" : "topics/trade.pc", "dest" : "parquet/trade.pc"} ,
               { "src" : "topics/trade.mob", "dest" : "parquet/trade.mob"},
               { "src" : "topics/actlog", "dest" : "parquet/actlog"} 
             ]
filegroupsize="1073741824"  #  parquet max file zie = 1G
KST = datetime.timezone(datetime.timedelta(hours=9))

# 경로가 존재하는지 검사한다.
def checkdir_ifexist(bucketname, path) -> bool:
    mybucket = s3.Bucket( bucketname  )
    objs = list ( mybucket.objects.filter(Prefix=path) )
    print ( path + ', filenum : ' + str( len( objs ) ) )
    return  len(objs) > 1

#dest 경로에 파일이 이미 존재하면 삭제한다.
def delete_destfile( bucketname, path) -> bool:
    mybucket = s3.Bucket( bucketname  )
    mybucket.objects.filter(Prefix=path).delete()
    

#  지정한 경로의 s3 json 파일들을 parquet 파일로 변환해서 저장한다.
def json2parquet(src, dest):
    inputDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                                        connection_options = {"paths": [src], 
                                                                              'groupFiles': 'inPartition',
                                                                              'groupSize': filegroupsize}, 
                                                        format = "json")
    print('record num : %i' % inputDF.count())
    inputDF.toDF().write.mode('overwrite').parquet( dest)
    #s_df = inputDF.toDF().repartition(1)
    #s_df = inputDF.toDF().repartition(partition_size="1KB")
#     glueContext.write_dynamic_frame.from_options(
#         frame = inputDF,
#         connection_type = "s3",    
#         connection_options = {"path": dest },
#         format = "parquet")


print("############ json2parquet job started ######## ")
#date_time_str='2021-05-14 17:10:00.123456'
#date_time_str='2021-05-26 15:10:00.123456'
#now = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
#now = datetime.datetime.now(timezone('Asia/Seoul'))
now = datetime.datetime.utcnow()
print('current time : ' + str(now)) 
target_date_hour= now + datetime.timedelta(hours=-1)
print('taget time : ' + str(target_date_hour))

timepartition = target_date_hour.strftime('_year=%Y/_month=%m/_day=%d/_hour=%H/')

for work  in targetlist:
    src  = 's3://{bucket}/{path}/{timepart}'.format( bucket=bucketname, path=work['src'], timepart=timepartition  )
    dest = 's3://{bucket}/{path}/{timepart}'.format( bucket=bucketname, path=work['dest'], timepart=timepartition  )
    print( 'source src : ' + src)
    print( 'target src : ' + dest)
    dir_exist =  checkdir_ifexist( bucketname, '{path}/{timepart}'.format(path=work['src'], timepart=timepartition) ) 
    if not dir_exist:
        print( 'Not yet created json path : {0}'.format(src))
        continue
#     if checkdir_ifexist(bucketname, '{path}/{timepart}'.format(path=work['dest'], timepart=timepartition) ):
#         print( 'alreay exist dest path , so delete all parquet before make it :' + dest)
#         delete_destfile( bucketname,  '{path}/{timepart}'.format(path=work['dest'], timepart=timepartition))    
    print( ' transforming work started for source src : ' + src)    
    json2parquet( src, dest)
    print( ' transforming work ended in  dest path : ' + dest)    
    

print("############ json2parquet job ended ########")


# Glue에서 실행시
Job.commit()
