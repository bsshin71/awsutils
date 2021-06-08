import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

#confiugre
filegroupsize="1073741824"  #  parquet max file zie = 1G
KST = datetime.timezone(datetime.timedelta(hours=9))
# To void creation of SUCCES files , it is quite bothersome
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# for local test 
# argv = [ 'whatevs', 
#          '--JOB_NAME=ThisIsMySickJobName',  
#          '--bucketname=dev-log-analysis',
#          '--src=topics/trade.pc/_year=2021/_month=05/_day=27/_hour=09/',
#          '--dest=parquet/trade.pc/_year=2021/_month=05/_day=27/_hour=09/'
       ]
#args = getResolvedOptions(argv, ['JOB_NAME','bucketname', 'src', 'dest'])
args = getResolvedOptions(sys.argv, ['JOB_NAME','bucketname', 'src', 'dest'])

print(args['JOB_NAME'])
print(args['bucketname'])
print(args['src'])
print(args['dest'])

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucketname = args['bucketname']
src  = args['src']
dest = args['dest']


#glueContext = GlueContext(SparkContext.getOrCreate()) 
s3 = boto3.resource('s3')

dir_exist =  checkdir_ifexist( bucketname, src ) 
if not dir_exist:
    print( 'Not yet created json path : {0}'.format(src))
    raise Exception('Not yet created json path : {0}'.format(src))
    sys.exit(0)

srcpath  = 's3://{bucket}/{src}/'.format( bucket=bucketname, src=src  )
destpath = 's3://{bucket}/{dest}/'.format( bucket=bucketname, dest=dest  )
    
print( ' transforming work started for source src : ' + srcpath)
json2parquet( srcpath, destpath) 
print( ' transforming work ended in  dest path : ' + destpath)    
      
job.commit()   

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
