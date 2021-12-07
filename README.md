Gradually, I will add a various utility programs that can use on AWS

## aws-athena-src
athena query runner.
After reading and executing the sql statement in the config file, the result is saved in the DB.

## aws-glue-src
It is a glue etl job source that converts the json format file stored in s3 into a parquet format file.

## cdp-rudderbackend-src
It is a backend api server source that can receive requests from rudderstack api.

## dbmon
The dbmon get the mornintoring metrics of AWS RDS DB ( mysql, aurora) periodically and compare it with the threshold value in the configuration file, if the comparion result is abnormal, the dbmon send alert message  via telegram

## RdsEventNoti
The rdseventNoti is a lambda code which can be called in AWS service,  AWS  cloud Event can be passed to RdsEventNoti Lambda code,
when RdsEventNoti Lambda receives the event  from cloudwatch , then it deliver the event  received  to the telegram.
the whole source code is simple and short, but  you can subscribe  the every event which occurred in RDS DB .



## s3load2redshift-src
when the new object is uploaded to s3 bucket , it's event be transfered to  AWS SQS
this loader program polling the new event from the queue of AWS SQS, and when it receives new event then loader program upload it to redshift with using 'copy from xxx' command
![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2FRohEO%2Fbtrhw8VKV4u%2FCqEY7q7QjKnXcVFJsOZoP1%2Fimg.png)
