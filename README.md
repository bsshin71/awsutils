# awsutils
various utility program that can use on AWS

## dbmon
The dbmon get the mornintoring metrics of AWS RDS DB ( mysql, aurora) periodically and compare it with the threshold value in the configuration file, if the comparion result is abnoraml, the dbmon send alert message  via telegram

## RdsEventNoti
The rdseventNoti is a lambda code which can be called in AWS service,  AWS  cloud Event can be passed to RdsEventNoti Lambda code,
when RdsEventNoti Lambda receives the event  from cloudwatch , then it deliver the event  received  to the telegram.
the whole source code is simple and short, but  you can subsrcibe  the every event which occurred in RDS DB .
