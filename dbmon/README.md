# dbmon

AWS  RDS DB ( mysql / aurora ) 의 상태를  폴링하다가  지정한 임계값을 초과할 경우  텔레그램으로 알림을 받을 수 있는 프로그램이다.

## 소스구성

| 소스명       | 용도                      |
| ------------ | ------------------------- |
| logging.conf | 로깅 configuration        |
| metric.yaml  | 모니터링 메트릭 정의 파일 |
| rdsmon3.py   | 실제 구동파일 python      |
| runmon.sh    | crontab 에 등록하는 파일  |

## 사용법

1.  logs 폴더를 생성한다.

2. metric.yaml 에  편집하여 필요한 임계값을 설정한다.   불필요한 메트릭은 off 로  disable 할 수 있다.

   메트릭을 추가할 수 도 있다.

3. rdsmon3.py  에서  메시지를 받을 텔레그램의   token 정보와  chat_id를 설정한다.  미리  텔레그램의 botFather를 통해서 bot을 생성하고  그룹창을 생성해 놔야  token 정보와 chat_id를 구할 수 있다.  구하는 방법은 인터넷에 찾으면 많이 나온다.

4. runmon.sh 를 crontab 명령어로  1분 주기로 반복 호출하도록 등록한다.

5. runmon.sh 가  구동되는 AWS EC2 서버에   aws config 와 credential 파일이 등록되어 있어야 한다.

   참고 : <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html>

6. AWS IAM 의 정책편집기로 아래의 정책을 생성하고   AWS 의 python 프로그램에서 사용하는 AWS 유저에 해당 권한을 부여한다.  

   정책명 : RDSPythonMONPolicy 

   ```
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Sid": "VisualEditor0",
               "Effect": "Allow",
               "Action": [
                   "cloudwatch:DescribeAlarmHistory",
                   "cloudwatch:GetDashboard",
                   "ec2:DescribeInstances",
                   "cloudwatch:GetMetricData",
                   "cloudwatch:DescribeAlarmsForMetric",
                   "cloudwatch:DescribeAlarms",
                   "cloudwatch:GetMetricStatistics",
                   "cloudwatch:GetMetricWidgetImage",
                   "cloudwatch:ListMetrics",
                   "rds:DescribeDBInstances",
                   "rds:DescribeDBClusters",
                   "cloudwatch:DescribeAnomalyDetectors",
                   "ec2:DescribeInstanceStatus"
               ],
               "Resource": "*"
           },
           {
               "Sid": "VisualEditor1",
               "Effect": "Allow",
               "Action": [
                   "logs:DescribeLogStreams",
                   "logs:GetLogEvents",
                   "logs:FilterLogEvents"
               ],
               "Resource": "arn:aws:logs:*:*:log-group:RDSOSMetrics:*"
           }
       ]
   }
   ```


## 사용예

![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fk.kakaocdn.net%2Fdn%2FbGwrYJ%2FbtqzxKKs7wL%2FLdvUCvLCH4ZKZ0hyrgvFEk%2Fimg.png)