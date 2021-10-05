
# s3load2redshift-src

s3 에 파일에 upload되면 upload Event 정보가 aws SQS로 전송된다.
이 프로그램은 aws SQS 대기열을 폴링하면서 new object 정보를
읽어 새로 upload된 s3 파일의 내용을  redshift copy from 구문으로 실행해서
reshift로 로딩하는 작업을 반복한다.


## 실제 파일 : 리스트에 없는 파일들은 사용하지 않는 테스트용 파일이다.

| 파일  | 용도                   |
| -------------- | ---------------------- |
| conf.json   | 접속정보파일   |
| config.py | config 로딩 class |
| logconfig.py | log 설정파일 |
| main.py| main 실행모듈 |
| posgresql.py| DB접속 class ( redshift 가능 )|
| lambda_function.py| s3 object 등록 event 감지 ( aws lambda function으로 등록|

# 설치
pip install -r requirement.txt

#실행
$sh run.sh
#종료
$sh stop.sh
