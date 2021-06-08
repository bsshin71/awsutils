
import boto3

class S3:
    def __init__(self, config):
        self.s3config = config.get_jsection('s3')
        self.client = boto3.client(
            's3',
            aws_access_key_id=self.s3config['access_key_id'],
            aws_secret_access_key=self.s3config['secret_access_key'],
            region_name=self.s3config['region_name']
            # config=my_config
        )

    def get_object(self, keylocationCsv):
        result = self.client.get_object ( Bucket=self.s3config['bucket'], Key=keylocationCsv)
        return result