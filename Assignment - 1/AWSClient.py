import boto3
import os

class AWSClient:
    
    def __init__(self, aws_access_key_id , aws_secret_access_key, aws_service='s3', region='us-east-2'):
        
        self.__client = boto3.client( 
                        aws_service, 
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region
        )
        self.__region = region

    @property
    def region(self):
        return self.__region
    
    @property
    def client(self):
        return self.__client
