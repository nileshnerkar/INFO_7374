import boto3
import os
import json

class AWSClient:
    
    def __init__(self, aws_access_key_id=None , aws_secret_access_key=None, aws_service='s3', region='us-east-1', endpoint_url=None):
        
        if not aws_access_key_id:
            #Read config.json
            with open("AWS/aws_config.json", 'rb') as config_file:
                config_data = json.load(config_file)
                self.__client = boto3.client( 
                            aws_service, 
                            aws_access_key_id=config_data['aws_access_key_id'],
                            aws_secret_access_key=config_data['aws_secret_access_key'],
                            region_name=region,
                            endpoint_url = endpoint_url
                )
            pass
        else:
            self.__client = boto3.client( 
                            aws_service, 
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=region,
                            endpoint_url = endpoint_url
            )
        self.__region = region

    @property
    def region(self):
        return self.__region
    
    @property
    def client(self):
        return self.__client
