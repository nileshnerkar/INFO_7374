import boto3
import logging
from io import StringIO
from botocore.exceptions import ClientError
import os

class DataIngestor:

    def __init__(self, aws_client):
        self.__client = aws_client.client
        self.__region = aws_client.region

    
    def createS3Bucket(self, bucketName):
        """
        Create S3 bucket at region
        If region not specified default to us-east-1

        :param bucketName:[Required] Specify the name of bucket to be created
        :param region: Region in which S3 bucket is created 
        """
        try:
            if bucketName not in self.listS3Buckets():
                location = {'LocationConstraint': self.__region}
                self.__client.create_bucket(Bucket=bucketName, CreateBucketConfiguration=location)
            else:
                print(f"Bucket {bucketName} already exists.")
                logging.info(f"Bucket {bucketName} already exists.")        
        except ClientError as e:
            logging.error(e)
            return False
        return True

        
    def listS3Buckets(self):
        """
        Returns List of S3 buckects
        """

        response = self.__client.list_buckets()

        bucket_names=[]
        for bucket in response['Buckets']:
            bucket_names.append(bucket['Name'])
        return bucket_names

    
    def upload_fileobj(self, DIR_PATH, bucketName):
        """
        Upload a DataFrame to S3 bucket

        :param DIR_PATH: (Str) Path of scraped CSV Files 
        :param bucketName:[Required] Name of S3 bucket where to upload DataFrame

        """
        try:

            file_list = os.listdir(DIR_PATH)
            for file_nm in file_list:
                FILE_NAME =  f'{DIR_PATH}\{file_nm}'
                yr_q = file_nm.split('_')[2].split('.')[0]
                key = f'{yr_q[0:4]}/{yr_q[4:6]}/LoanStats.csv'
                file_flag = False
                if self.isUploadedToS3(bucketName=bucketName, key=key):
                    print("Object already in S3: {}".format(key))
                else:
                    print("Uploading Object to S3: {}".format(key))
                    with open(FILE_NAME, "rb") as f:
                        self.__client.upload_fileobj(f, bucketName, key)

        except (ClientError, FileNotFoundError) as e:
                logging.error(e)

    
    def deleteS3File(self, bucketName, key):
        """
        Delete an Object from S3 bucket matching key
        :param bucketName: Name of S3 bucket from where object is to be deleted
        :param key: Key representing object to be deleted
        """
        # s3 = boto3.resource('s3')
        # s3.Object(bucketName, key).delete()
        try:
            self.__client.delete_object(Bucket=bucketName, Key=key)
        except (ClientError) as e:
            logging.error(e)


    def deleteS3Buket(self, bucketName):
        """
        Delete S3 Bucket matching Name
        :param bucketName: Specify name of bucket to be deleted
        """
        try:
            self.__client.delete_bucket(Bucket=bucketName)
        except (ClientError) as e:
            logging.error(e)

    
    def isUploadedToS3(self, bucketName, key):
        """
        Checks if key is already present in S3 Bucket
        :param : Specify name of bucket to look in
        :param : Specify name of Key to be looked for
        returns Boolean value. True if key exists in given Bucket else False.
        """
        try:
            obj = self.__client.get_object(Bucket=bucketName, Key=key)
            obj['Body'].read()
        except ClientError as e:
            #print(e.response)
            return int(e.response['ResponseMetadata']['HTTPStatusCode']) != 404
        return True