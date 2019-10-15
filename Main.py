import os
import pandas as pd
from DataIngestor import DataIngestor
from DataExtractor import DataExtractor
from DataProcessor import DataProcessor
from AWSClient import AWSClient

if __name__ == '__main__':
    
    #Get AWS id and access key from args or env variable
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    aws_client = AWSClient(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    #Constants for Data Extraction
    LOGIN_URL = 'https://www.lendingclub.com/auth/login'
    DOWNLOAD_URL ='https://www.lendingclub.com/info/download-data.action'
    DIR_PATH = 'Data\DOWNLOAD_LOAN_DATA'
    
    #Parameters for lending club data scraping
    fileTag = "loanStatsFileNamesJS"
    email=  os.environ['LENDING_CLUB_EMAIL'] #"nerkar.n@husky.neu.edu"
    password= os.environ['LENDING_CLUB_PASSWORD']  #"nilesh77"
    
    #Extract Data from Lending Club URL
    de = DataExtractor(email, password)
    de.extractData(LOGIN_URL=LOGIN_URL, DOWNLOAD_URL=DOWNLOAD_URL, fileTag=fileTag)  

    #Ingest Data into Pipeline
    di = DataIngestor(aws_client)    

    #Create Landing and Processed Buckets
    LANDING_BUCKET = 'lending-club-landing-data-kp'
    PROCESSED_BUCKET='lending-club-processed-data-kp'
    
    di.createS3Bucket(LANDING_BUCKET)
    di.createS3Bucket(PROCESSED_BUCKET)
    
    #Upload Extracted Data to S3 bucket
    di.upload_fileobj(DIR_PATH, bucketName=LANDING_BUCKET)
    
    #Data Pre-Processing
    dp = DataProcessor(aws_client, Landing_BucketName=LANDING_BUCKET, Processed_BucketName=PROCESSED_BUCKET)
    dp.process()