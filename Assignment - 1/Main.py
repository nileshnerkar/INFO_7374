import os
import sys
import pandas as pd
from DataIngestor import DataIngestor
from DataExtractor import DataExtractor
from DataProcessor import DataProcessor
from AWSClient import AWSClient

if __name__ == '__main__':
    
    #Get AWS id and access key from args or env variable
    # aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    # aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    
    if len(sys.argv[1:]) != 4 :
        #Get AWS id and access key from args or env variable
        print("Pass Arguments...!")
        exit(1)
    else:
        count=0
        for arg in sys.argv[1:]:
            if count == 0:
                aws_access_key_id = str(arg)
            elif count == 1:
                aws_secret_access_key = str(arg)
            elif count == 2:
                email = str(arg)
            elif count == 3:
                password = str(arg)
            count += 1


    # aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    # aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    aws_client = AWSClient(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    #Constants for Data Extraction
    LOGIN_URL = 'https://www.lendingclub.com/auth/login'
    DOWNLOAD_URL ='https://www.lendingclub.com/info/download-data.action'
    DIR_PATH = 'Data\DOWNLOAD_LOAN_DATA'
    
    #Parameters for lending club data scraping
    fileTag = "loanStatsFileNamesJS"
    # email=  os.environ['LENDING_CLUB_EMAIL']
    # password= os.environ['LENDING_CLUB_PASSWORD']

    print('Downloading Files...')
    #Extract Data from Lending Club URL
    de = DataExtractor(email, password)
    de.extractData(LOGIN_URL=LOGIN_URL, DOWNLOAD_URL=DOWNLOAD_URL, fileTag=fileTag)

    print('Ingesting Data...')
    #Ingest Data into Pipeline
    di = DataIngestor(aws_client)    

    #Create Landing and Processed Buckets
    LANDING_BUCKET = 'lending-club-landing-data'
    PROCESSED_BUCKET='lending-club-processed-data'

    print('Creating Buckets...')
    di.createS3Bucket(LANDING_BUCKET)
    di.createS3Bucket(PROCESSED_BUCKET)

    print('Uploading Files...')
    #Upload Extracted Data to S3 bucket
    di.upload_fileobj(DIR_PATH, bucketName=LANDING_BUCKET)

    print('Pre-Processing Data...')
    #Data Pre-Processing
    dp = DataProcessor(aws_client, Landing_BucketName=LANDING_BUCKET, Processed_BucketName=PROCESSED_BUCKET)
    dp.process()