import os
import glob
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from google_images_download import google_images_download

def create_s3_client(aws_access_key_id,aws_secret_access_key):
    client = boto3.client('s3', 
                 aws_access_key_id=aws_access_key_id, 
                 aws_secret_access_key=aws_secret_access_key)
    return client

def downloadimages(query):

    response = google_images_download.googleimagesdownload()
    
    arguments = {"keywords": query, 
                 "format": "jpg", 
                 "limit":50, 
                 "size": "medium",
                 "no_numbering":True} 
    try: 
        response.download(arguments)
      
    # Handling File NotFound Error     
    except FileNotFoundError:  
        arguments = {"keywords": query, 
                     "format": "jpg", 
                     "limit":5,  
                     "size": "medium",
                     "no_numbering":True}

        # Providing arguments for the searched query 
        try: 
            # Downloading the photos based 
            # on the given arguments 
            response.download(arguments)  
        except: 
            pass

def rename_files(downloaded_images_list):
    max_num = 0
    
    # if os.path.exists(os.getcwd()+'/Renamed/'):
    #     os.system("sudo rmdir " + os.getcwd() + "/Renamed/")
    # else:
    #     os.system("sudo mkdir " + os.getcwd() + "/Renamed/")
    
    renamed_file_list = glob.glob(os.getcwd()+'/downloads/' + '*.jpg')
    
    if len(downloaded_images_list) == 0:
        print('No files in download directory to rename')
    else:
        
        #Get the existing count of files present in the renamed directory
        for f in renamed_file_list:
            prev = int(f.split('/')[-1].split('.jpg')[0])
            max_num = max(prev,max_num)
            
        for i in range(len(downloaded_images_list)):
            max_num += 1
            os.rename(src=downloaded_images_list[i], dst=os.getcwd()+'/downloads/'+str(max_num)+'.jpg')

def main(**kwargs):

    exists = False
    search_queries = ['vehicle single license number plate USA']
    bucket_name = kwargs['bucket'] #'pgdp-3-bucket-1'
    
    AWS_ACCESS_KEY = kwargs['access'] 
    AWS_SECRET_KEY = kwargs['secret']
        
    #Create S3 Client
    s3_client = create_s3_client(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    print('S3 Client Created...')
    
    #Download Images
    for query in search_queries:
        downloadimages(query)
    
    #Get List of Downloaded Files to Rename
    downloaded_file_list = glob.glob(os.getcwd() + '/downloads/' + search_queries[0] + '/*.jpg')
    
    rename_files(downloaded_file_list)
    print('Files Renamed...')
    
    #Check if bucket exists and create if it doesn't
    for b in s3_client.list_buckets()['Buckets']:
        if b['Name'] == bucket_name:
            exists = True

    if not exists:
        s3_client.create_bucket(Bucket=bucket_name, ACL='public-read')
        print('Bucket: {} created'.format(bucket_name))

    already_uploaded_files = []
    local_file_list = []

    try:
        #Get All objects in S3 Bucket
        for obj in s3_client.list_objects(Bucket=bucket_name)['Contents']:
            already_uploaded_files.append(obj['Key'])
    except KeyError:
        pass

    #Get All Local objects
    for local in glob.glob(os.getcwd() + '/downloads/' + '*.jpg'):
        local_file_list.append(local.split('/')[-1])

    deltas = []

    if len(already_uploaded_files) > len(local_file_list):
        deltas = list(set(already_uploaded_files) - set(local_file_list))
    else:
        deltas = list(set(local_file_list) - set(already_uploaded_files))

    #Upload Delta Files
    for file in deltas:
        file_url = os.getcwd() + '/downloads/' + file
        s3_client.upload_file(Filename=file_url, Bucket=bucket_name, Key=file, ExtraArgs={'ACL': 'public-read'})
        print('Image : {} uploaded to bucket {}'.format(file, bucket_name))

default_args = {
    'owner': 'Abhinav Tiwari',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 5),
    'email': ['abhinavtiwariusa84@gmail.com'],
    'email_on_failure': True
}

dag = DAG('Download_Images',
          schedule_interval='*/10 * * * *',
          default_args=default_args,
		  catchup=False)

t1 = PythonOperator(
    task_id='Download_Google_Images',
    python_callable= main,
    op_kwargs={'bucket':'pgdp-3-bucket-1','access':'AKIAIK2UCAYCZP5GDMBA', 'secret':'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'},
    dag=dag
)

t1