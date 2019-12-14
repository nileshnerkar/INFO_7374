import pandas as pd
from DataPreProcessing import resize_frame
from DataPreProcessing import impute_nulls
from DataPreProcessing import assign_datatypes
from DataIngestor import DataIngestor
from io import BytesIO
from io import StringIO

class DataProcessor:
    # __client = None
    # __Landing_BucketName = None
    # __list_of_files_to_download =[]
    # __Processed_BucketName = None

    def __init__(self, aws_client, Landing_BucketName, Processed_BucketName):
        self.__client = aws_client.client
        self.__Landing_BucketName = Landing_BucketName
        self.__Processed_BucketName = Processed_BucketName
        self.__list_of_files_to_download = []

    def __getNonProcessedObj(self):
        """
            Private method that store the list of files 
            to be processed in __list_of_files_to_download attribute
        """
        landing_objects_list = []
        processed_objects_list = []

        #Get list of Objects in Landing Bucket
        try:
            l = self.__client.list_objects(Bucket=self.__Landing_BucketName)['Contents']
            landing_objects_list = list(pd.DataFrame(l)['Key'])
        except:
            landing_objects_list = []

        #Get list of Objects in Processed Bucket
        try:
            p = __client.list_objects(Bucket=self.__Processed_BucketName)['Contents']
            processed_objects_list = list(pd.DataFrame(p)['Key'])
        except:
            processed_objects_list = []

        #Find file names to download which are not in processed bucket
        if len(landing_objects_list) != 0:
            for object in landing_objects_list:
                if (object not in processed_objects_list) and ('.csv' in object):
                    self.__list_of_files_to_download.append(object)
        else:
            print('No objects in the Landing Bucket')
    

    def process(self):      
        """
            Performs data pre-processing on Files to be processed 
            and uploads processed file S3 bucket [Processed_BucketName]
        """
        self.__getNonProcessedObj()
        i=0
        while i < len(self.__list_of_files_to_download):
            file_name = self.__list_of_files_to_download[i]
            df = pd.read_csv(self.__client.get_object(Bucket= self.__Landing_BucketName, Key=file_name)['Body'], skiprows=1,low_memory=False)

            print('Resizing the data frame...')
            df = resize_frame(df)

            print('Assigning data types...')
            df = assign_datatypes(df)

            print('Imputing Null Values...')
            df = impute_nulls(df)

            print('Uploading to {} S3 Bucket...'.format(self.__Processed_BucketName))
            #upload_to_s3(df,name_of_file=file_name)
            
            s_buff = StringIO()
            df.to_csv(s_buff, index=False)
            
            buff = BytesIO(s_buff.getvalue().encode())
            #buff.seek(0)
            
            # di = DataIngestor(self.__client)
            # di.upload_fileobj(bucketName=self.__Processed_BucketName, key=file_name, data=buff)
            self.__client.upload_fileobj(buff, self.__Processed_BucketName, file_name)
            self.__list_of_files_to_download.remove(file_name)
        
        if self.__list_of_files_to_download:
           print("Problem processing files:", self.__list_of_files_to_download)
        else:
            print("All files processed and uploaded successfully...!") 
