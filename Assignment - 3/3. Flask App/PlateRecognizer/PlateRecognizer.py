import json
from AWS.AWSClient import AWSClient
import requests
class PlateRecognizer():
    def __init__(self):

        with open('PlateRecognizer\plateRecognizerConfig.json', 'rb') as config_file:
                config_data = json.load(config_file)

        self.__API_TOKEN=config_data['API_TOKEN']
        self.__api_url=config_data['api_url']
        self.__s3_client = AWSClient().client

    
    def predict(self, bucket, key):
        
        with open('tmp.jpg', 'wb') as data:
            self.__s3_client.download_fileobj(bucket, key, data)
            
        with open('tmp.jpg', 'rb') as data:
            response = requests.post(
                    self.__api_url,
                    files=dict(upload=data),
                    headers={'Authorization': f'Token {self.__API_TOKEN}'}
            )
        return response.json()['results'][0]['plate']
