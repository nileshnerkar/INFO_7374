import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
# from tempfile import mktemp
# import urllib
import requests
import io
from zipfile import ZipFile

class DataExtractor():
    
    email = None
    password = None

    def __init__(self, email, password):
        self.email = email
        self.password = password

    def extractData(self, LOGIN_URL, DOWNLOAD_URL, fileTag):
        """
        Scrapes Data from LendingClub website
        :param LOGIN_URL: (Str) URL for logging in to Lending Club
        :param DOWNLOAD_URL: (Str) URL to download Lending Club data
        :param fileTag: (Str) div tag id where files are listed
        """

        #constant
        path = "Data/DOWNLOAD_LOAN_DATA"
        os.makedirs(path, exist_ok=True)
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.set_headless()

        print('Logging into the site...')
        browser = webdriver.Chrome(executable_path=r'/usr/local/bin/chromedriver', chrome_options=chrome_options)
        browser.get(LOGIN_URL)

        browser.find_element_by_name('email').send_keys(self.email)
        browser.find_element_by_name('password').send_keys(self.password)
        browser.find_element_by_class_name('form-button').click()
        time.sleep(5)

        print('Logged in & moving to Download Page...')

        browser.get(DOWNLOAD_URL)
        fileNamesDiv = browser.find_element_by_id(fileTag)
        
        loanStatsFileList = fileNamesDiv.get_attribute('textContent').split("|")[4:-1]
        initial_path = "https://resources.lendingclub.com/"
        
        existing_files = os.listdir(path)
        for fileName in loanStatsFileList:
            if fileName.split(".zip")[0][7:] not in existing_files:
                fileurl = initial_path+fileName
                print('Downloading file {}'.format(fileName))
                res = requests.get(fileurl)
                res.raise_for_status()
                #print(res.headers)
                if res.ok:
                    thefile = ZipFile(io.BytesIO(res.content))
                    print('Extracting file {}'.format(fileName))
                    thefile.extractall(path)
                    thefile.close()
                else:
                    print(res.status_code)
            else:
                print("File {} Already Exists!".format(fileName))