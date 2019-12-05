import requests
from google.cloud import storage

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pandas_gbq
from pathlib import Path
import os, sys, logging, dotenv
import report

path1 = os.path.dirname(os.path.realpath(__file__))
parentPath = os.path.dirname(path1)
dotenv.load_dotenv(os.path.join(parentPath, '.env'))

def test():
    try:
        parentPath = report.parentPath
        KEY_FILE_LOCATION = os.path.join(parentPath,"creds","cj_data.json")
        
        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

    except:
        print("An exception occurred importing ga_data.py")


#not working, back to basics


if __name__ == '__main__':
    test()