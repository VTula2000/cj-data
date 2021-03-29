# This gets data from Google Search Console api and returns a dataFrame

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import os, sys, logging, string
import report
import auth
import nltk
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer

import datetime


def search_console(startDate,endDate,startRow):
    key_file = auth.auth('cj_data')
    SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(key_file, SCOPES)
    search_console =  build('webmasters', 'v3', credentials=credentials)
    property_uri = 'https://www.citizensadvice.org.uk/'

    # test request to get a list of sites
    site_list = search_console.sites().list().execute()

    # dates are in the format YYYY-MM-DD
    request = {
        'startDate': startDate,
        'endDate': endDate,
        'dimensions': ['query', 'device', 'page', 'date'],
        'searchType': 'web',
        'rowLimit': 25000,
        'startRow': startRow
    }

   
    response = search_console.searchanalytics().query(siteUrl=property_uri, body=request).execute()
    if len(response) > 1:
        rows = response['rows']

        df = pd.DataFrame.from_dict(rows)
        df[['query','device', 'page', 'date']] = pd.DataFrame(df['keys'].values.tolist(), index= df.index)
        result = df.drop(['keys'],axis=1)
        

        return result
    else:
        pass


# New columns added to table 'Website', 'Section' and 'Subsection'

def get_data(**kwargs):
    def website_section(y,x):
        try :
            if y == 'Public' :
                return x.split('/')[3]
            else : 
                return x.split('/')[3]
        except :
            return ''

    def website_sub_section(y,x):
        try :
            if y == 'Public' :
                return x.split('/')[4]
            else : 
                return x.split('/')[4]
        except :
            return ''
    today = datetime.date.today()
    date = today - datetime.timedelta(days=2)
    date = date.strftime('%Y-%m-%d')

    startDate = kwargs.get('startDate',date)
    endDate = kwargs.get('endDate',date)
    startRow = kwargs.get('startRow',0)


    results = pd.DataFrame()
    startRow = 0
    while True:
        frame = search_console(startDate=startDate, endDate=endDate, startRow=startRow)
        frame['report_date'] = pd.to_datetime('today')
        results = results.append(frame)
        #data_to_bq.send_data_bq(frame=frame, name='gsc_fullsite', writeType='WRITE_APPEND')
        startRow += 25000
        if len(frame) < 25000:
            break
        else:
            continue
            
    results['Website'] = results.apply(lambda row:'Advisernet' if 'advisernet' in row['page'] else 
                                   ('BMIS' if 'bmis' in row['page'] else 
                                    ( 'CABlink' if 'cablink' in row['page'] else 'Public')), axis =1 )    
    results['Section'] = results.apply(lambda row: website_section(row['Website'],row['page']), axis = 1)
    results['Subsection'] = results.apply(lambda row: website_sub_section(row['Website'],row['page']) if 
                                                   website_sub_section(row['Website'],row['page']) 
                                                   else '', axis = 1)
    results['Section'] = results.Section.replace('', 'homepage')
    results['Subsection'] = results.Subsection.replace('', 'homepage')
    return results


def clean(frame): 
    frame['query'] = frame['query'].apply(lambda x: x.lower())
    frame['query'] = frame['query'].apply(lambda x: x.translate(string.punctuation))
    frame['query'] = frame['query'].apply(lambda x: x.translate(string.digits))
    frame['tokens'] = frame['query'].apply(lambda x: x.split(" "))
    frame = frame.explode('tokens')

    stop = set(stopwords.words('english'))
    stop.add('uk')
    frame = frame[~frame['tokens'].isin(stop)]

    lemma = WordNetLemmatizer()
    frame['tokens'] = frame['tokens'].apply(lambda x: lemma.lemmatize(x, pos="v"))
    

    return frame

if __name__ == '__main__':
    pass
