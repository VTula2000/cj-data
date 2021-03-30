# This gets a csv report from episerver and returns a dataFrame

import pandas as pd
import csv,requests,os,sys,json
from io import StringIO
from datetime import datetime
import auth

auth_json = auth.auth('epi')
username = auth_json['user_name']
password = auth_json['password']
details = 'username=' + username + '&password=' + password
edit_login = auth_json['auth_uri'] + details

public = auth_json['public_report']
advisernet = auth_json['advisernet_report']

feedback30 = 'placeholder'
feedback60 = 'placeholder'

urls = {
    'public' : public,
    'advisernet': advisernet,
    'feedback30': feedback30,
    'feedback60': feedback60
}

#def epi_report(site, *args, **kwargs): 
def epi_report(**kwargs):
    site = kwargs['site'] 
    url = urls[site]    
    auth_json = auth.auth('epi')
    username = auth_json['user_name']
    password = auth_json['password']
    details = 'username=' + username + '&password=' + password
    edit_login = auth_json['auth_uri'] + details

    with requests.Session() as login:
        login.get(edit_login)
        # wrapping next line in a 'with' statement to hopefully reduce failures
        # makes requests release the connection properly when stream = True
        with login.get(url, stream = True) as getting:
            sheet = StringIO(getting.text)

        frame = pd.read_csv(sheet)

        return frame


# New columns added to table to show 'Website', 'Section' and 'Subsection'

def pages_clean(frame):
    def website_section(y,x):
        if y == 'Public' :
            return x.split('/')[1]
        else : 
            return x.split('/')[2]

    def website_sub_section(y,x):
        try :
            if y == 'Public' :
                return x.split('/')[2]
            else : 
                return x.split('/')[3]
        except :
                return ''
    site = 'https://www.citizensadvice.org.uk'
    country_code = dict([
        ('en-GB',''),
        ('en-SCT','/scotland'),
        ('en-NIR','/nireland'),
        ('en-WLS','/wales'),
        ('cy','/cymraeg')
    ])  

    frame['url'] = frame['Language']
    frame['url'] = frame['url'].replace(country_code)
    frame['url'] = site+frame['url']+frame['Path']
    
    #frame.loc[frame['LastAccuracyReview'] == '01/01/0001 00:00:00','LastAccuracyReview'] = None
    # errors = 'coerce', dayfirst = True, format = '%d/%m/%Y'
    frame['LastAccuracyReview'] = pd.to_datetime(frame['LastAccuracyReview'], errors = 'coerce')
    
    #frame.loc[frame['ReviewDate'] == '01/01/0001 00:00:00','ReviewDate'] = None
    frame['ReviewDate'] = pd.to_datetime(frame['ReviewDate'], errors = 'coerce')

    frame['StopPublish'] = pd.to_datetime(frame['StopPublish'], errors = 'coerce')
    frame['StartPublish'] = pd.to_datetime(frame['StartPublish'], errors = 'coerce')
    frame['Changed'] = pd.to_datetime(frame['Changed'], errors = 'coerce')
    frame['Website'] = frame.apply(lambda row:'Advisernet' if 'advisernet' in row['url'] else 
                                   ('BMIS' if 'bmis' in row['url'] else 
                                    ( 'CABlink' if 'cablink' in row['url'] else 'Public')), axis =1 )    
    frame['Section'] = frame.apply(lambda row: website_section(row['Website'],row['Path']), axis = 1)
    frame['Subsection'] = frame.apply(lambda row: website_sub_section(row['Website'],row['Path']) if 
                                                   website_sub_section(row['Website'],row['Path']) 
                                                   else '', axis = 1)
    frame['Section'] = frame.Section.replace('', 'homepage')
    frame['Subsection'] = frame.Subsection.replace('', 'homepage')
    frame.SimpleUrl.fillna('',inplace = True)   
    frame.SimpleUrl = frame.SimpleUrl.astype('str')
    
    

    return frame 


if __name__ == '__main__':
    pass
