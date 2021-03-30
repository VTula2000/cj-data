# This gets data from Google Analytics api and returns a dataFrame

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os, sys, logging, datetime
import report
import auth

try:
    key_file = auth.auth('cj_data')
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(key_file, SCOPES)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
except:
  print('An exception occurred importing ga_data.py')


# New columns added to table 'Website', 'Section','Subsection','yes_count', 'no_count', 'yes_count_percentage' and 'no_count_percentage'

def get_ga_report(**kwargs):
    def website_section(c,y,x):
        if c == 'England' : 
            try :
                if y == 'Public' :
                    return x.split('/')[1]
                else :
                    return x.split('/')[2]
            except :
                return ''
        else :
            
            try :
                if y == 'Public' :
                    return x.split('/')[2]
                else : 
                    return x.split('/')[3]
            except :
                return ''

    def website_sub_section(c,y,x):
        if c == 'England' : 
            try :
                if y == 'Public' :
                    return x.split('/')[2]
                else :
                    return x.split('/')[3]             
            except :
                return ''
            
        else :
            try :
                if y == 'Public' :
                    return x.split('/')[3]
                else : 
                    return x.split('/')[4]
            except :
                return ''
    view = kwargs['site']
    reporttype = kwargs['type']
    logger = logging.getLogger(__name__)
    period = kwargs['period']
    delta = datetime.timedelta(days=period)
    startDate = datetime.date.today() - delta
    startDate = str(startDate)

    VIEW_ID_DICT = {
    'advisernet':os.environ.get('advisernet_ga'),
    'all':os.environ.get('all_ga'),
    'public':os.environ.get('public_ga')
    }

    VIEW_ID = VIEW_ID_DICT[view]

    rating_body = {
        'reportRequests': [{
            'viewId': VIEW_ID,
            'dateRanges': [{'startDate': startDate, 'endDate': 'yesterday'}],
            'metrics': [{'expression': 'ga:totalEvents'}],
            'dimensions': [
                {'name': 'ga:eventLabel'},
                {'name': 'ga:pagePath'},
                {'name': 'ga:dimension6'}],
            'filtersExpression': ('ga:dimension2!~Start|index;'
            'ga:pagePath!~/about-us/|/local/|/resources-and-tools/|\?;'
            'ga:eventCategory=~pageRating'),
            'orderBys': [{'fieldName': 'ga:totalEvents', 'sortOrder': 'DESCENDING'}],
            'pageSize': 10000
        }]
    }

    size_body = {
        'reportRequests': [{
            'viewId': VIEW_ID,
            'dateRanges': [{'startDate': startDate, 'endDate': 'yesterday'}],
            'metrics': [{'expression': 'ga:pageviews'}],
            'dimensions': [
                {'name': 'ga:pagePath'},
                {'name': 'ga:dimension6'}],
            'filtersExpression': ('ga:dimension2!~Start|index;'
            'ga:pagePath!~/about-us/|/local/|/resources-and-tools/|\?'),
            'orderBys': [{'fieldName': 'ga:pageviews', 'sortOrder': 'DESCENDING'}],
            'pageSize': 10000
            }]
    }

    REPORT_TYPE = {
        'rating':rating_body,
        'size':size_body
    }

    cols_dict = {
        'rating': {'ga:totalEvents':'totalEvents', 'ga:dimension6':'Country', 'ga:pagePath':'pagePath', 'ga:eventLabel':'eventLabel'},
        'size': {'ga:pageviews':'pageviews', 'ga:dimension6':'Country', 'ga:pagePath':'pagePath'}
    }

    report_body = REPORT_TYPE[reporttype]



    response =  analytics.reports().batchGet(
        body= report_body
        ).execute()

    

    cols = cols_dict[reporttype]
    df = pandafy(response)
    df2 = df.rename(index=str, columns=cols)
    df2['Website'] = df2.apply(lambda row:'Advisernet' if 'advisernet' in row['pagePath'] else 
                                   ('BMIS' if 'bmis' in row['pagePath'] else 
                                    ( 'CABlink' if 'cablink' in row['pagePath'] else 'Public')), axis =1 )    
    df2['Section'] = df2.apply(lambda row: website_section(row['Country'],row['Website'],row['pagePath']), axis = 1)
    df2['Subsection'] = df2.apply(lambda row: website_sub_section(row['Country'],row['Website'],row['pagePath']) if 
                                                   website_sub_section(row['Country'], row['Website'],row['pagePath']) 
                                                   else '', axis = 1)
    df2['Section'] = df2.Section.replace('', 'homepage')
    df2['Subsection'] = df2.Subsection.replace('', 'homepage')
    
    if reporttype == 'rating' :
        gbycol = ['Country','pagePath','Website','Section','Subsection']

        df2 = pd.DataFrame({ 'yes_count': df2[df2['eventLabel']=='yes'].groupby(gbycol)['totalEvents'].sum(),
                   'no_count': df2[df2['eventLabel']=='no'].groupby(gbycol)['totalEvents'].sum()
                     }).reset_index()
        df2.yes_count.fillna(0, inplace=True)
        df2.no_count.fillna(0, inplace=True)
        df2['yes_count_percentage'] = (df2['yes_count']/(df2['yes_count'] + df2['no_count']))*100
        df2['no_count_percentage'] = (df2['no_count']/(df2['yes_count'] + df2['no_count']))*100
        
    
        return df2
    else : 
        return df2

# #######################################


def pandafy(response):
    list = []
  # get report data
    for report in response.get('reports', []):
        # set column headers
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])

        for row in rows:
            # create dict for each row
            dict = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            # fill dict with dimension header (key) and dimension value (value)
            for header, dimension in zip(dimensionHeaders, dimensions):
                 dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
            for i, values in enumerate(dateRangeValues):
                for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
                    if ',' in value or '.' in value:
                        dict[metric.get('name')] = float(value)
                    else:
                        dict[metric.get('name')] = int(value)

            list.append(dict)

        df = pd.DataFrame(list)
        return df


# Run the functions in order
if __name__ == '__main__':
    pass
