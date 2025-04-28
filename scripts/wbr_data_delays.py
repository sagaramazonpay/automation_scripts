import os
from time import sleep
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import Color, PatternFill, Font, Border
from openpyxl.styles import colors
from openpyxl.cell import Cell
import urllib.request as urllib2
import pandas as pd
from datetime import date
from datetime import timedelta 
from datetime import datetime
import win32com.client as win32
import numpy as np
import style as style
import html
from IPython.display import display, HTML
from pandas.io.formats.style import Styler
import base64
from markdown import markdown
#from html import strip_tags
from bs4 import BeautifulSoup
import quipclient as quip
import threading
from pytz import timezone
import sys
login = os.getlogin()
print(login)

import json
import pathlib
import tempfile
import warnings
import requests
import subprocess
from http import cookiejar
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

class MidwayAuthenticator:
    def __init__(self):
        self.session = requests.Session()
        self.kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    
    def _get_midway_cookies(self):
        home_folder = pathlib.Path.home()
        midway_cookie_file = os.path.join(home_folder, ".midway", "cookie")
        
        if not os.path.exists(midway_cookie_file):
            print("Cookie file not found. Running mwinit...")
            subprocess.run(['mwinit', '--no-aea'])
        
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            with open(midway_cookie_file) as midway_file:
                for line in midway_file:
                    if line.startswith("#HttpOnly_"):
                        temp_file.write(line[10:])
                    else:
                        temp_file.write(line)
            temp_file.flush()
            
            cookies = cookiejar.MozillaCookieJar(temp_file.name)
            cookies.load(ignore_discard=True, ignore_expires=True)
        
        os.remove(temp_file.name)
        return cookies
    
    def make_request(self, url, method="get", **kwargs):
        try:
            # Load fresh cookies for each request
            cookies = self._get_midway_cookies()
            response = self.session.request(
                method=method.lower(),
                url=url,
                auth=self.kerberos_auth,
                cookies=cookies,
                verify=False,
                **kwargs
            )
            return response
        except Exception as e:
            print(f"Request failed: {str(e)}")
            return None

midway_auth = MidwayAuthenticator()

def make_request(url, method="get", **kwargs):
    return midway_auth.make_request(url, method, **kwargs)

def verify_midway_auth(url):
    global midway_auth  # Access the global authenticator instance
    
    while True:
        response = make_request(url)
        conn_code = response.status_code
        print(f"Status code: {conn_code}")
        
        if conn_code == 200:
            print("Midway authenticated successfully!")
            return response
        else:
            print("Authentication failed. Running mwinit...")
            subprocess.run(['mwinit', '--no-aea'])
            # Create a new authenticator instance with fresh session and cookies
            midway_auth = MidwayAuthenticator()
            print("Retrying authentication...")
login = os.getlogin()
print(login)

URL = "https://phonetool.amazon.com/users/search?query={name}"
url = URL.format(name=login)

response = verify_midway_auth(url)
print(response.json())

# Parse the JSON data
data = {
  "events": [
    {
      "datetime": 1741699694,
      "detail": "null",
      "errorCode": "RS-XX000",
      "eventType": "UNKNOWN",
      "exceptionClass": "null",
      "hostName": "bdt-redshift-load-1e1-3808ad04",
      "level": 40000,
      "locationClass": "com.amazon.dw.om.DWPJobRun",
      "locationFile": "DWPJobRun.java",
      "locationLine": "970",
      "locationMethod": "logEvent",
      "objectId": 10730044783,
      "objectType": "JOB_RUN",
      "summary": "Job exceeded hella restart limit for error, RS-XX000: INTERNAL ERROR",
      "userId": "null"
    },
    {
      "datetime": 1741699694,
      "detail": "RS-XX000: com.amazon.dw.om.ExecutionException: java.sql.SQLException: ERROR: 1023\n  Detail: Serializable isolation violation on table - 26416290, transactions forming the cycle are: 403386940, 403444165, 403375465 (pid:1073812017)\n\tat com.amazon.dw.om.DWPLoadJobRun.execute(DWPLoadJobRun.java:312)\n\tat com.amazon.dw.jobdaemon.DWPRunnableStateWorkerThread.performTask(DWPRunnableStateWorkerThread.java:104)\n\tat com.amazon.dw.jobdaemon.DWPWorkerThread.run(DWPWorkerThread.java:65)\nCaused by: RS-XX000: com.amazon.dw.om.ExecutionException: java.sql.SQLException: ERROR: 1023\n  Detail: Serializable isolation violation on table - 26416290, transactions forming the cycle are: 403386940, 403444165, 403375465 (pid:1073812017)\n\tat com.amazon.dw.om.DatabaseErrorHandler.handleCompleteError(DatabaseErrorHandler.java:547)\n\tat com.amazon.dw.om.LoadAdapter.processExceptions(LoadAdapter.java:1114)\n\tat com.amazon.dw.om.LoadAdapterForRedshift.executeLoad(LoadAdapterForRedshift.java:334)\n\tat com.amazon.dw.om.LoadAdapter.execute(LoadAdapter.java:145)\n\tat com.amazon.dw.om.DWPLoadJobRun.execute(DWPLoadJobRun.java:265)\n\t... 2 more\nCaused by: java.sql.SQLException: ERROR: 1023\n  Detail: Serializable isolation violation on table - 26416290, transactions forming the cycle are: 403386940, 403444165, 403375465 (pid:1073812017)\n\tat com.amazon.dw.om.LoadAdapter.executeSQL(LoadAdapter.java:803)\n\tat com.amazon.dw.om.LoadAdapterForRedshift.executeLoad(LoadAdapterForRedshift.java:276)\n\t... 4 more\nCaused by: com.amazon.redshift.util.RedshiftException: ERROR: 1023\n  Detail: Serializable isolation violation on table - 26416290, transactions forming the cycle are: 403386940, 403444165, 403375465 (pid:1073812017)\n\tat com.amazon.redshift.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2607)\n\tat com.amazon.redshift.core.v3.QueryExecutorImpl.processResultsOnThread(QueryExecutorImpl.java:2275)\n\tat com.amazon.redshift.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1880)\n\tat com.amazon.redshift.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1872)\n\tat com.amazon.redshift.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:368)\n\tat com.amazon.redshift.jdbc.RedshiftStatementImpl.executeInternal(RedshiftStatementImpl.java:514)\n\tat com.amazon.redshift.jdbc.RedshiftStatementImpl.execute(RedshiftStatementImpl.java:435)\n\tat com.amazon.redshift.jdbc.RedshiftStatementImpl.executeWithFlags(RedshiftStatementImpl.java:376)\n\tat com.amazon.redshift.jdbc.RedshiftStatementImpl.executeCachedSql(RedshiftStatementImpl.java:362)\n\tat com.amazon.redshift.jdbc.RedshiftStatementImpl.executeWithFlags(RedshiftStatementImpl.java:339)\n\tat com.amazon.redshift.jdbc.RedshiftStatementImpl.executeUpdate(RedshiftStatementImpl.java:297)\n\tat sun.reflect.GeneratedMethodAccessor129.invoke(Unknown Source)\n\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n\tat java.lang.reflect.Method.invoke(Method.java:498)\n\tat com.amazon.bi.commons.sql.StatementTransformers$3.invoke(StatementTransformers.java:140)\n\tat com.sun.proxy.$Proxy94.executeUpdate(Unknown Source)\n\tat com.amazon.dw.om.LoadAdapter.executeQueryWithUpdateCount(LoadAdapter.java:860)\n\tat com.amazon.dw.om.LoadAdapter.executePrefixQuery(LoadAdapter.java:847)\n\tat com.amazon.dw.om.LoadAdapter.executeLoadSQL(LoadAdapter.java:823)\n\tat com.amazon.dw.om.LoadAdapter.executeSQL(LoadAdapter.java:768)\n\t... 5 more\n",
      "errorCode": "RS-XX000",
      "eventType": "EXCEPTION",
      "exceptionClass": "null",
      "hostName": "bdt-redshift-load-1e1-3808ad04",
      "level": 40000,
      "locationClass": "com.amazon.dw.om.DWPJobRun",
      "locationFile": "DWPJobRun.java",
      "locationLine": "975",
      "locationMethod": "logEvent",
      "objectId": 10730044783,
      "objectType": "JOB_RUN",
      "summary": "Exception for error: RS-XX000",
      "userId": "null"
    },
    {
      "datetime": 1741699688,
      "detail": "One of the possible reasons for ERROR could be Schema Mismatch, For more info please refer wiki -https://w.amazon.com/index.php/Execution%20Schema%20Mismatch%20related%20Errors",
      "errorCode": "DWP-99999",
      "eventType": "UNKNOWN",
      "exceptionClass": "null",
      "hostName": "bdt-redshift-load-1e1-3808ad04",
      "level": 40000,
      "locationClass": "com.amazon.dw.om.DWPJobRun",
      "locationFile": "DWPJobRun.java",
      "locationLine": "988",
      "locationMethod": "logEventWithDetails",
      "objectId": 10730044783,
      "objectType": "JOB_RUN",
      "summary": "Possible reason for ERROR could be Schema Mismatch",
      "userId": "null"
    },
    {
      "datetime": 1741682117,
      "detail": "ERROR: 1023 Detail: Serializable isolation violation on table - 26416290",
      "errorCode": "null",
      "eventType": "CREATE",
      "exceptionClass": "null",
      "hostName": "dw-jobgen-prod-1a-5f5366c8.us-east-1.amazon.com",
      "level": 20000,
      "locationClass": "com.amazon.cascades.orchestrator.sdk.jobruns.LegacyJobRunEvents",
      "locationFile": "null",
      "locationLine": "null",
      "locationMethod": "postEvent",
      "objectId": 10730044783,
      "objectType": "JOB_RUN",
      "summary": "Automatically resubmitted. Old job run in Error with id 10729236017",
      "userId": 4163881
    }
  ],
  "requestId": "null"
}

# Count the number of events
event_count = len(data["events"])

print(f"Number of events: {event_count}")

# Search for events with "Old job run" in summary
for event in data["events"]:
    if "Old job run" in event["summary"]:
        print("Found matching event:")
        print(f"Summary: {event['summary']}")
        print(f"DateTime: {event['datetime']}")
        print(f"Event Type: {event['eventType']}")
        print(f"Object ID: {event['objectId']}")

old_job_events = [event for event in data["events"] if "old job run".lower() in event["summary"].lower()]
if old_job_events:
    print("\nUsing list comprehension:")
    print(f"Found {len(old_job_events)} event(s) with 'Old job run' in summary")
    for event in old_job_events:
        print(f"Summary: {event['summary']}")


# QUIP INPUT STARTS
baseurl = 'https://platform.quip-amazon.com'
access_token = "UFRGOU1BWEZlQTQ=|1763527256|SQHi+b9hge2suD1/dY40J82nzcID4L1+iDm7TIVsYjA=" 
thread_id = 'PvwNA3gSmj88'
client = quip.QuipClient(access_token = access_token, base_url=baseurl)
rawdictionary = client.get_thread(thread_id)
dfs=pd.read_html(rawdictionary['html'])

df_links_core = dfs[0]
weekly_df = dfs[-1]
print('Printing dfs')
print(dfs)
# raw_df = dfs[-1]
# raw_df.columns=raw_df.iloc[0]
# raw_df=raw_df.iloc[1:,1:] 
# raw_df=raw_df.replace('\u200b', np.nan)
# raw_df = raw_df.dropna(how='all')
# raw_df = raw_df.dropna(axis=1,how='all')
# print(raw_df)
# print(raw_df.info())
print("All dfs printed")
print("Printing Sheet 1\n")
print(df_links_core)
print("Printing Sheet 2\n")
print(weekly_df)
df_links_core.columns=df_links_core.iloc[0]
df_links_core=df_links_core.iloc[1:,1:]
df_links_core=df_links_core.replace('\u200b', np.nan)
df_links_core = df_links_core.dropna(how='all')
df_links_core = df_links_core.dropna(axis=1,how='all')
df_links = df_links_core[['link','name']].drop_duplicates()
linkdf = df_links
weekly_df.columns=weekly_df.iloc[0] 
weekly_df=weekly_df.iloc[1:,1:] 
weekly_df=weekly_df.replace('\u200b', np.nan) 
weekly_df = weekly_df.dropna(how='all')
weekly_df = weekly_df.dropna(axis=1,how='all')
weekly_df = weekly_df.drop_duplicates()
weekly_df['Date'] = pd.to_datetime(weekly_df['Date'], format='mixed')
weekly_df.rename(columns={'Elapsed Time (Hrs)':'Elapsed_Hours'},inplace=True)
weekly_df.rename(columns={'Benchmark Data Availability (Hrs)':'SLA_Hours'},inplace=True)


print("Printing main link Table \n")
print(df_links)
print(df_links.info())
print("Printing weekly delayed Tables \n")
print(weekly_df)
print(weekly_df.info())
df_links.to_excel("links.xlsx")
weekly_df.to_excel("linkswbr_org.xlsx")

thread_id2 = 'iKqHAWP6VJ0l'
rawdictionary = client.get_thread(thread_id2)
dfs_2=pd.read_html(rawdictionary['html'])
df_links_p0 = dfs_2[0]
df_links_p0.columns=df_links_p0.iloc[0]
df_links_p0=df_links_p0.iloc[1:,1:]
df_links_p0=df_links_p0.replace('\u200b', np.nan)
df_links_p0 = df_links_p0.dropna(how='all')
df_links_p0 = df_links_p0.dropna(axis=1,how='all')
df_links_p0 = df_links_p0[['link','name']].drop_duplicates()
df_agg_jobs = dfs_2[6]
df_agg_jobs.columns=df_agg_jobs.iloc[0]
df_agg_jobs=df_agg_jobs.iloc[1:,1:]
df_agg_jobs=df_agg_jobs.replace('\u200b', np.nan)
df_agg_jobs = df_agg_jobs.dropna(how='all')
df_agg_jobs = df_agg_jobs.dropna(axis=1,how='all')
df_agg_jobs = df_agg_jobs[['TABLE_NAME']].drop_duplicates()
tables_to_exclude = df_agg_jobs['TABLE_NAME'].tolist()

linksdf_2 = df_links_p0[~df_links_p0['name'].str.endswith('[L]')].copy()
linksdf_2['name'] = linksdf_2['name'].apply(lambda x: x.replace(' [E]', '') if x.endswith('[E]') else x.strip())
linksdf_2 = linksdf_2[['link','name']].drop_duplicates()
linksdf_2.to_excel("linksdf_2.xlsx")
links_df = pd.concat([linksdf_2, linkdf[~linkdf['name'].isin(linksdf_2['name'])]])
links_df.to_excel("links.xlsx")

print("\nPrinting AGG Jobs\n",df_agg_jobs)
# QUIP INPUT ENDS

linkswbr = pd.read_excel("linkswbr_org.xlsx")


# 1. Remove tables with 'AGG' except AGG_FRAUD_CX_METRICS
filtered_df = linkswbr[
    ~(linkswbr['Table'].str.contains('AGG', case=False) & 
      (linkswbr['Table'] != 'AGG_FRAUD_CX_METRIC'))
]

# # 2. Remove specific tables
# filtered_df = filtered_df[
#     ~filtered_df['Table'].isin(['EUC_CLM_LIFECYCLE_PHASE3', 'FINANCING_COMP_WT','FINANCING_NCE_GV_WT'])
# ]

# If the column name is different, like 'table_name'
filtered_df = filtered_df[~filtered_df['Table'].isin(tables_to_exclude)]


# 3. Remove where Elapsed_Hours - SLA_Hours <= 4
filtered_df = filtered_df[
    ~(filtered_df['Elapsed_Hours'] - filtered_df['SLA_Hours'] <= 4)
]

# 4. For DIGITS and SANDBOX schemas, remove where SLA=12 and Elapsed<=24
filtered_df = filtered_df[
    ~((filtered_df['Schema'].isin(['DIGITS', 'SANDBOX'])) & 
      (filtered_df['SLA_Hours'] == 12) & 
      (filtered_df['Elapsed_Hours'] <= 24))
]

# Display results
print("Printing org weekly file")
print(linkswbr)
print("Printing filtered weekly file\n")

print(filtered_df)
filtered_df.to_excel("linkswbr.xlsx")


filedate = date.today()
print(filedate)

team = 0

user_input = None

def get_user_input():
    global user_input
    user_input = input("Send mail to 1. Team, 2. Self: ")

# Start the input thread
input_thread = threading.Thread(target=get_user_input)
input_thread.daemon = True  # Allows program to exit even if thread is running
input_thread.start()

# Wait for the input for 10 seconds
input_thread.join(timeout=10)

# Check if user input was provided; if not, set default value
if user_input is None:
    user_input = '2'  # Default value

# Handle the user input
# if user_input in ['1', '2']:
#     variable = user_input  # Set your desired variable here based on input
#     print(f"You entered: {variable}")
# else:
#     print("Invalid input, defaulting to 2.")
#     variable = '2'  # Set variable to default if invalid input
if user_input is None:
        print("\nNo input provided, defaulting to 1.")
        variable = '1'  # Default value
# Handle the user input
elif user_input in ['1', '2']:
    variable = user_input  # Set your desired variable here based on input
    print(f"\nYou entered: {variable}")
else:
    print("\nInvalid input, defaulting to 1.")
    variable = '1'  # Set variable to default if invalid input

teamss = variable

if team==0:
    a="linkswbr"
else:
    a="linkswbr"
if team==0:
    b="links"
else:
    b="links"

wbr = pd.read_excel(str(a)+".xlsx")
wbr_l = pd.read_excel(str(a)+".xlsx")
print(wbr)
print(wbr_l)
wbr['mart'] = wbr['Schema'].str.strip().str.upper() + '.' + wbr['Table'].str.strip().str.upper()
wbr_l['mart'] = wbr_l['Schema'].str.strip().str.upper() + '.' + wbr_l['Table'].str.strip().str.upper()
wbr = wbr[['mart','Date','SLA_Hours','Elapsed_Hours']]
wbr_l = wbr_l[['mart','Date','SLA_Hours','Elapsed_Hours']]
print(wbr)
print(wbr_l)


wbr_l['Date'] = wbr_l['Date'].astype(str)
wbr_l['Elapsed_Hours'] = wbr_l['Elapsed_Hours'].astype(str)
wbr_l['SLA_Hours'] = wbr_l['SLA_Hours'].astype(str)


print(wbr_l)
wbr.rename(columns={'Date':'Date_Wbr'},inplace=True)
wbr = wbr [['mart','Date_Wbr']]
pf = pd.read_excel(str(b)+".xlsx")
print('weekly table')
wbr  = wbr.drop_duplicates()
wbr['Date_Wbr'] = wbr['Date_Wbr'].astype(str)
print(wbr)
print(wbr.dtypes)
print('our table')
print(pf)

wbr_nd = wbr.groupby('mart')['Date_Wbr'].agg([('min_wbr_date','min'),('max_wbr_date','max')])
wbr_nd['min_wbr_date'] = wbr_nd['min_wbr_date'].astype(str)
wbr_nd['max_wbr_date'] = wbr_nd['max_wbr_date'].astype(str)
wbr_nd['index'] = range(len(wbr_nd))
print(wbr_nd)
df1 = pd.merge(pf,wbr_nd,how='inner',left_on='name',right_on='mart')
df1 = df1[['link','name','min_wbr_date','max_wbr_date']]
df1 = df1.sort_values(by=['name'], ascending=[True]).reset_index(drop=True)
df1.to_excel("df1_toexcel.xlsx")
df1 = pd.read_excel("df1_toexcel.xlsx")
print('Print 1st List')
print('Printing total matched count')
print(df1.count())
print(df1)
print(df1[['name','min_wbr_date','max_wbr_date']])
print(df1.dtypes)
print(wbr_l)
upstream = wbr_l[~wbr_l['mart'].isin(df1['name'])]
print(upstream)
upstream = upstream.sort_values(by=['Date'], ascending=[True]).reset_index(drop=True)

upstream.rename(columns={'mart':'Delayed Table'},inplace=True)
upstream.rename(columns={'Date':'Date_Wbr'},inplace=True)
print(upstream)
upstream.to_excel("upstream_"+str(filedate)+"report.xlsx")
upstream = pd.read_excel("upstream_"+str(filedate)+"report.xlsx")
upstream['Elapsed_Hours'] = upstream['Elapsed_Hours'].astype(str)
upstream['SLA_Hours'] = upstream['SLA_Hours'].astype(str)

df_dates_w = upstream.groupby('Delayed Table')['Date_Wbr'].apply(','.join).reset_index()
print("ku6 bhi")
print("ku6 bhi")
print(upstream)

df_dates_elapsed = upstream.groupby('Delayed Table')['Elapsed_Hours'].apply(','.join).reset_index()
df_dates_w = pd.merge(df_dates_w,df_dates_elapsed, on='Delayed Table')
df_dates_sla = upstream.groupby('Delayed Table')['SLA_Hours'].apply(','.join).reset_index()
df_dates_w = pd.merge(df_dates_w,df_dates_sla, on='Delayed Table')


df_dates_w['Frequency'] = upstream.groupby('Delayed Table').size().reset_index(name='count')['count']
df_dates_w['Sl No.'] = df_dates_w.index + 1

upstream = df_dates_w[['Sl No.','Delayed Table','Date_Wbr','Frequency','SLA_Hours','Elapsed_Hours']]
print("Upstream printed")
print(upstream)
print("downstream printed")
print(df1[['name','min_wbr_date','max_wbr_date']])


# API connection with midway authentication starts
URL = "https://phonetool.amazon.com/users/search?query={name}"
url = URL.format(name=login)
#url = "https://datacentral.a2z.com/dw-platform/servlet/dwp/template/DWPJobPerformanceHistory.vm/job_id/26339173"
response = make_request(url)
print(response.json())
data = response.json()
full_id = data[0]['id']

mailname = full_id.split(' (')[0]
print(mailname)
# API connection with midway authentication ends


i=0
df1_app = pd.DataFrame()






#Enter FOR loop1
for index, row in df1.iterrows():
    name = row['name']
    link = row['link']
    min_date = row['min_wbr_date']
    max_date = row['max_wbr_date']
    i+=1
    print(i)
    response = make_request(link)
    soup = BeautifulSoup(response.content, 'html.parser')


    # Getting the table
    table = soup.find('table', class_='tablesorter')
    table_data = pd.read_html(str(table))
    table_rows = table_data[0]


    table_rows = table_rows[(table_rows['Dataset Date']>=min_date) & (table_rows['Dataset Date']<=max_date)]
    table_rows['Name'] = name
    table_rows['Link'] = link
    table_rows['Sl No'] = i
    table_rows['Min_Date'] = min_date
    table_rows['Max_Date'] = max_date
    table_rows.rename(columns={'Start Date (PST)':'Start_PST'},inplace=True)
    table_rows = table_rows[['Sl No','Name','Link','Job Run','Dataset Date','Status','Database','Min_Date','Max_Date','Start_PST']]
    print(table_rows)
    print(table_rows.dtypes)

    df1_app_f = pd.concat([table_rows],ignore_index=True)
    df1_app = df1_app._append(df1_app_f,ignore_index=True)
#Came out of FOR loop1
print("Printing FOR loop 1 Resultant Table")    
print(df1_app)
print("Printing 1st Table Again")    
print(df1)

df1_app.to_excel("df1_app_wbr.xlsx",index=False)







df1_app = pd.read_excel("df1_app_wbr.xlsx")
#df1_app = pd.read_excel("df1_app.xlsx")
df2 = pd.merge(wbr,df1_app,how='inner',left_on=['mart','Date_Wbr'],right_on=['Name','Dataset Date'])
df2['JobRunLink'] = "https://datacentral.a2z.com/console?action=jobrun_details&jobrun_id=" + df2['Job Run'].astype(str)
df2['JobRunLink2'] = "https://datacentral.a2z.com/console?action=jobrun_details&jobrun_id=" + df2['Job Run'].astype(str) + "#dependencies"
df2 = df2[['JobRunLink','Job Run','Date_Wbr','Sl No','Name','Link','Dataset Date','Status','Database','Min_Date','Max_Date','JobRunLink2','Start_PST']]
df2 = df2.sort_values(by=['Name'], ascending=[True]).reset_index(drop=True)
print('Print 2nd Table')
print(df2)
a = df2.columns[-1]
print(a)
print(df2.dtypes)

df2.to_excel("df2_wbr.xlsx",index=False)
df2 = pd.read_excel("df2_wbr.xlsx")
df2['NameDATE'] = df2['Name']+df2['Date_Wbr'].astype(str)

j=0
df2_app = pd.DataFrame()
df2_execption = pd.DataFrame()
#Enter FOR loop2
for index, row in df2.iterrows():
    try:    
        name = row['Name']
        link_j = row['JobRunLink']
        link_j2 = row['JobRunLink2']

        wbrdate = row['Date_Wbr']
        link_h = row['Link']
        start_pst = row['Start_PST']
        if not start_pst:
            pst = timezone('America/Los_Angeles')
            start_pst = datetime.now(pst)
            print(f"Start_PST is found null hence current PST timestamp is taken into account: {start_pst}")
        else:
            start_pst = datetime.strptime(start_pst, '%Y/%m/%d %H:%M:%S')
            print(f"Start_PST is found and is taken into account: {start_pst}")


        j+=1
        print(j)
        # Navigating to the status webpage via the link
        response2 = make_request(link_j2)
        soup2 = BeautifulSoup(response2.content, 'html.parser')
        # Getting the table
        #table_dependencies_heading_file = soup2.find('h4', text='Data File Dependencies')
        table_dependencies_heading_table = soup2.find('h4', text='Table Load Dependencies')
        # if table_dependencies_heading_file:
        #     table = table_dependencies_heading_file.find_next('table')  # Find the table after the heading
        #     df = pd.read_html(str(table))[0]  # This directly extracts the table into a dataframe
        #     print(df)
        if table_dependencies_heading_table:
            table = table_dependencies_heading_table.find_next('table')  # Find the table after the heading
            df = pd.read_html(str(table))[0]  # This directly extracts the table into a dataframe
            print(df)
        else:
            df = pd.DataFrame()
            print("Data File Dependencies and Table Load Dependencies sections not found")
        table_rowsn = df
        table_rows2 = table_rowsn
        print(table_rowsn)
        print(table_rowsn.dtypes)
    
        b = table_rows2.columns[-1]
        print(b)
        table_rows2.rename(columns={b:'LastChecked'},inplace=True)
        table_rows2.rename(columns={'Table':'Causing Table'},inplace=True)
        # table_rows2.rename(columns={b:'LastChecked'},inplace=True)


        #table_rows2 = table_rows2['Valid?']
        # table_rows2 = table_rows2[table_rows2['Valid?'] == 'true']
        # if 'false'  not in table_rows2['Valid?'].values:
        #     ok = 'ok'
        print("Print Horiya Hein")
        print(table_rows2.dtypes)
        print(table_rows2['Valid?'])
        max_d = table_rows2['LastChecked'].max()
        count_max = (table_rows2['LastChecked'] == max_d).sum()
        count_false = (table_rows2['Valid?'] == False).sum()
        print('Count max= ',count_max,'\n')
        print('Count false= ',count_false,'\n')
        print(f"\nPrinting max of last checked: {max_d}")
        max_d_cal = datetime.strptime(max_d, '%Y-%m-%d %H:%M:%S')
        difference = start_pst - max_d_cal
        diff_minutes = difference.total_seconds() / 60
        diff_minutes = round(diff_minutes, 3)


        
        # (table_rows2['Valid?'].str.upper() == 'TRUE').all()
        if count_false == 0 & count_max == 1:
            
            table_rows2 = table_rows2[table_rows2['LastChecked'] == max_d]
            print("Printing for loop2 inside table2")
            print(table_rows2)
            print("Printing for loop2 inside table2 head 1")
            # table_rows2 = table_rows2.head(1)
            found_name  = table_rows2['Causing Table'].values[0]
            datede = table_rows2['Dataset Date'].values[0]
            dbs = table_rows2['DB'].values[0]
            fname2 = found_name.replace('ANDES.', '') if found_name.startswith('ANDES.') else found_name
            fname3 = fname2 + str(datede)
            print(str(found_name + datede + dbs))
            k=0
            #print(str(found_name+datede+dbs))
            print(table_rows2)
            while found_name  in pf['name'].values and fname3 not in df2['NameDATE'].values:
                k+=1
                new_df_output = pf[pf['name'] == found_name]
                #new_df_output = pd.DataFrame({'Name': new_filtered_df['name'], 'link': new_filtered_df['link']})
                # print(new_filtered_df)
                print(new_df_output)

                response3 = make_request(new_df_output['link'].values[0])
                soup3 = BeautifulSoup(response3.content, 'html.parser')
                # Find the table you're interested in
                table3 = soup3.find('table', class_='tablesorter')
                # Extract the table data using pandas
                table_data3 = pd.read_html(str(table3))
                # Display the table data
                table_rows3 = table_data3[0]



                table_rows3 = table_rows3[(table_rows3['Dataset Date']==datede) & (table_rows3['Database']==dbs)]
                table_rows3 = table_rows3.head(1)
                table_rows3['Name'] = found_name
                table_rows3['Link'] = new_df_output['link'].values[0]
                table_rows3['Sl No'] = k
                table_rows3['Date'] = datede
                table_rows3 = table_rows3[['Sl No','Name','Link','Job Run','Dataset Date','Status','Database']]
                print(table_rows3)
                print(table_rows3.dtypes)
                jobid = str(table_rows3['Job Run'].values[0])

                link_n = "https://datacentral.a2z.com/console?action=jobrun_details&jobrun_id=" + jobid
                response2 = make_request(link_n)
                soup2 = BeautifulSoup(response2.content, 'html.parser')
                
                table_dependencies_heading_table = soup2.find('h4', text='Table Load Dependencies')
                if table_dependencies_heading_table:
                    table = table_dependencies_heading_table.find_next('table')  # Find the table after the heading
                    df = pd.read_html(str(table))[0]  # This directly extracts the table into a dataframe
                    print(df)
                else:
                    df = pd.DataFrame()
                    print("Data File Dependencies and Table Load Dependencies sections not found")
                table_rowsn = df
                table_rows4 = table_rowsn
                
                count_false4 = (table_rows4['Valid?'] == False).sum()
                if count_false4 == 0:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    dfl3 = table_rows4.sort_values(by=['Last Checked (PDT)','flag_s','flag_d','Table'], ascending=[False,True,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
                else:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    table_rows4['flag_v'] = table_rows4['Valid?'] == False
                    dfl3 = table_rows4.sort_values(by=['flag_v','flag_s','flag_d'], ascending=[False,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
            table_rows2['Causing Table'].values[0] = found_name
            table_rows2['Dataset Date'].values[0] = datede
            table_rows2['DB'].values[0] = dbs


            print (found_name)
            print(table_rows2)

            table_rows2['Table Name'] = name
            table_rows2['JobRunLink'] = link_j
            table_rows2['JobHistoryLink'] = link_h
            table_rows2['Sl No.'] = j
            table_rows2['Date_Wbr'] = wbrdate
            table_rows2['Is_FE'] = 0
            table_rows2['Is_LCE'] = 0
            table_rows2['WFR_MINUTES'] = diff_minutes
            table_rows2 = table_rows2[['Sl No.','Table Name','Date_Wbr','Causing Table','Dataset Date','LastChecked','JobRunLink','JobHistoryLink','DB','Is_FE','Is_LCE','WFR_MINUTES']]
            #table_rows2 = table_rows2[['1/2','Last Checked (PDT)',Name','Link','Job Run','Dataset Date','Status','Database']]
            print(table_rows2)
            df2_app_f = pd.concat([table_rows2],ignore_index=True)
            df2_app = df2_app._append(df2_app_f,ignore_index=True)
        elif count_false == 1:
            table_rows2 = table_rows2[table_rows2['Valid?'] == False]
            print("Printing for loop2 inside table2")
            print(table_rows2)
            print("Printing for loop2 inside table2 false 1")
            found_name  = table_rows2['Causing Table'].values[0]
            datede = table_rows2['Dataset Date'].values[0]
            dbs = table_rows2['DB'].values[0]
            fname2 = found_name.replace('ANDES.', '') if found_name.startswith('ANDES.') else found_name
            fname3 = fname2 + str(datede)
            print(str(found_name + datede + dbs))
            k=0
            #print(str(found_name+datede+dbs))
            print(table_rows2)
            while found_name  in pf['name'].values and fname3 not in df2['NameDATE'].values:
                k+=1
                new_df_output = pf[pf['name'] == found_name]
                #new_df_output = pd.DataFrame({'Name': new_filtered_df['name'], 'link': new_filtered_df['link']})
                # print(new_filtered_df)
                print(new_df_output)
                response3 = make_request(new_df_output['link'].values[0])
                soup3 = BeautifulSoup(response3.content, 'html.parser')
                # Find the table you're interested in
                table3 = soup3.find('table', class_='tablesorter')
                # Extract the table data using pandas
                table_data3 = pd.read_html(str(table3))
                # Display the table data
                table_rows3 = table_data3[0]
                jobid = str(table_rows3['Job Run'].values[0])

                link_n = "https://datacentral.a2z.com/console?action=jobrun_details&jobrun_id=" + jobid
                response2 = make_request(link_n)
                soup2 = BeautifulSoup(response2.content, 'html.parser')
                
                table_dependencies_heading_table = soup2.find('h4', text='Table Load Dependencies')
                if table_dependencies_heading_table:
                    table = table_dependencies_heading_table.find_next('table')  # Find the table after the heading
                    df = pd.read_html(str(table))[0]  # This directly extracts the table into a dataframe
                    print(df)
                else:
                    df = pd.DataFrame()
                    print("Data File Dependencies and Table Load Dependencies sections not found")
                table_rowsn = df
                table_rows4 = table_rowsn
                count_false4 = (table_rows4['Valid?'] == False).sum()
                if count_false4 == 0:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    dfl3 = table_rows4.sort_values(by=['Last Checked (PDT)','flag_s','flag_d','Table'], ascending=[False,True,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
                else:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    table_rows4['flag_v'] = table_rows4['Valid?'] == False
                    dfl3 = table_rows4.sort_values(by=['flag_v','flag_s','flag_d'], ascending=[False,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
            table_rows2['Causing Table'].values[0] = found_name
            table_rows2['Dataset Date'].values[0] = datede
            table_rows2['DB'].values[0] = dbs


            print (found_name)
            print(table_rows2)

            table_rows2['Table Name'] = name
            table_rows2['JobRunLink'] = link_j
            table_rows2['JobHistoryLink'] = link_h
            table_rows2['Sl No.'] = j
            table_rows2['Date_Wbr'] = wbrdate
            table_rows2['Is_FE'] = 0
            table_rows2['Is_LCE'] = 0
            table_rows2['WFR_MINUTES'] = 0
            table_rows2 = table_rows2[['Sl No.','Table Name','Date_Wbr','Causing Table','Dataset Date','LastChecked','JobRunLink','JobHistoryLink','DB','Is_FE','Is_LCE','WFR_MINUTES']]
            #table_rows2 = table_rows2[['1/2','Last Checked (PDT)',Name','Link','Job Run','Dataset Date','Status','Database']]
            print(table_rows2)
            df2_app_f = pd.concat([table_rows2],ignore_index=True)
            df2_app = df2_app._append(df2_app_f,ignore_index=True)
        elif count_false>=2:
            #table_rows2 = table_rows2[table_rows2['Valid?'].str.upper() == 'FALSE']
            table_rows2['flag'] = table_rows2['Causing Table'].str.upper().str.startswith(('DIGITS', 'SANDBOX'))
            table_rows2['flag_v'] = table_rows2['Valid?'] == False
            table_rows2 = table_rows2.sort_values(by=['flag_v','flag'], ascending=[False,True])
            print("Printing for loop2 inside table2")
            print(table_rows2)
            print("Printing for loop2 inside table2 false 1")
            table_rows2 = table_rows2.head(1)
            found_name  = table_rows2['Causing Table'].values[0]
            datede = table_rows2['Dataset Date'].values[0]
            fname2 = found_name.replace('ANDES.', '') if found_name.startswith('ANDES.') else found_name
            fname3 = fname2 + str(datede)
            dbs = table_rows2['DB'].values[0]
            print(str(found_name + datede + dbs))
            k=0
            #print(str(found_name+datede+dbs))
            print(table_rows2)
            while found_name  in pf['name'].values and fname3 not in df2['NameDATE'].values:
                k+=1
                new_df_output = pf[pf['name'] == found_name]
                #new_df_output = pd.DataFrame({'Name': new_filtered_df['name'], 'link': new_filtered_df['link']})
                # print(new_filtered_df)
                print(new_df_output)
                response3 = make_request(new_df_output['link'].values[0])
                soup3 = BeautifulSoup(response3.content, 'html.parser')
                # Find the table you're interested in
                table3 = soup3.find('table', class_='tablesorter')
                # Extract the table data using pandas
                table_data3 = pd.read_html(str(table3))
                # Display the table data
                table_rows3 = table_data3[0]
                jobid = str(table_rows3['Job Run'].values[0])

                link_n = "https://datacentral.a2z.com/console?action=jobrun_details&jobrun_id=" + jobid
                response2 = make_request(link_n)
                soup2 = BeautifulSoup(response2.content, 'html.parser')
                
                table_dependencies_heading_table = soup2.find('h4', text='Table Load Dependencies')
                if table_dependencies_heading_table:
                    table = table_dependencies_heading_table.find_next('table')  # Find the table after the heading
                    df = pd.read_html(str(table))[0]  # This directly extracts the table into a dataframe
                    print(df)
                else:
                    df = pd.DataFrame()
                    print("Data File Dependencies and Table Load Dependencies sections not found")
                table_rowsn = df
                table_rows4 = table_rowsn
                count_false4 = (table_rows4['Valid?'] == False).sum()
                if count_false4 == 0:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    dfl3 = table_rows4.sort_values(by=['Last Checked (PDT)','flag_s','flag_d','Table'], ascending=[False,True,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
                else:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    table_rows4['flag_v'] = table_rows4['Valid?'] == False
                    dfl3 = table_rows4.sort_values(by=['flag_v','flag_s','flag_d'], ascending=[False,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
            table_rows2['Causing Table'].values[0] = found_name
            table_rows2['Dataset Date'].values[0] = datede
            table_rows2['DB'].values[0] = dbs


            print (found_name)
            print(table_rows2)

            table_rows2['Table Name'] = name
            table_rows2['JobRunLink'] = link_j
            table_rows2['JobHistoryLink'] = link_h
            table_rows2['Sl No.'] = j
            table_rows2['Date_Wbr'] = wbrdate
            table_rows2['Is_FE'] = 1
            table_rows2['Is_LCE'] = 0
            table_rows2['WFR_MINUTES'] = 0
            table_rows2 = table_rows2[['Sl No.','Table Name','Date_Wbr','Causing Table','Dataset Date','LastChecked','JobRunLink','JobHistoryLink','DB','Is_FE','Is_LCE','WFR_MINUTES']]
            #table_rows2 = table_rows2[['1/2','Last Checked (PDT)',Name','Link','Job Run','Dataset Date','Status','Database']]
            print(table_rows2)
            df2_app_f = pd.concat([table_rows2],ignore_index=True)
            df2_app = df2_app._append(df2_app_f,ignore_index=True)
        elif count_max >= 2 & count_false == 0:
            # table_rows2 = table_rows2.sort_values(by=['LastChecked'], ascending=[False])
            table_rows2['flag'] = table_rows2['Causing Table'].str.upper().str.startswith(('DIGITS', 'SANDBOX'))
            table_rows2 = table_rows2.sort_values(by=['LastChecked','flag'], ascending=[False,True])
            print("Printing for loop2 inside table2")
            print(table_rows2)
            print("Printing for loop2 inside table2 head 1")
            table_rows2 = table_rows2.head(1)
            found_name  = table_rows2['Causing Table'].values[0]
            datede = table_rows2['Dataset Date'].values[0]
            fname2 = found_name.replace('ANDES.', '') if found_name.startswith('ANDES.') else found_name
            fname3 = fname2 + str(datede)
            dbs = table_rows2['DB'].values[0]
            print(str(found_name + datede + dbs))
            k=0
            #print(str(found_name+datede+dbs))
            print(table_rows2)
            while found_name  in pf['name'].values and fname3 not in df2['NameDATE'].values:
                k+=1
                new_df_output = pf[pf['name'] == found_name]
                #new_df_output = pd.DataFrame({'Name': new_filtered_df['name'], 'link': new_filtered_df['link']})
                # print(new_filtered_df)
                print(new_df_output)
                response3 = make_request(new_df_output['link'].values[0])
                soup3 = BeautifulSoup(response3.content, 'html.parser')
                # Find the table you're interested in
                table3 = soup3.find('table', class_='tablesorter')
                # Extract the table data using pandas
                table_data3 = pd.read_html(str(table3))
                # Display the table data
                table_rows3 = table_data3[0]
                jobid = str(table_rows3['Job Run'].values[0])

                link_n = "https://datacentral.a2z.com/console?action=jobrun_details&jobrun_id=" + jobid
                response2 = make_request(link_n)
                soup2 = BeautifulSoup(response2.content, 'html.parser')
                
                table_dependencies_heading_table = soup2.find('h4', text='Table Load Dependencies')
                if table_dependencies_heading_table:
                    table = table_dependencies_heading_table.find_next('table')  # Find the table after the heading
                    df = pd.read_html(str(table))[0]  # This directly extracts the table into a dataframe
                    print(df)
                else:
                    df = pd.DataFrame()
                    print("Data File Dependencies and Table Load Dependencies sections not found")
                table_rowsn = df
                table_rows4 = table_rowsn
                count_false4 = (table_rows4['Valid?'] == False).sum()
                if count_false4 == 0:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    dfl3 = table_rows4.sort_values(by=['Last Checked (PDT)','flag_s','flag_d','Table'], ascending=[False,True,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
                else:
                    table_rows4['flag_d'] = table_rows4['Table'].str.upper().str.startswith('DIGITS')
                    table_rows4['flag_s'] = table_rows4['Table'].str.upper().str.startswith('SANDBOX')
                    table_rows4['flag_v'] = table_rows4['Valid?'] == False
                    dfl3 = table_rows4.sort_values(by=['flag_v','flag_s','flag_d'], ascending=[False,True,True]).head(1)
                    print(dfl3)
                    dfl4 = dfl3[['Table','Dataset Date','DB']]
                    print(dfl4)
                    found_name  = dfl4.iloc[0, 0]
                    dbs = dfl4.iloc[0, 2]
                    datede = dfl4.iloc[0, 1]
                    print (found_name)
                # Prev Logic Kept Saved    
                # if count_false4 == 0:
                #     dfl3 = table_rows4.sort_values(by=['Last Checked (PDT)'], ascending=[False]).head(1)
                #     print(dfl3)
                #     dfl4 = dfl3[['Table','Dataset Date','DB']]
                #     print(dfl4)
                #     found_name  = dfl4.iloc[0, 0]
                #     dbs = dfl4.iloc[0, 2]
                #     datede = dfl4.iloc[0, 1]
                #     print (found_name)
                # else:
                #     dfl3 =  table_rows4[table_rows4['Valid?'].str.upper() == 'FALSE'].head(1)
                #     print(dfl3)
                #     dfl4 = dfl3[['Table','Dataset Date','DB']]
                #     print(dfl4)
                #     found_name  = dfl4.iloc[0, 0]
                #     dbs = dfl4.iloc[0, 2]
                #     datede = dfl4.iloc[0, 1]
                #     print (found_name)
            table_rows2['Causing Table'].values[0] = found_name
            table_rows2['Dataset Date'].values[0] = datede
            table_rows2['DB'].values[0] = dbs


            print (found_name)
            print(table_rows2)

            table_rows2['Table Name'] = name
            table_rows2['JobRunLink'] = link_j
            table_rows2['JobHistoryLink'] = link_h
            table_rows2['Sl No.'] = j
            table_rows2['Date_Wbr'] = wbrdate
            table_rows2['Is_FE'] = 0
            table_rows2['Is_LCE'] = 1
            table_rows2['WFR_MINUTES'] = diff_minutes
            table_rows2 = table_rows2[['Sl No.','Table Name','Date_Wbr','Causing Table','Dataset Date','LastChecked','JobRunLink','JobHistoryLink','DB','Is_FE','Is_LCE','WFR_MINUTES']]
            #table_rows2 = table_rows2[['1/2','Last Checked (PDT)',Name','Link','Job Run','Dataset Date','Status','Database']]
            print(table_rows2)
            df2_app_f = pd.concat([table_rows2],ignore_index=True)
            df2_app = df2_app._append(df2_app_f,ignore_index=True)
    except Exception as d:
        new_df = pd.DataFrame()
        name = row['Name']
        link_j = row['JobRunLink']
        wbrdate = row['Date_Wbr']
        link_h = row['Link']
        j+=1
        new_df['Name'] = pd.Series([name] * 1)
        new_df['link'] = pd.Series([link_j] * 1)
        new_df['wbrdate'] = pd.Series([wbrdate] * 1)
        new_df['linkh'] = pd.Series([link_h] * 1)
        new_df['Exception_Details'] = pd.Series([d] * 1)
        new_df['Sl No.'] = pd.Series([j] * 1)

        df2_execption_f = pd.concat([new_df],ignore_index=True)
        df2_execption = df2_execption._append(df2_execption_f,ignore_index=True)



    
#Came out of FOR loop2
df2_app.to_excel("AllP0_NonP0_Delays.xlsx",index = False)
df2_execption.to_excel('exception_wbr.xlsx')






df2_app = pd.read_excel("AllP0_NonP0_Delays.xlsx")
linksdf_2 = pd.read_excel("linksdf_2.xlsx")
filtered_df2_app = df2_app[df2_app['Table Name'].isin(linksdf_2['name'])]
filtered_df2_app.to_excel("df2_app_wbr_filtered.xlsx",index = False)
df2_app = pd.read_excel("df2_app_wbr_filtered.xlsx")

df2_app = pd.merge(df2_app,wbr_l,how='left',left_on=(('Table Name','Date_Wbr')),right_on=(('mart','Date'))).reset_index()
df2_app = df2_app[['Sl No.','Table Name','Date_Wbr','SLA_Hours','Elapsed_Hours','Causing Table','Dataset Date','LastChecked','JobRunLink','JobHistoryLink','DB','Is_FE','Is_LCE','WFR_MINUTES']]
df2_app = df2_app.sort_values(by=['Date_Wbr'], ascending=[True]).reset_index(drop=True)
df2_app['Sl No.'] = df2_app.index + 1


print("Printing FOR loop 2 Resultant Table")    
print(df2_app)
print("Printing 2nd Table Again")    
print(df2)
df_upstream_all = pd.read_excel("upstream_"+str(filedate)+"report.xlsx")
df_upstream_all['tdate'] = df_upstream_all['Delayed Table'] +":"+ df_upstream_all['Date_Wbr'].astype(str)
df_downstream_all = df2_app
df_downstream_all['tdate_1'] = df_downstream_all['Table Name']+":" + df_downstream_all['Date_Wbr'].astype(str)
#df_downstream_all['tdate_2'] = df_downstream_all['Causing Table'].str.replace('ANDES.','')+":" + df_downstream_all['Dataset Date'].astype(str)
df_downstream_all['tdate_2'] = df_downstream_all['Causing Table'].apply(lambda x: x.replace('ANDES.', '') if x.startswith('ANDES.') else x) + ":" + df_downstream_all['Dataset Date'].astype(str)
# df_downstream_all['us_flag'] = 0
# df_downstream_all['ds_flag'] = 0
for index, row in df_downstream_all.iterrows():
    if row['tdate_2'] in df_upstream_all['tdate'].values:
        #df_downstream_all['us_flag'] = 1
        df_downstream_all.at[index, 'us_flag'] = 1
    else:
        df_downstream_all.at[index, 'us_flag'] = 0
for index, row in df_downstream_all.iterrows():
    if row['tdate_2'] in df_downstream_all['tdate_1'].values:
        # df_downstream_all['ds_flag'] = 1
        df_downstream_all.at[index, 'ds_flag'] = 1

    else:
        df_downstream_all.at[index, 'ds_flag'] = 0

def find_final_root_table(df):
    tdate_3 = []
    for index, row in df.iterrows():
        current_table = row['tdate_1']
        root_table = row['tdate_2']
        while root_table in df['tdate_1'].values:
            root_table = df.loc[df['tdate_1'] == root_table, 'tdate_2'].values[0]
        tdate_3.append(root_table)
    df['tdate_3'] = tdate_3
    return df

df_downstream_all = find_final_root_table(df_downstream_all)
print(df_downstream_all)
df_downstream_all[['t3', 'date3']] = df_downstream_all['tdate_3'].str.split(':', expand=True)
for index, row in df_downstream_all.iterrows():
    if row['tdate_3'] in df_upstream_all['tdate'].values:
        #df_downstream_all['us_flag'] = 1
        df_downstream_all.at[index, 'us_flag'] = '1'
    else:
        df_downstream_all.at[index, 'us_flag'] = '0'
for index, row in df_downstream_all.iterrows():
    if row['tdate_3'] in df_downstream_all['tdate_1'].values:
        # df_downstream_all['ds_flag'] = 1
        df_downstream_all.at[index, 'ds_flag'] = '1'

    else:
        df_downstream_all.at[index, 'ds_flag'] = '0'
df_downstream_all ['Causing Table'] = df_downstream_all['t3'] +'('+ df_downstream_all['us_flag'] + ')'

df_downstream_all.to_excel("df_downstream_all.xlsx")
df_upstream_all.to_excel("df_upstream_all.xlsx")


#Store to excel
df2_app.to_excel("downstream_"+str(filedate)+"report.xlsx",index=False)
df2_app = pd.read_excel("downstream_"+str(filedate)+"report.xlsx")
df2_app = pd.read_excel("df_downstream_all.xlsx")
filedate = date.today()
r_a = df2_app[['Table Name']].drop_duplicates()
# Comment till here for saved file query

filedate = date.today()
#df2_app = pd.read_excel(str(filedate)+"report.xlsx")
# r = df2_app[(df2_app['Is_FE']) == 0 & (df2_app['Is_LCE'] == 0)].reset_index()
r = df2_app[(df2_app['Is_FE'] == 0) & (df2_app['Is_LCE'] == 0)].reset_index()

# r_e = df2_app[df2_app['Is_FE'] == 1 | df2_app['Is_LCE'] == 1]
print(df2_app)
print(r)
r_a = df2_app[['Table Name']].drop_duplicates()
print('ra')
print(r_a)
r_lce = df2_app[(df2_app['Is_FE'] == 0) & (df2_app['Is_LCE'] == 1)].reset_index()
print(r_lce)
print('lce')
r_fe = df2_app[(df2_app['Is_FE'] == 1) & (df2_app['Is_LCE'] == 0)].reset_index()
print(r_fe)
print('fe')
print(r)



# Normal Calculated Tables Start
r.rename(columns={'Table Name':'Delayed Table'},inplace=True)
r.rename(columns={'Date_Wbr':'Delayed Table Dates'},inplace=True)
r.rename(columns={'Dataset Date':'Causing Table Dates'},inplace=True)
r['Elapsed_Hours'] = r['Elapsed_Hours'].astype(str)
r['SLA_Hours'] = r['SLA_Hours'].astype(str)
r['WFR_MINUTES'] = r['WFR_MINUTES'].astype(str)



df_dates_d = r.groupby('Delayed Table')['Delayed Table Dates'].apply(','.join).reset_index()
df_dates_c = r.groupby('Delayed Table')['Causing Table Dates'].apply(','.join).reset_index()
df_ct = r.groupby('Delayed Table')['Causing Table'].apply(','.join).reset_index()
df_cdb = r.groupby('Delayed Table')['DB'].apply(','.join).reset_index()
df_elap = r.groupby('Delayed Table')['Elapsed_Hours'].apply(','.join).reset_index()
df_sla = r.groupby('Delayed Table')['SLA_Hours'].apply(','.join).reset_index()
df_wfr = r.groupby('Delayed Table')['WFR_MINUTES'].apply(','.join).reset_index()


#df_lc = r.groupby('Delayed Table')['LastChecked'].apply(','.join).reset_index()
name_link = r[['Delayed Table','JobHistoryLink']]
name_link = name_link.drop_duplicates()

output_df = pd.merge(df_dates_d,df_dates_c, on='Delayed Table')
output_df = pd.merge(output_df,df_ct, on='Delayed Table')
output_df = pd.merge(output_df,df_cdb, on='Delayed Table')
output_df = pd.merge(output_df,df_elap, on='Delayed Table')
output_df = pd.merge(output_df,name_link, on='Delayed Table')
output_df = pd.merge(output_df,df_sla, on='Delayed Table')
output_df = pd.merge(output_df,df_wfr, on='Delayed Table')



output_df['Frequency'] = r.groupby('Delayed Table').size().reset_index(name='count')['count']
output_df['Sl No.'] = output_df.index + 1
output_df = output_df[['Sl No.','Delayed Table','Delayed Table Dates','Frequency','Causing Table','JobHistoryLink','Causing Table Dates','SLA_Hours','Elapsed_Hours','WFR_MINUTES','DB']]
output_df['JobHistoryLink'] = output_df['JobHistoryLink'].apply(lambda x: f'<a href="{x}">Click here</a>')
#output_df = output_df[['Sl No.','Delayed Table','JobHistoryLink','Frequency']]
r = output_df
#Normal Calculated Tables End

#Exceptional Calculated Tables Start
# r_e.rename(columns={'Table Name':'Delayed Table'},inplace=True)
# r_e.rename(columns={'Date_Wbr':'Delayed Table Dates'},inplace=True)
# r_e.rename(columns={'Dataset Date':'Causing Table Dates'},inplace=True)
# df_dates_d_e = r_e.groupby('Delayed Table')['Delayed Table Dates'].apply(','.join).reset_index()
# df_dates_c_e = r_e.groupby('Delayed Table')['Causing Table Dates'].apply(','.join).reset_index()
# df_ct_e = r_e.groupby('Delayed Table')['Causing Table'].apply(','.join).reset_index()
# df_cdb_e = r_e.groupby('Delayed Table')['DB'].apply(','.join).reset_index()
# #df_lc = r.groupby('Delayed Table')['LastChecked'].apply(','.join).reset_index()
# name_link_e = r_e[['Delayed Table','JobHistoryLink']]
# name_link_e = name_link_e.drop_duplicates()

# output_df_e = pd.merge(df_dates_d_e,df_dates_c_e, on='Delayed Table')
# output_df_e = pd.merge(output_df_e,df_ct_e, on='Delayed Table')
# output_df_e = pd.merge(output_df_e,df_cdb_e, on='Delayed Table')
# output_df_e = pd.merge(output_df_e,name_link_e, on='Delayed Table')

# output_df_e['Frequency'] = r_e.groupby('Delayed Table').size().reset_index(name='count')['count']
# output_df_e['Sl No.'] = output_df_e.index + 1
# output_df_e = output_df_e[['Sl No.','Delayed Table','Delayed Table Dates','Frequency','Causing Table','Causing Table Dates','JobHistoryLink','DB']]
# output_df_e['JobHistoryLink'] = output_df_e['JobHistoryLink'].apply(lambda x: f'<a href="{x}">Click here</a>')
# #output_df = output_df[['Sl No.','Delayed Table','JobHistoryLink','Frequency']]
# r_e = output_df_e
# Old exceptions above
# Last Time Check Exception
r_lce.rename(columns={'Table Name':'Delayed Table'},inplace=True)
r_lce.rename(columns={'Date_Wbr':'Delayed Table Dates'},inplace=True)
r_lce.rename(columns={'Dataset Date':'Causing Table Dates'},inplace=True)
r_lce['Elapsed_Hours'] = r_lce['Elapsed_Hours'].astype(str)
r_lce['SLA_Hours'] = r_lce['SLA_Hours'].astype(str)
r_lce['WFR_MINUTES'] = r_lce['WFR_MINUTES'].astype(str)



df_dates_d_lce = r_lce.groupby('Delayed Table')['Delayed Table Dates'].apply(','.join).reset_index()
df_dates_c_lce = r_lce.groupby('Delayed Table')['Causing Table Dates'].apply(','.join).reset_index()
df_ct_lce = r_lce.groupby('Delayed Table')['Causing Table'].apply(','.join).reset_index()
df_cdb_lce = r_lce.groupby('Delayed Table')['DB'].apply(','.join).reset_index()
df_elap_lce = r_lce.groupby('Delayed Table')['Elapsed_Hours'].apply(','.join).reset_index()
df_sla_lce = r_lce.groupby('Delayed Table')['SLA_Hours'].apply(','.join).reset_index()
df_wfr_lce = r_lce.groupby('Delayed Table')['WFR_MINUTES'].apply(','.join).reset_index()

#df_lc = r.groupby('Delayed Table')['LastChecked'].apply(','.join).reset_index()
name_link_lce = r_lce[['Delayed Table','JobHistoryLink']]
name_link_lce = name_link_lce.drop_duplicates()

output_df_lce = pd.merge(df_dates_d_lce,df_dates_c_lce, on='Delayed Table')
output_df_lce = pd.merge(output_df_lce,df_ct_lce, on='Delayed Table')
output_df_lce = pd.merge(output_df_lce,df_cdb_lce, on='Delayed Table')
output_df_lce = pd.merge(output_df_lce,df_elap_lce, on='Delayed Table')
output_df_lce = pd.merge(output_df_lce,name_link_lce, on='Delayed Table')
output_df_lce = pd.merge(output_df_lce,df_sla_lce, on='Delayed Table')
output_df_lce = pd.merge(output_df_lce,df_wfr_lce, on='Delayed Table')


output_df_lce['Frequency'] = r_lce.groupby('Delayed Table').size().reset_index(name='count')['count']
output_df_lce['Sl No.'] = output_df_lce.index + 1
output_df_lce = output_df_lce[['Sl No.','Delayed Table','Delayed Table Dates','Frequency','Causing Table','JobHistoryLink','Causing Table Dates','SLA_Hours','Elapsed_Hours','WFR_MINUTES','DB']]
output_df_lce['JobHistoryLink'] = output_df_lce['JobHistoryLink'].apply(lambda x: f'<a href="{x}">Click here</a>')
#output_df = output_df[['Sl No.','Delayed Table','JobHistoryLink','Frequency']]
r_lce = output_df_lce

# False Valid Exception

r_fe.rename(columns={'Table Name':'Delayed Table'},inplace=True)
r_fe.rename(columns={'Date_Wbr':'Delayed Table Dates'},inplace=True)
r_fe.rename(columns={'Dataset Date':'Causing Table Dates'},inplace=True)
r_fe['Elapsed_Hours'] = r_fe['Elapsed_Hours'].astype(str)
r_fe['SLA_Hours'] = r_fe['SLA_Hours'].astype(str)
r_fe['WFR_MINUTES'] = r_fe['WFR_MINUTES'].astype(str)


df_dates_d_fe = r_fe.groupby('Delayed Table')['Delayed Table Dates'].apply(','.join).reset_index()
df_dates_c_fe = r_fe.groupby('Delayed Table')['Causing Table Dates'].apply(','.join).reset_index()
df_ct_fe = r_fe.groupby('Delayed Table')['Causing Table'].apply(','.join).reset_index()
df_cdb_fe = r_fe.groupby('Delayed Table')['DB'].apply(','.join).reset_index()
df_elap_fe = r_fe.groupby('Delayed Table')['Elapsed_Hours'].apply(','.join).reset_index()
df_sla_fe = r_fe.groupby('Delayed Table')['SLA_Hours'].apply(','.join).reset_index()
df_wfr_fe = r_fe.groupby('Delayed Table')['WFR_MINUTES'].apply(','.join).reset_index()

#df_lc = r.groupby('Delayed Table')['LastChecked'].apply(','.join).reset_index()
name_link_fe = r_fe[['Delayed Table','JobHistoryLink']]
name_link_fe = name_link_fe.drop_duplicates()

output_df_fe = pd.merge(df_dates_d_fe,df_dates_c_fe, on='Delayed Table')
output_df_fe = pd.merge(output_df_fe,df_ct_fe, on='Delayed Table')
output_df_fe = pd.merge(output_df_fe,df_cdb_fe, on='Delayed Table')
output_df_fe = pd.merge(output_df_fe,df_elap_fe, on='Delayed Table')
output_df_fe = pd.merge(output_df_fe,name_link_fe, on='Delayed Table')
output_df_fe = pd.merge(output_df_fe,df_sla_fe, on='Delayed Table')
output_df_fe = pd.merge(output_df_fe,df_wfr_fe, on='Delayed Table')



output_df_fe['Frequency'] = r_fe.groupby('Delayed Table').size().reset_index(name='count')['count']
output_df_fe['Sl No.'] = output_df_fe.index + 1
output_df_fe = output_df_fe[['Sl No.','Delayed Table','Delayed Table Dates','Frequency','Causing Table','JobHistoryLink','Causing Table Dates','SLA_Hours','Elapsed_Hours','WFR_MINUTES','DB']]
output_df_fe['JobHistoryLink'] = output_df_fe['JobHistoryLink'].apply(lambda x: f'<a href="{x}">Click here</a>')
#output_df = output_df[['Sl No.','Delayed Table','JobHistoryLink','Frequency']]
r_fe = output_df_fe
# Exceptional Calculated Tables End



# upstream = wbr[~wbr['mart'].isin(r_a['Table Name'])]
# upstream = upstream.sort_values(by=['Date_Wbr'], ascending=[True]).reset_index(drop=True)

# upstream.rename(columns={'mart':'Delayed Table'},inplace=True)
# df_dates_w = upstream.groupby('Delayed Table')['Date_Wbr'].apply(','.join).reset_index()
# df_dates_w['Frequency'] = upstream.groupby('Delayed Table').size().reset_index(name='count')['count']
# df_dates_w['Sl No.'] = df_dates_w.index + 1

# upstream = df_dates_w[['Sl No.','Delayed Table','Date_Wbr','Frequency']]
# print(upstream)

# df = pd.read_excel("2023-10-27report.xlsx")
# r = df
# filedate = date.today()

# def create_clickable_text(row):
#     return '<a href="{}">Click here</a>'.format(row['JobRunLink'])

# # Applying the function to create the clickable text column
# df['JobRunLink'] = df.apply(create_clickable_text, axis=1)
# #r = pd.read_excel(str(filedate)+"report.xlsx")
# print(df)

# def highlights(rows):
#     print("sd")
#     if rows['Status']=='Error':
#         print("ini")
#         return ['background-color: red; color: white;'] * len(rows)
#     elif rows['Status']=='Waiting for Dependencies' or rows['Status']=='Waiting for Resources':
#         print("ini1")
#         return ['background-color: whitesmoke; color: black;'] * len(rows)
#     elif rows['Status']=='Executing':
#         print("ini2")
#         return ['background-color: orange; color: black;'] * len(rows)
#     else:
#         return [''] * len(rows)
# r = r.style.apply(highlights, axis=1)
# print(r)


# # border_style = '1px solid black'
# # r = Styler(pd.DataFrame(r)).set_table_styles([
# #     {'selector': 'th', 'props': [('border', border_style)]},
# #     {'selector': 'td', 'props': [('border', border_style)]}
# # ])
# table_style = [
#     {'selector': 'table',
#      'props': [
#          ('border-collapse', 'collapse')
#          #('border-radius', '500px')  # Adjust the radius as needed
#      ]},
#     {'selector': 'th, td',
#      'props': [
#          ('border', '1px solid black'),
#          ('padding', '8px')
#          #('border-radius', '500px')
#      ]},
#     {'selector': 'th',
#      'props': [
#          ('background-color', 'lightblue')
#      ]}
# ]

# r = r.set_table_styles(table_style)
# r = r.set_table_attributes('style="border: 1px solid black;"')
# r.hide()
# r.set_table_styles(
#     [{"selector": "", "props": [("border", "1px solid grey")]},
#       {"selector": "tbody td", "props": [("border", "1px solid grey")]},
#      {"selector": "th", "props": [("border", "1px solid grey")]}
#     ]
# )
#body1=f'<style>table {{border-collapse: collapse;}} th, td {{border: 1px solid black; padding: 8px; text-align: left;}}</style>{r}'
#body1 = r.to_html(border=1)
#body1 = er_df.to_html(index=False)
# r = er_df.style.apply(highlights, axis=1)
# r.hide()
#r.hide()
print(r)
# r['JobRunLink'] = r['JobRunLink'].apply(lambda x: f'<a href="{x}">link</a>',axis = 1)
# r['JobRunLink'] = df.apply(lambda row: f'<a href="{row["JobRunLink"]}">Click here</a>', axis=1)

# Create a new column 'clickable_link' with modified link text
#r['JobRunLink'] = r.apply(lambda row: f'Click here ({strip_tags(row["JobRunLink"])})', axis=1)
# Create a new column 'clickable_link' with modified link text
#r['JobRunLink'] = r.apply(lambda row: f'Click here ({BeautifulSoup(row["JobRunLink"], "html.parser").get_text()})', axis=1)

# Send the email with email_body as the content
# (Code for sending email not included here as it depends on the specific method you're using)

# r = r[['JobRunLink']]
table_style2 = [
    {'selector': 'table',
     'props': [
         ('border-collapse', 'collapse')
         #('border-radius', '500px')  # Adjust the radius as needed
     ]},
    {'selector': 'th, td',
     'props': [
         ('border', '1px solid black'),
         ('padding', '8px')
         #('border-radius', '500px')
     ]},
    {'selector': 'th',
     'props': [
         ('background-color', 'bisque')
     ]}
]

# s = r.style.apply(highlight, axis=1)
def highlights(rows):
    print("sd")
    if rows['DB']=='Error':
        print("ini")
        return ['background-color: red; color: white;'] * len(rows)
    elif rows['DB']=='Waiting for Dependencies' or rows['DB']=='Waiting for Resources':
        print("ini1")
        return ['background-color: lavender; color: black;'] * len(rows)
    elif rows['DB']=='Executing':
        print("ini2")
        return ['background-color: orange; color: black;'] * len(rows)
    else:
        return [''] * len(rows)
s = r.style.apply(highlights, axis=1)
mwinit = type(s)    
s = s.set_table_styles(table_style2)
s = s.set_table_attributes('style="border: 1px solid black;"')
s.hide()
mwinit2 = type(s)    


body1 = s.to_html()
mwinit3 = type(body1)

# Exception
# s_e = r_e.style.apply(highlights, axis=1)
# mwinit_e = type(s_e)    
# s_e = s_e.set_table_styles(table_style2)
# s_e = s_e.set_table_attributes('style="border: 1px solid black;"')
# s_e.hide()
# mwinit2_e = type(s_e)    


# body1_e = s_e.to_html()
# mwinit3_e = type(body1_e) 

# LCE
s_lce = r_lce.style.apply(highlights, axis=1)
mwinit_lce = type(s_lce)    
s_lce = s_lce.set_table_styles(table_style2)
s_lce = s_lce.set_table_attributes('style="border: 1px solid black;"')
s_lce.hide()
mwinit2_lce = type(s_lce)    


body1_lce = s_lce.to_html()
mwinit3_lce = type(body1_lce) 

# FE
s_fe = r_fe.style.apply(highlights, axis=1)
mwinit_fe = type(s_fe)    
s_fe = s_fe.set_table_styles(table_style2)
s_fe = s_fe.set_table_attributes('style="border: 1px solid black;"')
s_fe.hide()
mwinit2_fe = type(s_fe)    


body1_fe = s_fe.to_html()
mwinit3_fe = type(body1_fe) 

#body1 = r.to_html(index=False)
body2 = upstream.to_html(index=False)
body2 = upstream.to_html(index=False)

# df['JobRunLink'] = df['JobRunLink'].apply(lambda x: f'<a href="{x}">link</a>')
# print(df['JobRunLink'])
outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
# print(r)
# print(body1)
# print(df['JobRunLink'])
#body1['JobRunLink'] = body1.apply(lambda row: f'<a href="{row["JobRunLink"]}">Click here</a>', axis=1)
display(r)
display(HTML(body1))
display(HTML(body2))

# link = "https://www.example.com"
# clickable_text = '<a href="{link}">Click her</a>'
# print(clickable_text) 
# link = "https://www.example.com"
# clickable_text = markdown(f"[Click here]({link})")
# print(clickable_text)
# link = "https://www.example.com"
# clickable_text = f'[Click here]({link})'
# print(clickable_text)
# from markdown import markdown

# link = "https://www.example.com"
# clickable_text = markdown(f"[Click here]({link})")
# print(clickable_text)


# df = pd.DataFrame({'name':['Pandas', 'Linux']})

# df['name'] = df['name'].apply(lambda x: f'<a href="http://softhints.com/tutorial/{x}">{x}</a>')
# ok = HTML(df.to_html(escape=False))
# display("ok")
# display(ok)
# df = pd.DataFrame({'link':['https://www.softhints.com', 'https://datascientyst.com']})

# def make_clickable(val):
#     return f'<a href="{val}">"bal"</a>'

# df.style.format(make_clickable)
# df = pd.DataFrame({
#     'name':['Softhints', 'DataScientyst'],
#     'url':['https://www.softhints.com', 'https://datascientyst.com'],
#     'url2':['https://www.blog.softhints.com/tag/pandas', 'https://datascientyst.com/tag/pandas']
# })

# print(df)
# def make_clickable(url, name):
#     return '<a href="{}" rel="noopener noreferrer" target="_blank">{}</a>'.format(url,name)

# df['link'] = df.apply(lambda x: make_clickable(x['url'], x['name']), axis=1)
# df.style

# print(df)
# display(HTML(df.to_html()))
# body1 = df.to_html()

# data = {
#     'customer_name': ['John', 'Mary', 'John', 'Alice', 'Mary'],
#     'order_date': ['2021-01-01', '2021-02-15', '2021-03-08', '2021-04-10', '2021-05-20'],
#     'item_name': ['Apple', 'Banana', 'Orange', 'Grapes', 'Kiwi']
# }

# # Create the dataframe
# df = pd.DataFrame(data)

# # Display the dataframe
# print(df)
# grouped = df.groupby('customer_name')

# # Aggregate 'order_date' as comma-separated values using join
# order_dates = grouped['order_date'].apply(','.join)

# # Aggregate 'item_name' as comma-separated values using join
# item_names = grouped['item_name'].apply(','.join)

# # Create a new dataframe with the desired output
# output_df = pd.DataFrame({'Order Dates': order_dates, 'Item Names': item_names})


# df['count'] = df.groupby('customer_name')['customer_name'].transform('count')

# # Display the updated dataframe
# print(df)

# # Display the resulting dataframe
# print(output_df)
# import pandas as pd

# # Dummy data
# data = {
#     'customer_name': ['John', 'Mary', 'John', 'Alice', 'Mary'],
#     'order_date': ['2021-01-01', '2021-02-15', '2021-03-08', '2021-04-10', '2021-05-20'],
#     'item_name': ['Apple', 'Banana', 'Orange', 'Grapes', 'Kiwi']
# }

# # Create the dataframe
# df = pd.DataFrame(data)

# # Group the dataframe by 'customer_name' and aggregate order dates as a comma-separated string
# df_dates = df.groupby('customer_name')['order_date'].apply(','.join).reset_index()

# # Group the dataframe by 'customer_name' and aggregate item names as a comma-separated string
# df_items = df.groupby('customer_name')['item_name'].apply(','.join).reset_index()

# # Merge the dataframes by 'customer_name'
# output_df = pd.merge(df_dates, df_items, on='customer_name')

# # Add a 'count' column representing the number of orders for each customer
# output_df['frequency'] = df.groupby('customer_name').size().reset_index(name='count')['count']
# output_df['serial'] = output_df.index + 1

# # Display the resulting dataframe
# print(output_df)


#Outlook setup starts

if teamss == '1':
    # mail.To = 'kumraov'+'@amazon.com'
    # mail.To = 'mdsagkha'+'@amazon.com;' + 'kumraov'+'@amazon.com;'
    mail.To = 'inpay-dp-team@amazon.com;'  + 'mdsagkha'+'@amazon.com;'
    mail.cc='sastry' + '@amazon.com;' + 'plammojo'+'@amazon.com;iankranj@amazon.com'
        

else:
    mail.cc = login + '@amazon.com;'

# if team==0:
#     # mail.To = 'kumraov'+'@amazon.com'
#     # mail.To = 'mdsagkha'+'@amazon.com;' + 'kumraov'+'@amazon.com;'
#     mail.To = 'mdsagkha'+'@amazon.com;'
#     # mail.cc='pandev'+'@amazon.com'
# else:
#     mail.To ='mdsagkha'+'@amazon.com'

# mail.To = 'kumraov'+'@amazon.com'
mail.Body = 'Hi team,'+'\n\n'+'Please find the wbr report sheet for delayed tables'+'\n'
#mail.HTMLBody += "<br>There were some code changes done, hence sending updated mail<br><br>"
mail.HTMLBody += "<b><i><u>Upstream</u></i></b><br>"
mail.HTMLBody += "<br><br>"
mail.HTMLBody += body2
mail.HTMLBody += "<br><br>"
mail.HTMLBody += "<br><b><i><u>Downstream Normal</u></i></b><br>"
mail.HTMLBody += "<br><br>"
mail.HTMLBody += body1
mail.HTMLBody += "<br><br>"
mail.HTMLBody += "<br><b><i><u>Downstream Multiple Same Last Checked Exceptions</u></i></b><br>"
mail.HTMLBody += "<br><br>"
mail.HTMLBody += body1_lce
mail.HTMLBody += "<br><br>"
mail.HTMLBody += "<br><b><i><u>Downstream Multiple Valid False Exceptions</u></i></b><br>"
mail.HTMLBody += "<br><br>"
mail.HTMLBody += body1_fe
mail.HTMLBody += f"<b><br>Thanks,<br>{mailname}</b>"
# mail.Body += 'Thanks,'
# #mail.Body += "<br>"
# mail.Body += 'Sagar'
mail.Subject = 'Delayed Tables WBR Report Sheet - Platform Team'
# mail.Body = 'Hi,'+'\n\n'+'Please find the updated sheet for data monitoring job.'+'\n\n'+'Thank you'
login = os.getlogin()
attachment  = "C:\\Users\\"+login+"\\Downloads\\automation_workspace\\downstream_"+str(filedate)+"report.xlsx"
att2 = "C:\\Users\\"+login+"\\Downloads\\automation_workspace\\upstream_"+str(filedate)+"report.xlsx"
att3 = "C:\\Users\\"+login+"\\Downloads\\automation_workspace\\AllP0_NonP0_Delays.xlsx"
mail.Attachments.Add(attachment)
mail.Attachments.Add(att2)
mail.Attachments.Add(att3)

mail.Send()
print('mail gaya')


#Outlook setup ends
