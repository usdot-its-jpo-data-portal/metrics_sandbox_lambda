import boto3
import psycopg2
import httplib2
import os
import time
import datetime
import json
import yaml

from googleapiclient.http import MediaFileUpload
from googleapiclient import discovery
from google.oauth2 import service_account
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

from sesemail import sendEmail

value_range_body = {'values':[]}

pageviews = 0
wydot_bsm_downloads = 0
wydot_tim_downloads = 0
tampa_bsm_downloads = 0
tampa_spat_downloads = 0
tampa_tim_downloads = 0
nyc_bsm_downloads = 0
nyc_spat_downloads = 0
nyc_map_downloads = 0

with open("config.yml", 'r') as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

def get_credentials():
    service_account_info = json.loads(os.environ['google_api_credentials'])
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials

def process_lines(log):
    '''
    Reads each line in log file. Looks for keyword REST.GET.OBJECT that indicates a file was downloaded by a user. Checks which file was
    downloaded/accessed by the user and adds to the appropriate count.
    '''
    global pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,tampa_tim_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads
    for line in log:
        if " REST.GET.OBJECT " in line:
            row = line.split(" ")
            item = row[row.index("REST.GET.OBJECT") + 1]
            if item == "index.html":
                pageviews += 1
            elif "wydot" in item:
                if "BSM" in item:
                    wydot_bsm_downloads += 1
                else:
                    wydot_tim_downloads += 1
            elif "thea" in item:
                if "BSM" in item:
                    tampa_bsm_downloads += 1
                elif "SPAT" in item:
                    tampa_spat_downloads += 1
                else:
                    tampa_tim_downloads += 1
            elif "nyc" in item:
                if "BSM" in item:
                    nyc_bsm_downloads += 1
                elif "SPAT" in item:
                    nyc_spat_downloads += 1
                else:
                    nyc_map_downloads += 1

def get_monthly(cur, today):
    '''
    Queries database to get past month of data to write to Google Sheets for dashboard to access
    '''
    last_month = today - datetime.timedelta(days=29)
    cur.execute("SELECT datetime,pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads,tampa_tim_downloads FROM ipdh_metrics.sandbox_metrics WHERE datetime >= %s ORDER BY datetime",[last_month])
    results = cur.fetchall()
    value_range_body = {'values':[]}
    for record in results:
        row = []
        row.append(record[0].strftime("%Y-%m-%d %H:%M:%S"))
        row.append(record[1])
        row.append(record[2])
        row.append(record[3])
        row.append(record[4])
        row.append(record[5])
        row.append(record[9])
        row.append(record[6])
        row.append(record[7])
        row.append(record[8])
        value_range_body['values'].append(row)
    return value_range_body

def lambda_handler(event, context):
    try:
        session = boto3.session.Session()
        s3 = session.resource('s3')
        #Add s3 bucket name that contains server access log files
        mybucket = s3.Bucket('usdot-its-cvpilot-public-data-logs')

        today = datetime.datetime.combine(datetime.date.today(),datetime.time(tzinfo=datetime.timezone(datetime.timedelta(0))))
        yesterday = today - datetime.timedelta(hours=24)
        yesterdayfmat = yesterday.strftime("%Y-%m-%d")
        prefix = "logs/" + yesterdayfmat
        print("Prefix: " + str(prefix))
        for record in mybucket.objects.filter(Prefix=str(prefix)):
            if record.last_modified > yesterday and record.last_modified <= today:
                log = record.get()['Body'].read().decode('utf-8')
                log = log.splitlines()
                # print("====This is where process_lines(log) would get called ====")
                process_lines(log)

        #Add parameters to connect to specific Postgres database
        conn = psycopg2.connect(os.environ["pg_connection_string"])
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("INSERT INTO ipdh_metrics.sandbox_metrics (datetime,pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(today,pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads))
        value_range_body = get_monthly(cur, today)
        conn.commit()
        cur.close()
        conn.close()
         
        credentials = get_credentials()
        # http = credentials.authorize(httplib2.Http())
        service = discovery.build('sheets', 'v4', credentials=credentials)
        #Enter spreadsheet id from Google Sheets object
        spreadsheet_id = os.environ["spreadsheet_id_s3"]
        spreadsheetRange = "A2:J" + str(len(value_range_body['values']) + 1)
        value_input_option = 'USER_ENTERED'
        request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=spreadsheetRange, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()
        print(response)
    except Exception as e:
        sendEmail("Sandbox - Lambda", str(e) )
