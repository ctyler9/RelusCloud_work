# Days Off Scheduler
# Relus Corp
# Written By: Clay Tyler


# GOAL: the goal of the script is to automate input of days off found from
# Pingboard, and have it automatically add into the scheduling google sheet

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import json
import datetime as dt
from collections import namedtuple
from math import ceil
import boto3
import gspread_dataframe as gd

def lambda_handler(context, event):
    ### API Authentication ###
    client_id = ''
    client_secret = ''

    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url='https://app.pingboard.com/oauth/token',client_id=client_id,
            client_secret=client_secret)


    ### linking google sheet to gspread module with google drive API ###
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('relus360.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open('Scheduling')
    worksheet = sheet.worksheet('PTO')


    ### linking account names with actual names ###
    rs = oauth.get('https://app.pingboard.com/api/v2/users?page_size=1000')
    ds = rs.json()
    name_dict = {}
    for i in ds['users']:
        name_dict[int(i['id'])] = i['first_name'] + ' ' + i['last_name']


    rs1 = oauth.get('https://app.pingboard.com/api/v2/groups?page_size=1000')
    ds1 = rs1.json()
    cloud_dict = {}
    str_counter = 0
    for i in ds1['groups']:
        if i['name'] == 'Cloud Delivery':
            for j in i['links']['users']:
                str_counter += 1
                cloud_dict['ID' + str(str_counter)] = j



    # to make sure this can run uninterupted and without human input,
    # dt_now will always update the current time
    dt_now = dt.datetime.now()
    sdate = dt_now - dt.timedelta(days=dt_now.weekday())
    edate = sdate + dt.timedelta(days=4)


    ### Writing data to pandas dataframe, then to google sheets and S3 ###
    func_ender = 0
    df_master = pd.DataFrame()
    while func_ender != 13:

        sdate_str = str(sdate)[0:10] + 'T' + str(sdate)[11:19]
        edate_str = str(edate)[0:10] + 'T' + str(edate)[11:19]

        chart_sdate_str = sdate_str[5:7] + '/' + sdate_str[8:10]
        chart_edate_str = edate_str[5:7] + '/' + edate_str[8:10]
        tot_chart_date_str = chart_sdate_str + '-' + chart_edate_str


        # accessing API, converting storing local json file
        r = oauth.get('https://app.pingboard.com/api/v2/statuses?starts_at={0}&en,\
            ds_at={1}?page_size=10000'.format(sdate_str, edate_str))
        d = r.json()

        # loading only necessary data into a dictionary
        api_dict = {}
        for i in d['statuses']:
            api_dict[i['user_id']] = [i['starts_at'], i['ends_at']]


        # making a copy
        api_dict_copy = api_dict.copy()

        # iterating through the api_dict_copy, converting strings into datetime, then
        # subtracting the two datetime object to get difference, then converting the
        # delta.datetime object to days with float value
        for key in api_dict_copy:
            start_date = api_dict[key][0]
            end_date = api_dict[key][1]
            sd = dt.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')
            ed = dt.datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%SZ')

            Range = namedtuple('Range', ['start', 'end'])
            r1 = Range(start=sd, end=ed)
            r2 = Range(start=sdate, end=edate)
            latest_start = max(r1.start, r2.start)
            earliest_end = min(r1.end, r2.end)
            delta = (earliest_end - latest_start).days + 1
            delta2 = (earliest_end - latest_start).seconds/(3600*24)
            if delta2 > .5:
                delta_total = (delta + ceil(delta2))
            else:
                delta_total = delta
            overlap = max(0, delta_total)
            api_dict_copy[key] = overlap

        # esentially goes through the list of keys in local_dict and replaces the name
        # with the id_num, and if they aren't in the list, assigned a value of 0
        for id_num in name_dict:
            if id_num in api_dict_copy.keys():
                api_dict_copy[name_dict[id_num]] = api_dict_copy.pop(id_num)
            else:
                api_dict_copy[name_dict[id_num]] = 0

        # printing data to pandas dataframe
        for key, value in sorted(api_dict_copy.items()):
            df_master['Name'] = api_dict_copy.keys()
            df_master[tot_chart_date_str] = [x*8 for x in api_dict_copy.values()]


        func_ender += 1
        sdate += dt.timedelta(days = 7)
        edate += dt.timedelta(days = 7)

    df_master = df_master.sort_values(by='Name')
    df_master = df_master.reset_index(drop=True)



    ### uploading to google sheet ###
    #culling the data so it only shows cloud delivery team
    culled_dict = {}
    for i in name_dict.items():
        for j in cloud_dict.items():
            if i[0] == int(j[1]):
                culled_dict[i[0]] = i[1]

    df_master_culled = df_master.copy()
    df_master_culled = df_master_culled[df_master_culled['Name'].isin(culled_dict.values())]
    df_master_culled = df_master_culled.reset_index(drop=True)

    upload = gd.set_with_dataframe(worksheet, df_master_culled)


    ### uploading to aws web servers ###
    session = boto3.Session(region_name='us-east-1')
    s3 = session.client('s3',aws_access_key_id='',
        aws_secret_access_key='')
    bucketName = 'relus-360-pto'
    current_date = dt.datetime.now()
    c_date = current_date.strftime('%Y%m%d')

    historical_outfile = 'relus360_pto_raw/pto/ingestdate={0}/pto.parquet'.format(c_date)
    df_master.to_parquet('pto.parquet', engine='fastparquet')
    s3.upload_file('pto.parquet', bucketName, historical_outfile)


