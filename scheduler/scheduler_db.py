# Scheduler to DB
# Relus Corp
# Written By: Clay Tyler

# GOAL: the goal of the script is to actively read the scheduler dashboard
# and convert it into a queryable dataframe to be then uploaded to AWS S3

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from dateutil import parser
import boto3
import datetime as dt

def lambda_handler(event, context):

    # linking google sheet to gspread module with google drive API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('relus360.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open('Scheduling')
    worksheet = sheet.worksheet('Forecasts')

    # scraping data from excel to pandas data frame and cleaning it up
    data_dump = worksheet.get_all_records()
    analyzed = pd.DataFrame(data_dump)
    analyzed.rename(index=str, columns={"Allocation Type": "allocationType", "NS Resource": "nsResource", \
        "Project Role": "projectRole", "Project Type": "projectType", "Employee Type":"employeeType","Hours Totals":"hoursTotals", \
        "Role": "role", "Project": "project", "Practice": "practice", "PM":"pm"}, inplace=True)
    analyzed = analyzed.iloc[:, ::-1]
    header_list = list(analyzed.columns.values)


    # contigency solution if excel sheet is modified
    count = 0
    for j in header_list:
        try:
            text = int(j[0])
        except:
            count += 1

    count2 = 0
    for projects in analyzed['project']:
        if projects == 'INTERNAL':
            count2 += 1

    report_data = analyzed.iloc[count2:, 0:count]
    hour_data = analyzed.iloc[count2:, count:]
    report_data.reset_index(drop=True, inplace=True)
    hour_data.reset_index(drop=True, inplace=True)

    ### start flattening of data from google sheets to data frame ###
    # create a dictionary to store the flattened arrays
    frame_dict = {}
    data_range_list = header_list[count:]

    # repeat the date object n amount of times, where n = number of rows of DF
    # then append this array into a the dictionary created previously
    repeat_data_list = np.repeat(data_range_list, hour_data.shape[0])
    frame_dict['week'] = repeat_data_list

    # take number of hours and keep concatonating to an array, once done link
    # giant array to 'Hours' key in the dictionary
    hours_list = []
    for i in data_range_list:
        hours_list.append(hour_data[i].tolist())
    flat_list = [item for sublist in hours_list for item in sublist]
    frame_dict['hours'] = flat_list

    # convert the dictionary to the data frame
    # prime the report data to concatonate onto itself n many times
    # reset index so two different data frames will join
    ss = pd.DataFrame(frame_dict, index=None)
    num = int(ss.shape[0]/report_data.shape[0])
    sd = pd.concat([report_data] * num)
    sd.reset_index(drop=True, inplace=True)

    # join tables based off of index
    df_final = sd.join(ss, how='outer', sort=False)

    ### cleaning up the data in final_data dataframe ###
    #convert str object in week column to datetime objects
    date_time_dict = {}
    for i in data_range_list:
        date_time = parser.parse(i)
        str_time = date_time.strftime('%m/%d/%Y')
        date_time_dict[i] = str_time

    for i, j in enumerate(df_final['week']):
        for k in date_time_dict.keys():
            if str(j) == str(k):
                df_final['week'][i] = date_time_dict[k]

    df_final['project_str'] = None
    for i, j in enumerate(df_final['project']):
        text = j.split()
        try:
            int_test = int(text[0][1])
        except:
            int_test = 'var'
        if type(int_test) == int:
            df_final['project'][i] = text[0]
            df_final['project_str'][i] = ' '.join(text[1:])

    df_final['pm_str'] = None
    for i, j in enumerate(df_final['pm']):
        text = j.split()
        try:
            int_test = int(text[0][1])
        except:
            int_test = 'var'
        if type(int_test) == int:
            df_final['pm'][i] = text[0]
            df_final['pm_str'][i] = ' '.join(text[1:])

    df_final['nsResource_str'] = None
    for i, j in enumerate(df_final['nsResource']):
        text = j.split()
        try:
            int_test = int(text[0][1])
        except:
            int_test = 'var'
        if type(int_test) == int:
            df_final['nsResource'][i] = text[0]
            df_final['nsResource_str'][i] = ' '.join(text[1:])

    for i, j in enumerate(df_final['hours']):
        if type(j) == str:
            df_final['hours'][i] = 0

    for i, j in enumerate(df_final['hoursTotals']):
        if type(j) == str:
            df_final['hoursTotals'][i] = 0

    df_final['week_dt'] = None
    for i, j in enumerate(df_final['week']):
        df_final['week_dt'][i] = pd.to_datetime(j)


    ### UPLOADING TO S3 BUCKET ###
    session = boto3.Session(region_name='us-east-1')
    s3 = session.client('s3',aws_access_key_id='', aws_secret_access_key='')
    bucketName = 'prof-dashboard'
    current_date = dt.datetime.now()
    c_date = current_date.strftime('%Y%m%d')

    historical_outfile = 'relus360_scheduler_raw/scheduler/ingestdate={0}/scheduler.parquet'.format(c_date)

    df_final.to_parquet('/tmp/scheduler.parquet', engine = "fastparquet", compression="gzip")
    s3.upload_file('/tmp/scheduler.parquet', bucketName, historical_outfile)

lambda_handler('a','a')
