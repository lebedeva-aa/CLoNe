import argparse

import mysql.connector
import sys
import os
import re
import datetime as dt
import requests
import errno
import shutil
import json
import random
import time
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import AODDB

SCOPES = ['https://www.googleapis.com/auth/spreadsheets',  'https://www.googleapis.com/auth/drive']

path = os.path.dirname(os.path.realpath(__file__))

def read_pipe_config():
    with open(PIPE_CONFIG_PATH, 'r') as config_file:
        return json.loads(config_file.read())

pipe_config = read_pipe_config()

config = AODDB.read_config()

def get_db_connection():
    return  mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['pwd'],
                              host=config['mysql']['host'],
                              database=config['mysql']['db'])

def create_spreadsheet(service, folder_id, analysis_id, panel_code):
    file_metadata = {
        'name': f"{analysis_id}.{panel_code}",
        'parents': [folder_id],
        'mimeType': 'application/vnd.google-apps.spreadsheet',

    }
    file = service.files().create(body=file_metadata,
                                    fields='id').execute()
    print('Spreadsheet created')
    print('Spreadsheet ID: %s' % file.get('id'))
    return file.get('id')

def create_sheet(service, name, spreadsheet_id):

    body = {
      "requests": [
        {
          "addSheet": {
            "properties": {
              "title": name
            }
          }
        }
      ]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def delete_base_sheet(service, spreadsheet_id):
    body = {
      "requests": [
        {
          "deleteSheet": {
              "sheetId": 0
          }
        }
      ]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def insert_values(service, sheet_name, values, spreadsheet_id):

    data_range = f"{sheet_name}!A1"

    body =  {
      "valueInputOption": "RAW",
        "data": [
        {
                "range": data_range,
                "majorDimension": "ROWS",
                "values": values
          }
        ],
    }
    #print(values[0][0])
    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    
def append_line(service, sheet_name, line, spreadsheet_id):
    
    data_range = f"{sheet_name}!A1"

    body =  {
      "range": data_range,
      "values": 
        [
            line.split('\t')
        ]
      
    }

    service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=data_range, valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body).execute()

def upload_to_spreadsheet(service, spreadsheet_id, barcode_name, panel_code, analysis_id):
    panel_data = pipe_config['upload_data'][panel_code]   
    for entry in panel_data:
        relative_path = entry[0]
        relative_path = relative_path.replace('$analysis_id', analysis_id)
        relative_path = relative_path.replace('./', '')
        sheet_name = entry[2]
        file_folder = config['data_path']['barcodePath']
        absolute_path = f'{file_folder}/{barcode_name}/{relative_path}'
        print(absolute_path)
        create_sheet(service, sheet_name, spreadsheet_id)
        with open(absolute_path) as f:
            lines = f.read().splitlines()
        data = [line.split('\t') for line in lines]
        insert_values(service, sheet_name, data, spreadsheet_id)

# Select * from Analysis where barcodeName = <указанный идентификатор баркода> and analysisRole = 'Major';

parser = argparse.ArgumentParser()
parser.add_argument('-a', help='Идентификатор анализа (таблица Analysis в MySQL)')
parser.add_argument('-b', help='Идентификатор баркода (таблица Barcode в MySQL)')
parser.add_argument('-r', help='Роль (варианты test[default]/major)')
args = parser.parse_args()
analysis_id = args.a
barcode_id = args.b
role = args.r

drive_service = AODDB.get_drive_service()
sheets_service = AODDB.get_sheets_service()

if role == 'major':
    insert_data = True
else:
    insert_data = False

if analysis_id is not None and barcode_id is not None:
    raise Exception

if barcode_id is not None:
    analysis_id = AODDB.Barcode(barcode_id).major_AN.info['analysisName']

if analysis_id is not None:
    #barcode_name = get_barcode_name(analysis_id)
    Analysis = AODDB.Analysis(analysis_id)
    Barcode = Analysis.Barcode
    print(f"Folder ID : '{Barcode.Case.Test_folder}'")
    spreadsheet_id = create_spreadsheet(drive_service, Barcode.Case.Test_folder, 
            analysis_id, Barcode.info['panelCode'])
    if insert_data:
        print('Adding to GDFile')
        command = f"DELETE FROM GDFile where analysisName = '{analysis_id}'"
        AODDB.execute(command)
        command = f"INSERT INTO GDFile (analysisName, fileKey, fileType) VALUES ('{analysis_id}', '{spreadsheet_id}', 'spreadsheet')"
        AODDB.execute(command)
    upload_to_spreadsheet(sheets_service, spreadsheet_id, 
            Barcode.info['barcodeName'], Barcode.info['panelCode'], analysis_id)
    delete_base_sheet(sheets_service, spreadsheet_id)
