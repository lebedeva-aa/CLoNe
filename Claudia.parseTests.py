import mysql.connector
import os
import re
import datetime as dt
import requests
import errno
import shutil
import json
import random
import sys
import argparse
from pprint import pprint
import re
import AODDB
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from datetime import datetime
import subprocess
import Atlas
import codecs


path = os.path.dirname(os.path.realpath(__file__))

def run_through_update(gs_line, playerName):
    
    Case = gs_line.get_value('internalbarcodeid')
    Case = AODDB.Case(Case)
    
    match = re.search(r'(\d\d\d\d\d-\d\d-\d\d-\D\D)', gs_line.get_value('AnalysisName'))
    if match:
        match = str(match.group(0))
    Analysis = AODDB.Analysis(match)
    if Analysis.Barcode.Case.caseName != Case.caseName:
        raise Exception(f"FAIL:Wrong analysis - Analysis {match} does not belong to Case {Case.caseName}")
    
    inputId = Analysis.GDFile.info['fileKey']
    try:
        if (Analysis.Barcode.info['panelCode'] == 'NOVOPMV2'):
            cmd = f"python3 {path}/Claudia.parseAnalysisResultsNovoGene.py -k '{inputId}'"
            print(cmd)
            inputId = subprocess.check_output(f"python3 {path}/Claudia.parseAnalysisResultsNovoGene.py -k '{inputId}'", shell=True)
        else:
            inputId = subprocess.check_output(f"python3 {path}/Claudia.parseAnalysisResults.py -k '{inputId}'", shell=True)
        gs_line.update(str(Case.ClinicalInterpretation.TMB()), 'TMB')
    except subprocess.CalledProcessError as e:
        raise Exception("FAILED: try to check ##PASS at QC.decision")

    return 0

    

def run_through(service, SSID, playerName):
    Table = AODDB.call_sheets_api(service, SSID, 'Tests')
    header = AODDB.parse_TableHeader(Table)

    for number, values in enumerate(Table):
        gs_line = AODDB.gs_line(service, SSID, 'Tests', header, values, number)
        if not 'dbStatus' in gs_line.field:
            continue
        if (gs_line.field['dbStatus'] == 'in progress...'):
            gs_line.msg('FAILED: unknown error, try to repeat the command')
            continue

        if (gs_line.field['dbStatus'] == 'update'):
            gs_line.msg('in progress...')
            try:
                run_through_update(gs_line, playerName)
                gs_line.msg('done')
            except Exception as e:
                gs_line.msg(e)
            continue


config = AODDB.read_config()
service = AODDB.get_sheets_service()
for Player in AODDB.element_array('Player'):
    if Player.info['status'] != 'active':
        continue
    if Player.info['role_code'] != 'Interp':
        continue
    for PlayerTool in Player.PlayerTools:
        if PlayerTool.info['playerToolCode'] != 'GSHEET_I':
            continue
        SSID = PlayerTool.PlayerToolField('GSHEET_key').info['playerToolFieldValue']
        run_through(service, SSID, Player.info['playerName'])


sys.exit()













sys.exit();
