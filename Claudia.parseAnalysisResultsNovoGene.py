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
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import AODDB
import subprocess
import time

path = os.path.dirname(os.path.realpath(__file__))

alphabet = { # this to translate column number to range name for Google Spreadsheets
        0: 'A',
        1: 'B',
        2: 'C',
        3: 'D',
        4: 'E',
        5: 'F',
        6: 'G',
        7: 'H',
        8: 'I',
        9: 'J',
        10: 'K',
        11: 'L',
        12: 'M',
        13: 'N',
        14: 'O',
        15: 'P',
        16: 'Q',
        17: 'R',
        18: 'S',
        19: 'T',
        20: 'U',
        21: 'V',
        22: 'W',
        23: 'X',
        24: 'Y',
        25: 'Z'
        }

def get_name(chromosome, position, ref, alt):
    name = ''
    if (ref == '-'):
        name = chromosome + ":" + str(position) + seq.decode('utf-8') + ">" + seq.decode('utf-8') + alt
    elif (alt == '-'):
        position = position - 1
        name = chromosome + ":" + str(position) + seq.decode('utf-8') + ref + ">" + seq.decode('utf-8')
    else:
        name = chromosome + ":" + str(position) + ref + ">" + alt
    return(name)


def run_through(service, SSID):
    analysis_name = AODDB.select_single(f"SELECT analysisName FROM GDFile where fileKey = '{SSID}'")
    Analysis = AODDB.Analysis(analysis_name)
    
    print(analysis_name)
    Table = AODDB.call_sheets_api(service, SSID, 'SNV')
    header = AODDB.parse_TableHeader(Table)
    
    tmp_list_file = AODDB.read_config()['data_path']['tmpPath'] + '/' + AODDB.get_seed_string(16)
    tmp_vcf_file = tmp_list_file + '.vcf'
    tmp_list_file = tmp_list_file + '.list'

    content = subprocess.check_output(f"echo -n > {tmp_list_file}", shell=True)
    for number, values in enumerate(Table):
        if (number  == 0):
            continue
        gs_line = AODDB.gs_line(service, SSID, 'SNV', header, values, number)
        name = get_name(('chr' + str(gs_line.field['Chr'])), int(gs_line.field['Start']), gs_line.field['Ref'], gs_line.field['Alt'])
        gs_line.update_single(name, 'Chromosome')
        content = subprocess.check_output(f"echo '{name}' >> {tmp_list_file}", shell=True)
    subprocess.check_output(f"rm -f {tmp_list_file} {tmp_vcf_file}", shell = True)
    
    Table = AODDB.call_sheets_api(service, SSID, 'SNV')
    header = AODDB.parse_TableHeader(Table)
    for number, values in enumerate(Table):
        if (number == 0):
            continue
        gs_line = AODDB.gs_line(service, SSID, 'SNV', header, values, number)
        Mutation = AODDB.Mutation(gs_line.field['Chromosome'])
        if (('M_ORIGIN' in gs_line.field)and(gs_line.field['M_ORIGIN'])):
            MR_content = dict()
            MR_content['mutationId'] =  Mutation.info['mutationId']
            MR_content['analysisName'] = analysis_name
            MR_content['qual'] = 255
            MR_content['filter'] = 'PASS'
            MR_content['depth'] = int(gs_line.field['TotalDepth'])
            MR_content['alleleFrequency'] = int(gs_line.field['MutDepth'])/int(gs_line.field['TotalDepth'])
            MR_content['zygosityCurated'] = gs_line.field['M_ORIGIN']
            mrId = AODDB.insert_single('MutationResult', MR_content)
            if ((mrId)and(isinstance(mrId, int))):
                continue
            else:
                MR = AODDB.Analysis(analysis_name).MutationResult(Mutation.info['mutationId'])
                if (MR.exists):
                    AODDB.update_single('MutationResult', {'zygosityCurated': gs_line.field['M_ORIGIN']}, 'mutationResultId', MR.info['mutationResultId'])

            #mutation_id = Mutation.info['mutationId']
            #DBCOUNT = AODDB.select_single(f"select COUNT(*) FROM MutationResult INNER JOIN Analysis ON Analysis.analysisName = MutationResult.analysisName INNER JOIN Barcode ON Barcode.barcodeName = Analysis.barcodeName WHERE mutationId = {mutation_id} and panelCode = 'NOVOPMV2';")
            #print (f"select COUNT(*) FROM MutationResult INNER JOIN Analysis ON Analysis.analysisName = MutationResult.analysisName INNER JOIN Barcode ON Barcode.barcodeName = Analysis.barcodeName WHERE mutationId = {mutation_id} and panelCode = 'NOVOPMV2';")
            #print (DBCOUNT)
            #gs_line.update_single(DBCOUNT, 'DB_count')
        rs_id = ''
        if (Mutation.info['mutationRs']):
            rs_id = Mutation.info['mutationRs']
        gs_line.update_single(rs_id, 'dbSNP')
        mutation_id = Mutation.info['mutationId']
        print (DBCOUNT)
        gs_line.update_single(DBCOUNT, 'DB_count')

    Table = AODDB.call_sheets_api(service, SSID, 'CNV')
    header = AODDB.parse_TableHeader(Table)
    for number, values in enumerate(Table):
        if (number  == 0):
            continue
        gs_line = AODDB.gs_line(service, SSID, 'CNV', header, values, number)
        if (float(gs_line.field['Copy_Number']) < 5):
            continue
        gene_id = AODDB.Gene(gs_line.field['Gene']).info['ezGeneId']
        CNV_id = AODDB.select_single(f"SELECT CNVId FROM CNV WHERE ezGeneId = '{gene_id}' AND type = 'amp'")
        if not (CNV_id):
            content = dict()
            content['ezGeneId'] = gene_id
            content['type'] = 'amp'
            AODDB.insert_single('CNV', content)
            CNV_id = AODDB.select_single(f"SELECT CNVId FROM CNV WHERE ezGeneId = '{gene_id}' AND type = 'amp'")
        content = dict()
        content['analysisName'] = analysis_name
        content['CNVId'] = CNV_id
        content['fraction'] = float(gs_line.field['Copy_Number'])
        content['depth'] = 1000*float(gs_line.field['Copy_Number'])
        AODDB.insert_single('CNVResult', content)



def read_config():
        return json.loads(config_file.read())

config = read_config()

def get_db_connection():
    return mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['pwd'],
                              host=config['mysql']['host'],
                              database=config['mysql']['db'])


def prepare_creds():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(f'{path}/token.json'):
        creds = Credentials.from_authorized_user_file(f'{path}/token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        print("Creating creds")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                f'{path}/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(f'{path}/token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def get_sheets_service():
    creds = prepare_creds()
    sheets_service = build('sheets', 'v4', credentials=creds)
    return sheets_service

def call_sheets_api(service, spreadsheet_id, WS_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range=WS_name).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        return values

def get_range_by_number(row, col_num):
        row += 1
        col_num += 1
        col_chr =  chr(64 + col_num)
        range_name = str(col_chr) + str(row)
        return range_name

def update_cell(service, spreadsheet_id, range_name, value):
    cells = [
        [
            value
        ]
    ]
    body = {
        'values': cells
    }      
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, 
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))

# Почему приоритет?
# Добавить поиск по кейсу
def get_case_by_internal_barcode(internal_barcode):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    # Not found?
    cursor.execute(query)
    case_name = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return case_name

def get_full_name_by_case(case):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    query = f"select patientId from `Case` where caseName = '{case}'"
    # Not found?
    cursor.execute(query)
    patient_id = cursor.fetchone()[0]
    cursor.close()
    cursor = cnx.cursor()
    query = f"select patientFamilyName, patientGivenName, patientAddName from Patient where patientId = '{patient_id}'"
    # Not found?
    cursor.execute(query)
    name_array = cursor.fetchone()
    # Check for nulls
    full_name = ' '.join(name_array)
    cursor.close()
    cnx.close()
    return full_name

def get_folder_by_case(case):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    query = f"select fileKey from GDFile where caseName = '{case}' and fileType = 'folder'"
    cursor.execute(query)
    folder =  cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return folder

def get_analysis_from_GDF(key):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    query = f"select analysisName from GDFile where fileKey = '{key}'"
    cursor.execute(query)
    analysisName = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return analysisName

def def_pos_from_HEADER(SNV, name): # это нужно в отдельный модуль вынести
    for number,value in enumerate(SNV[0]):
        if value.lower() == name.lower():
            return number

def update_MORIGIN(analysisName, mutationId, zygosity):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    return 0

def parseMutation(mutationName): # это нужно в отдельный модуль вынести
    match = re.search(r'^([^:;>]+):(\d+)([AGCTNagctn]+)>([AGCTNagctn]+)$', mutationName)
    if match:
        return {'chr': match.group(1).lower(), 'pos': int(match.group(2)), 'ref': match.group(3).upper(), 'alt': match.group(4).upper()}
    else:
        return match

def getMutationName(Mutation): # это нужно в отдельный модуль вынести
    return (Mutation['chr']+":"+str(Mutation['pos'])+Mutation['ref']+">"+Mutation['alt'])

def get_mutationId_by_name(mutationName): # это нужно в отдельный модуль вынести
    Mutation = parseMutation(mutationName)
    cnx = get_db_connection()
    cursor = cnx.cursor()
    mChr = Mutation['chr']
    mPos = Mutation['pos']
    mRef = Mutation['ref']
    mAlt = Mutation['alt']
    cursor.execute(query)
    mutationId = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return mutationId
    
def M_ORIGIN_to_SQL(analysisName, SNV):
    M_ORIGIN_pos = def_pos_from_HEADER(SNV, 'M_ORIGIN')
    Mutation_name_pos = def_pos_from_HEADER(SNV, 'Chromosome')
    for number, line in enumerate(SNV):
        Mutation = parseMutation(line[Mutation_name_pos])
        if not Mutation:
            continue
        update_MORIGIN(analysisName, get_mutationId_by_name(getMutationName(Mutation)), line[M_ORIGIN_pos])

def check_QC(QC):
    if not QC[0]:
        return 'FAIL'
    if len(QC[0]) < 2:
        return 'FAIL'
    if QC[0][1].rstrip() == '##PASS':
        return 'PASS'
    else:
        return 'FAIL'

def read_config():
    with open('/home/onco-admin/ATLAS_software/aod-admin/conf/Config.json', 'r') as config_file: # убрать из кода абсолютные пути к файлам
        return json.loads(config_file.read())

def getPH(gene):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    cursor.execute(query)
    res = []
    #phenotypes = cursor.fetchone()[0]
    for (phId) in cursor:
        res.append(phId[0])
    cursor.close()
    cnx.close()
    return res

def getPhenotypeName(phId):
    cnx = get_db_connection()
    query = f"select phenotypeName_r from Phenotype where phenotypeId = '{phId}';"
    cursor = cnx.cursor()
    cursor.execute(query)
    phName = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return phName
    
def parseVariantFromCI(variant):
    match = re.search(r'^([^:;>]+:\d+[AGCTNagctn]+>[AGCTNagctn]+):(\D+)$', variant)
    if match:
        return (match.group(1))
    else:
        return match


def updateCI(SNV, CI): # найти, какие элементы нужно добавить в таблицу CI
    SNV_M_ORIGIN_pos = def_pos_from_HEADER(SNV, 'M_ORIGIN')
    SNV_GENE_pos = def_pos_from_HEADER(SNV, 'Gene')
    SNV_Mutation_name_pos = def_pos_from_HEADER(SNV, 'Chromosome')
    
    CI_variant_pos = def_pos_from_HEADER(CI, 'Variant')
    CI_phId_pos = def_pos_from_HEADER(CI, 'PhenotypeId')
    result = []
    for numberSNV, lineSNV in enumerate(SNV):
        if len(lineSNV) <= max(SNV_M_ORIGIN_pos, SNV_GENE_pos, SNV_Mutation_name_pos):
            continue
        if not (lineSNV[SNV_M_ORIGIN_pos].lower() == 'germline_het' or lineSNV[SNV_M_ORIGIN_pos].lower() == 'variant_nos'):
            continue
        for Phenotype in getPH(lineSNV[SNV_GENE_pos]):
            found = 0
            for numberCI, lineCI in enumerate(CI):
                if len(lineCI) <= max(CI_variant_pos, CI_phId_pos):
                    continue
                if not lineCI[CI_variant_pos]:
                    continue
                if not lineCI[CI_phId_pos]:
                    continue
                if not parseVariantFromCI(lineCI[CI_variant_pos]):
                    continue
                if parseVariantFromCI(lineCI[CI_variant_pos]).lower() == lineSNV[SNV_Mutation_name_pos].lower() and lineCI[CI_phId_pos] == str(Phenotype):
                    found = 1
                    break
            if found == 0:
                result.append([Phenotype, lineSNV[SNV_GENE_pos], lineSNV[SNV_Mutation_name_pos]])
    return result
        
def get_playerName_by_analysisName(analysisName):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    cursor.execute(query)
    playerName = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return playerName



# Сделать метод для получения спика таблиц
config = AODDB.read_config()
service = AODDB.get_sheets_service()

parser = argparse.ArgumentParser()
parser.add_argument('-k', help='Google Spreadsheet ID')
args = parser.parse_args()
SSID = args.k

run_through(service, SSID)

SNV = call_sheets_api(service, SSID, 'SNV')
CI = call_sheets_api(service, config['drive']['files']['interpretation']['key'], 'NOS')
analysisName = get_analysis_from_GDF(SSID)
index = 1
for phId, Gene, variant in updateCI(SNV, CI):
    values = [
        [
           'load',
           get_playerName_by_analysisName(analysisName),
           'Current',
           'Current',
            '',
            variant + ':germline_het',
            phId,
            getPhenotypeName(phId),
            '',
            Gene,
            AODDB.Mutation(variant).info['mutationRs']
        ],
    ]
    body = {
        'values': values
    }
    range_name = 'A' + str(len(CI) + index) + ':K' + str(len(CI) + index)
    result = service.spreadsheets().values().update(
        spreadsheetId=config['drive']['files']['interpretation']['key'], range=range_name,
        valueInputOption='RAW', body=body).execute()
    index += 1


sys.exit()
