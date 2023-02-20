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
from datetime import date
from datetime import datetime


path = os.path.dirname(os.path.realpath(__file__))

guidelineDic = {}
guidelineDic['NCCN'] = 1;
guidelineDic['ESMO'] = 1;
guidelineDic['ASCO'] = 1;
guidelineDic['RUSSCO'] = 1;

def validateAD(gs_line):
    values = re.split(';|,| |\*|\n',gs_line.get_value('accompanyingDisease'))
    values = [v.strip() for v in values]
    res = []
    for V in values:
        if len(V) < 1:
            continue
        Pathology = AODDB.Pathology(V)
        if not Pathology.info:
            return None
        res.append(V)
    return res

def validate_for_update(gs_line):
    Case = gs_line.get_value('internalbarcodeid')
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')
    Case = AODDB.Case(Case)
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')

    for fieldName in REQUIRED_for_update:
        if not gs_line.is_exist(fieldName):
            raise Exception(f"FAILED: Field {fieldName} can not be empty ('NA' is accepted)")

    if gs_line.get_value('accompanyingDisease'):
        if not validateAD(gs_line):
            raise Exception('FAILED: accompanyingDisease - wrong input or unknown diseases')

    for fieldName in ['pathologyCodeBaseline', 
            'pathologyCodeResult', 
            'pathologyCodePurpose']:
        if gs_line.get_value(fieldName):
            if not (AODDB.Pathology(gs_line.get_value(fieldName)).info):
                raise Exception(f'FAILED: {fieldName} - wrong input or unknown diseases')

def create_spreadsheet_in_barcode(analysis_name, folder_id, service, surname = ''):
    file_metadata = {
        'name': analysis_name,
        'parents': [folder_id],
        'mimeType': 'application/vnd.google-apps.spreadsheet',

    }
    file = service.files().create(body=file_metadata,
                                    fields='id').execute()
    print('Spreadsheet created')
    sheet_id = file.get('id')
    print('Spreadsheet ID ', sheet_id)
    return sheet_id


def run_through_generateNG(gs_line, playerName):
    Case = gs_line.get_value('internalbarcodeid')
    Case = AODDB.Case(Case)
    
    patient_id = Case.info['patientId']
    barcode_name = AODDB.select_single(f"SELECT barcodeName FROM Barcode WHERE patientId = '{patient_id}' AND panelCode = 'NOVOPMV2'")
    if not (barcode_name):
        known_barcodes = []
        for i in range(1,9):
            known_string = "''"
            for b in known_barcodes:
                known_string = known_string + f", '{b}'"
            command = f"SELECT barcodeName FROM Barcode WHERE patientId = '{patient_id}' and barcodeName not in ({known_string}) limit 1;"
            print(command)
            barcode_name = AODDB.select_single(command)
            if not (barcode_name):
                break
            else:
                known_barcodes.append(barcode_name)
        barcode_name = Case.info['caseName'] + '-0' + str(i)
        barcode_content = dict()
        barcode_content['barcodeId'] = '0' + str(i)
        barcode_content['caseId'] = Case.info['caseId']
        barcode_content['patientId'] = Case.info['patientId']
        barcode_content['panelCode'] = 'NOVOPMV2'
        barcode_content['isPairedEnd'] = '1'
        barcode_content['patientCheck'] = '1'
        barcode_content['dataAcquisitionDate'] = date.today().strftime("%Y-%m-%d") + ' ' + datetime.now().strftime("%H:%M:%S")
        barcode_content['libraryTag'] = 'NOVOGENE'
        AODDB.insert_single('Barcode', barcode_content)

        libQC_content = dict()
        libQC_content['barcodeName'] = barcode_name
        libQC_content['result'] = 'PASS'
        libQC_content['analysisVersion'] = 'LEGACY'
        AODDB.insert_single('LibraryQC', libQC_content)

        CONFIG = AODDB.read_config()
        os.mkdir(CONFIG['data_path']['barcodePath'] + '/' + barcode_name)
        os.mkdir(CONFIG['data_path']['barcodePath'] + '/' + barcode_name + '/raw')
        #raise Exception(f"Cant find barcode")
    Barcode = AODDB.Barcode(barcode_name)
    
    analysis_name = AODDB.select_single(f"SELECT analysisName FROM Analysis WHERE barcodeName = '{barcode_name}'")
    if (analysis_name):
        raise Exception(f"I have already added, cant do this twice for the same patient")
    analysis_name = barcode_name + '-AA'
    analysis_content = dict()
    analysis_content['analysisId'] = 'AA'
    analysis_content['barcodeName'] = barcode_name
    analysis_content['analysisCode'] = 'manual'
    analysis_content['analysisRole'] = 'Major'

    AODDB.insert_single('Analysis', analysis_content)
    test_folder_id = AODDB.get_Test_folder(Case.GDFile.info['fileKey'])
    analysis_file_id = AODDB.create_empty_spreadsheet((analysis_name + '.NOVOPMV2'), test_folder_id)
    AODDB.add_worksheet(analysis_file_id, 'SNV')
    AODDB.add_worksheet(analysis_file_id, 'CNV')
    AODDB.add_worksheet(analysis_file_id, 'SV')
    AODDB.delete_base_sheet(analysis_file_id)
    GDfile_content = dict()
    GDfile_content['fileKey'] = analysis_file_id
    GDfile_content['analysisName'] = analysis_name
    GDfile_content['fileType'] = 'spreadsheet'
    AODDB.insert_single('GDFile', GDfile_content)
    for Barcode in Case.Barcodes:
        if not Barcode.major_AN:
            continue
        if (not(Barcode.major_AN.hyperlink_for_gs)):
            raise Exception(f"analysis {Barcode.major_AN.info['analysisName']} not loaded to GDrive")
        AODDB.GT_add_unique_line(service, SSID, 'Tests',
                {"internalbarcodeid": Case.InternalBarcode.info['internalBarcodeId'],
                    "Folder": Case.hyperlink_for_gs,
                    "FullName": Case.Patient.patientName,
                    "AnalysisName": Barcode.major_AN.hyperlink_for_gs},
                ["internalbarcodeid", "AnalysisName"])


    


def run_through_update(gs_line, playerName):
    
    validate_for_update(gs_line)

    Case = gs_line.get_value('internalbarcodeid')
    Case = AODDB.Case(Case)
    
    gs_line.update(Case.Patient.patientName, 'Full name', valueInputOption = 'RAW')
    gs_line.update(Case.hyperlink_for_gs, 'Folder')
    
    res = Case.BaselineStatus.update(
            {"caseName": Case.caseName,
                "diagnosisMain": gs_line.get_value('diagnosismain'),
                "pathologyCodeBaseline": gs_line.get_value('pathologyCodeBaseline'),
                "T": gs_line.get_value('T'),
                "N": gs_line.get_value('N'),
                "M": gs_line.get_value('M'),
                "Stage": gs_line.get_value('Stage'),
                "histologicalDisease": gs_line.get_value('histologicalDisease'),
                "tumorStatusCode": Atlas.TumorStatusDic[gs_line.get_value('tumorStatusCode').lower()],
                "familyBurden": gs_line.get_value('familyBurden'),
                "ECOG": gs_line.get_value('ECOG'),
                "surgicalTreatment": gs_line.get_value('surgicalTreatment'),
                "drugTreatment": gs_line.get_value('drugTreatment'),
                "radiotherapyTreatment": gs_line.get_value('radiotherapyTreatment'),
                "diagnosisYear": Atlas.split_date(gs_line.get_value('dateOfDiagnosis'))[0],
                "diagnosisMonth": Atlas.split_date(gs_line.get_value('dateOfDiagnosis'))[1],
                "diagnosisDay": Atlas.split_date(gs_line.get_value('dateOfDiagnosis'))[2],
                }, forceInsert = True)
    if res:
        raise Exception(res)
    res = Case.update(
            {"mgtTypeCode": Atlas.mgtTypeDic[gs_line.get_value('specimenType').lower()],
                "profileDateYear": Atlas.split_date(gs_line.get_value('tumorSamplingDate'))[0],
                "profileDateMonth": Atlas.split_date(gs_line.get_value('tumorSamplingDate'))[1],
                "profileDateDay": Atlas.split_date(gs_line.get_value('tumorSamplingDate'))[2]
                }
            )
    if res:
        raise Exception(res)
    res = Case.ClinicalInterpretation.update(
            {"caseName": Case.caseName,
                "pathologyCodePurpose": gs_line.get_value('pathologyCodePurpose'),
                "interpretationVersion": config['general']['interpretationVersion']
                }, forceInsert = True)
    if res:
        raise Exception(res)
    if gs_line.get_value('pathologyCodeResult'):
        res = Case.PathoResult.update(
                {"caseName": Case.caseName,
                    'pathologyCodeResult': gs_line.get_value('pathologyCodeResult')}, 
                forceInsert = True)
        if res:
            raise Exception(res)
    elif Case.PathoResult.info:
        AODDB.delete_by_value('PathoResult', 'caseName', Case.caseName)
    if gs_line.get_value('accompanyingDisease'):
        Case.BaselineStatus.update_AD(validateAD(gs_line))
    elif len(Case.BaselineStatus.AccompanyingDiseases) > 0:
        AODDB.delete_by_value('AccompanyingDisease', 'baselineStatusId', 
                Case.BaselineStatus.info['baselineStatusId'])

    for Barcode in Case.Barcodes:
        if not Barcode.major_AN:
            continue
        if (not(Barcode.major_AN.hyperlink_for_gs)):
            raise Exception(f"analysis {Barcode.major_AN.info['analysisName']} not loaded to GDrive")
        AODDB.GT_add_unique_line(service, SSID, 'Tests', 
                {"internalbarcodeid": Case.InternalBarcode.info['internalBarcodeId'],
                    "Folder": Case.hyperlink_for_gs,
                    "FullName": Case.Patient.patientName,
                    "AnalysisName": Barcode.major_AN.hyperlink_for_gs},
                ["internalbarcodeid", "AnalysisName"])

    return 0

def run_through_load(gs_line):
    Case = gs_line.get_value('internalbarcodeid')
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')
    Case = AODDB.Case(Case)
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')
    
    BS = Case.BaselineStatus
    gs_line.update({
        "diagnosismain": (BS.field_value('diagnosisMain') or 'N/A'),
        "pathologyCodeBaseline": (BS.field_value('pathologyCodeBaseline') or 'N/A'),
        "pathologyCodeResult": (Case.PathoResult.field_value('pathologyCodeResult') or 'N/A'),
        "pathologyCodePurpose": (Case.ClinicalInterpretation.field_value('pathologyCodePurpose') or 'N/A'),
        "accompanyingDisease": ((', '.join([el.field_value('pathologyCode') for el in BS.AccompanyingDiseases])).upper() or 'N/A'),
        "T": (BS.field_value('T') or ''),
        "N": (BS.field_value('N') or ''),
        "M": (BS.field_value('M') or ''),
        "Stage": (BS.field_value('Stage') or ''),
        "tumorSamplingDate": ('-'.join([str(el) for el in filter(None, 
            [Case.field_value('profileDateYear'), 
                Case.field_value('profileDateMonth'), 
                Case.field_value('profileDateDay')])]) or 'N/A'),
        "specimenType": ({str(v).lower(): str(k).title() for k, v in Atlas.mgtTypeDic.items()}[Case.field_value('mgtTypeCode').lower()] if Case.field_value('mgtTypeCode') else 'N/A'),
        "dateOfDiagnosis": ('-'.join([str(el) for el in filter(None, 
            [BS.field_value('diagnosisYear'), 
                BS.field_value('diagnosisMonth'), 
                BS.field_value('diagnosisDay')])]) or 'N/A'),
        "tumorStatusCode": ({str(v).lower(): str(k).title() for k, v in Atlas.TumorStatusDic.items()}[BS.field_value('tumorStatusCode').lower()] if BS.field_value('tumorStatusCode') else 'N/A'),
        "histologicalDisease": (BS.field_value('histologicalDisease') or ''),
        "familyBurden": (BS.field_value('familyBurden') or ''),
        "ECOG": (BS.field_value('ECOG') or ''),
        "surgicalTreatment": (BS.field_value('surgicalTreatment') or ''),
        "drugTreatment": (BS.field_value('drugTreatment') or ''),
        "radiotherapyTreatment": (BS.field_value('radiotherapyTreatment') or '')
        })
    gs_line.update(Case.Patient.patientName, 'Full name', valueInputOption = 'RAW')
    gs_line.update(Case.hyperlink_for_gs, 'Folder')

    return 0

def encode_RCT(gs_line, playerName):
    TableCT = AODDB.call_sheets_api(gs_line.service, gs_line.SSID, 'ClinicalTrial')
    headerCT = AODDB.parse_TableHeader(TableCT)
    for number, values in enumerate(TableCT):
        gs_line_CT = AODDB.gs_line(gs_line.service, gs_line.SSID, 'ClinicalTrial', headerCT, values, number)
        if gs_line_CT.get_value('internalbarcodeid') != gs_line.get_value('internalbarcodeid'):
            continue
        Case = AODDB.Case(gs_line_CT.get_value('internalbarcodeid'))
        
        if gs_line_CT.get_value('Variant') is None:
            raise Exception("Empty Variant column in ClinicalTrial list")
        if ',' in gs_line_CT.get_value('Variant'):
            Variant = gs_line_CT.get_value('Variant')
            raise Exception(f"FAILED: In clinical trial list, Variant {Variant}: ',' is not allowed. Use '+' for co-biomarkers and ';' for several independent biomarkers")

        for marker in (re.split(';',gs_line_CT.get_value('Variant'))):
            content = gs_line_CT.field
            content['Variant'] = marker
            content = {str(k).lower(): v for k, v in content.items()}
            content = json.dumps(content)
            content = subprocess.check_output(f"perl {path}/prepareRCT.pl '{content}'", shell=True)
            content = json.loads(content)
    
            if content['message']:
                raise Exception(content['message'])
    
            for RCT in content['result']:
                res = AODDB.insert_single('RecommendationCT',
                        {"clinicalInterpretationId": Case.ClinicalInterpretation.info['clinicalInterpretationId'],
                            "molecularTargetId": (RCT['moleculartargetid'] if 'moleculartargetid' in RCT.keys() else None),
                            "treatmentSchemeId": RCT['treatmentschemeid'],
                            "NCTid": RCT['NCTid'],
                            "molecularTargetTitle": RCT['moleculartargettitle'],
                            "recommendationDescription": RCT['recommendationdescription'],
                            "markerName": (RCT['markername'] if 'markername' in RCT.keys() else None)
                            })

def encode_RTP(gs_line, playerName):
    TableTP = AODDB.call_sheets_api(gs_line.service, gs_line.SSID, 'Biomarkers')
    headerTP = AODDB.parse_TableHeader(TableTP)
    for number, values in enumerate(TableTP):
        gs_line_TP = AODDB.gs_line(gs_line.service, gs_line.SSID, 'Biomarkers', headerTP, values, number)
        if gs_line_TP.get_value('internalbarcodeid') != gs_line.get_value('internalbarcodeid'):
            continue
        Case = AODDB.Case(gs_line_TP.get_value('internalbarcodeid'))
        
        if ((gs_line_TP.get_value('Mutation')) and (',' in gs_line_TP.get_value('Mutation'))):
            Mutation = gs_line_TP.get_value('Mutation')
            raise Exception(f"FAILED: In Biomarkers list, Mutation {Mutation}: ',' is not allowed. Use '+' for co-biomarkers and ';' for several independent biomarkers")
        
        mas = []
        if (gs_line_TP.get_value('Mutation')):
            mas = re.split(';',gs_line_TP.get_value('Mutation'))
        else:
            mas = [None]

        for marker in mas:
            content = gs_line_TP.field
            if marker:
                content['Mutation'] = marker
            content = {str(k).lower(): v for k, v in content.items()}
            content = json.dumps(content)
            content = subprocess.check_output(f"perl {path}/prepareRTP.pl '{content}'", shell=True)
            content = json.loads(content)

            if content['message']:
                raise Exception(content['message'])

            for RTP in content['result']:
                res = AODDB.insert_single('RecommendationTP',
                        {"clinicalInterpretationId": Case.ClinicalInterpretation.info['clinicalInterpretationId'],
                            "molecularTargetId": (RTP['moleculartargetid'] if ('moleculartargetid' in RTP.keys() and len(RTP['moleculartargetid']) > 0) else None),
                            "treatmentSchemeId": RTP['treatmentschemeid'],
                            "therapyRecommendationType": RTP['therapyrecommendationtype'],
                            "confidenceLevel": RTP['confidencelevel'],
                            "description": RTP['description'],
                            "markerName": (RTP['markername'] if ('markername' in RTP.keys() and len(RTP['markername']) > 0) else None)
                            })
                resRTP = AODDB.RecommendationTP(res)
                for i in range(1, 9):
                    if not gs_line_TP.get_value(f'Reference{i}'):
                        continue
                    Reference = gs_line_TP.get_value(f'Reference{i}').lower()
                    match = re.search(r'doi:(\S+)', Reference)
                    if match:
                        Reference = match.group(1)
                        Reference = AODDB.ReferenceDic(Reference)
                        if not Reference.info:
                            if AODDB.getCitation(match.group(1)):
                                Reference = AODDB.insert_single('ReferenceDic',
                                        {'doi': match.group(1)})
                                Reference = AODDB.ReferenceDic(Reference)
                        if Reference.info:
                            resRTP.addReference(Reference)
                        else:
                            raise Exception(f"FAILED: Table Biomarker, reference {gs_line_TP.get_value(f'Reference{i}')} is unknown")
                if gs_line_TP.get_value('GLine'):
                    GDLines = re.split(';|,', gs_line_TP.get_value('GLine'))
                    for GDLine in GDLines:
                        GDLine = str(GDLine).strip()
                        if not GDLine in guidelineDic.keys():
                            raise Exception(f"FAILED: Table Biomarker - Guideline {GDLine} does not exist. Possible values: {guidelineDic.keys()}")
                        resRTP.addGuideline(GDLine)

def encode_RGC(gs_line, playerName):
    TableGC = AODDB.call_sheets_api(gs_line.service, gs_line.SSID, 'NOS')
    headerGC = AODDB.parse_TableHeader(TableGC)
    for number, values in enumerate(TableGC):
        gs_line_GC = AODDB.gs_line(gs_line.service, gs_line.SSID, 'NOS', headerGC, values, number)
        if gs_line_GC.get_value('internalbarcodeid') != gs_line.get_value('internalbarcodeid'):
            continue
        Case = AODDB.Case(gs_line_GC.get_value('internalbarcodeid'))

        if ',' in gs_line_GC.get_value('Variant'):
            Variant = gs_line_GC.get_value('Variant')
            raise Exception(f"FAILED: In NOS list, Variant {Variant}: ',' is not allowed. Use '+' for co-biomarkers and ';' for several independent biomarkers")
        Mutation = AODDB.Mutation(gs_line_GC.get_value('Variant'))
        if not Mutation.info:
            Variant = gs_line_GC.get_value('Variant')
            raise Exception(f"FAILED: In NOS list, Variant {Variant} unknown")
        if (Case.VariantZygosity(Mutation)):
            MT = AODDB.MolecularTarget(str(Mutation.mutationName()) + ":" + (Case.VariantZygosity(Mutation)))
        else:
            raise Exception(f"Не указана зиготность варианта {str(Mutation.mutationName())}. Попробуйте повторить update на листе tests для всех баркодов этого пациента, укажите на всех соответстующих листах SNV.good гугл таблиц зиготность")
        res = AODDB.insert_single('RecommendationGC',
            {"clinicalInterpretationId": Case.ClinicalInterpretation.info['clinicalInterpretationId'],
                "molecularTargetId": MT.info['molecularTargetId']})




def run_through_encode(gs_line, playerName):
    Case = gs_line.get_value('internalbarcodeid')
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')
    Case = AODDB.Case(Case)
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')

    CI = Case.ClinicalInterpretation
    res = Case.ClinicalInterpretation.update(
            {"caseName": Case.caseName,
                "playerName": playerName,
                }, forceInsert = True)
    CI.purgeRCT()
    CI.purgeRTP()
    CI.purgeRGC()
    encode_RCT(gs_line, playerName)
    encode_RTP(gs_line, playerName)
    encode_RGC(gs_line, playerName)
    return 0

def run_through_report(gs_line, playerName):
    Case = gs_line.get_value('internalbarcodeid')
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')
    Case = AODDB.Case(Case)
    if not Case:
        raise Exception('FAILED: unknown barcode at internalbarcodeid')
    
    CI = Case.ClinicalInterpretation
    CI.report()
    return 0



def run_through(service, SSID, playerName):
    TableClinical = AODDB.call_sheets_api(service, SSID, 'Clinical characteristics')
    headerClinical = AODDB.parse_TableHeader(TableClinical)

    for number, values in enumerate(TableClinical):
        gs_line = AODDB.gs_line(service, SSID, 'Clinical characteristics', headerClinical, values, number)
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
        if (gs_line.field['dbStatus'] == 'load'):
            gs_line.msg('in progress...')
            try:
                run_through_load(gs_line)
                gs_line.msg('done')
            except Exception as e:
                gs_line.msg(e)
            continue
        if (gs_line.field['dbStatus'] == 'encode'):
            gs_line.msg('in progress...')
            try:
                run_through_encode(gs_line, playerName)
                gs_line.msg('done')
            except Exception as e:
                gs_line.msg(e)
            continue
        if (gs_line.field['dbStatus'] == 'report'):
            gs_line.msg('in progress...')
            try:
                run_through_report(gs_line, playerName)
                gs_line.msg('done')
            except Exception as e:
                gs_line.msg(e)
            continue
        if (gs_line.field['dbStatus'] == 'generate NG'):
            gs_line.msg('in progress...')
            #try:
            run_through_generateNG(gs_line, playerName)
            gs_line.msg('done')
            #except Exception as e:
            #    gs_line.msg(e)
            continue

config = AODDB.read_config()
service = AODDB.get_sheets_service()
for Player in AODDB.element_array('Player'):
#    if not(Player.info['playerName'] == 'lebedeva-A-A'):
#        continue
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
