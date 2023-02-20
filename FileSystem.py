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

def read_config():
    with open('/home/onco-admin/ATLAS_software/aod-admin/conf/Config.json', 'r') as config_file:
        return json.loads(config_file.read())

class File(object):
    def __init__(self):
        self.path = None
        self.type = None
        self.name = None

class bam(File):
    def __init__(self):
        self.type = 'bam'

class Folder(object):
    def __init__(self):
        self.path = None

import AODDB













