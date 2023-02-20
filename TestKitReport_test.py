import AODDB
import sys
import json
import os
import re
from pprint import pprint
from datetime import date
import string
import random
import AODDB
import sys
import json
import os
import re
from pprint import pprint
from datetime import date
import string
import random
import TestKitReport

def gether_report(case_name, output_file):
    report = TestKitReport.Report(case_name)
    report.make(panel_code = 'AODABCV1')
    pprint(report.content)
    report.post()

case_name = sys.argv[1]
try:
    output_file = sys.argv[2]
except IndexError:
    output_file = None
gether_report(case_name, output_file)

