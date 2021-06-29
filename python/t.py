import os
import re

import glob
from sql_formatter.core import format_sql

sql = """
SELECT id, ad_layout_changes, site_id, created_at as created_raw, to_char(to_date(date_trunc('second', convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)))), 'YYYY-MM-DD HH24:MI:SS') as created_time, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast(created_at as timestamp_ntz))), 'YYYY-MM-DD') as created_date, to_char(to_date(date_trunc('week', convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)))), 'YYYY-MM-DD') as created_week, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz))), 'YYYY-MM') as created_month, to_char(date_trunc('month', cast(date_trunc('quarter', convert_timezone('UTC', 'America/New_York', cast(created_at as timestamp_ntz))) as date)), 'YYYY-MM') as created_quarter, extract(year FROM convert_timezone('UTC', 'America/New_York', cast(created_at as timestamp_ntz)))::integer as created_year, end_date as end_raw, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast(end_date as timestamp_ntz))), 'YYYY-MM-DD') as end_date, to_char(to_date(date_trunc('week', convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)))), 'YYYY-MM-DD') as end_week, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz))), 'YYYY-MM') as end_month, to_char(date_trunc('month', cast(date_trunc('quarter', convert_timezone('UTC', 'America/New_York', cast(end_date as timestamp_ntz))) as date)), 'YYYY-MM') as end_quarter, extract(year FROM convert_timezone('UTC', 'America/New_York', cast(end_date as timestamp_ntz)))::integer as end_year, start_date as start_raw, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast(start_date as timestamp_ntz))), 'YYYY-MM-DD') as start_date, to_char(to_date(date_trunc('week', convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)))), 'YYYY-MM-DD') as start_week, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz))), 'YYYY-MM') as start_month, to_char(date_trunc('month', cast(date_trunc('quarter', convert_timezone('UTC', 'America/New_York', cast(start_date as timestamp_ntz))) as date)), 'YYYY-MM') as start_quarter, extract(year FROM convert_timezone('UTC', 'America/New_York', cast(start_date as timestamp_ntz)))::integer as start_year, updated_at as updated_raw, to_char(to_date(date_trunc('second', convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)))), 'YYYY-MM-DD HH24:MI:SS') as updated_time, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast(updated_at as timestamp_ntz))), 'YYYY-MM-DD') as updated_date, to_char(to_date(date_trunc('week', convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz)))), 'YYYY-MM-DD') as updated_week, to_char(to_date(convert_timezone('UTC', 'America/New_York', cast({} as timestamp_ntz))), 'YYYY-MM') as updated_month, to_char(date_trunc('month', cast(date_trunc('quarter', convert_timezone('UTC', 'America/New_York', cast(updated_at as timestamp_ntz))) as date)), 'YYYY-MM') as updated_quarter, extract(year FROM convert_timezone('UTC', 'America/New_York', cast(updated_at as timestamp_ntz)))::integer as updated_year FROM ADTHRIVE.adthrive.site_rpm_input_after
"""
n = format_sql(sql)
print(sql)
print(n)

'''

dirs = os.walk('../data/its_sig')
models = '.*.model.lkml'

base_dir = './data/its_sig/'
includes = '*.model'

model_dirs = []

for dir, subdir, files in dirs:
    for f in files:
        rx = re.compile(models)
        filePath = os.path.join(dir, f)
        #print("Checking: {}".format(filePath))
        if rx.match(filePath):
            print("Found: {}".format(filePath))
            model_dirs.append(
                {
                    "DirName": dir,
                    "FileName" : f
                }
            )
    

for model_dir in model_dirs:
    print(model_dir)
    
'''


'''
#fname = '../data/its_sig/**/*.view'
fname = '../data/its_sig/**/_site_extended_athena.view.lkml'
if not fname.endswith('.lkml'):
    fname = '{}.lkml'.format(fname)

#print(fname)
for name in glob.glob(fname):
    print(name)
'''
'''
dirs = os.walk('../data/its_sig')
parent = '/'

files_included = '.^/([^/]+)/?(.*)$.view.lkml'
folder_name = '/'

traverse_path = '{}{}'.format(folder_name, files_included)

for dir, subdir, files in dirs:
    for f in files:
        rx = re.compile(traverse_path)
        filePath = os.path.join(dir, f)
        print("Checking: {}".format(filePath))
        if rx.match(filePath):
            print("Found: {}".format(filePath))
        else:
            print("Not Found: {}".format(filePath))
    print("---------------------------")


'''