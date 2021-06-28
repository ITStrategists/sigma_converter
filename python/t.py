import os
import re

import glob

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



#fname = '../data/its_sig/**/*.view'
fname = '../data/its_sig/**/_site_extended_athena.view.lkml'
if not fname.endswith('.lkml'):
    fname = '{}.lkml'.format(fname)

#print(fname)
for name in glob.glob(fname):
    print(name)

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