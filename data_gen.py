#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3, random
import datetime
import json
import os
from optparse import OptionParser

PROVIDERIDs = []
MAILNOs = []
MAILTYPEs = []
WEIGHTs = []
CITYCODEs = []
COUNTRYCODEs = []
NAMEs = []
MOBILEs = []
PHONEs = []
PROVINCEs = []
CITYs = []
LOCATIONs = []
ADDRESSes = []
CONTENTTYPEs = []
CONTENTNAMEs = []
CONTENTCODEs = []
DATEs = []# ['2017/5/13','2017/5/14']
INSURANCEs = []

conn = sqlite3.connect('/root/order.db')

cur = conn.execute('select distinct logisticproviderid from orig_order')
for row in cur:
    PROVIDERIDs.append(row[0])

cur = conn.execute('select distinct mailno from orig_order')
for row in cur:
    MAILNOs.append(row[0])

cur = conn.execute('select distinct mailtype from orig_order')
for row in cur:
    MAILTYPEs.append(row[0])

cur = conn.execute('select distinct weight from orig_order')
for row in cur:
    WEIGHTs.append(row[0])

cur = conn.execute('select distinct sencitycode from orig_order')
for row in cur:
    CITYCODEs.append(row[0])

cur = conn.execute('select distinct senareacode from orig_order')
for row in cur:
    COUNTRYCODEs.append(row[0])

cur = conn.execute('select distinct senname from orig_order')
for row in cur:
    NAMEs.append(row[0])

cur = conn.execute('select distinct senmobile from orig_order')
for row in cur:
    MOBILEs.append(row[0])

cur = conn.execute('select distinct senphone from orig_order')
for row in cur:
    PHONEs.append(row[0])

cur = conn.execute('select distinct senprov from orig_order')
for row in cur:
    PROVINCEs.append(row[0])

cur = conn.execute('select distinct sencity from orig_order')
for row in cur:
    CITYs.append(row[0])

cur = conn.execute('select distinct sencounty from orig_order')
for row in cur:
    LOCATIONs.append(row[0])

cur = conn.execute('select distinct senaddress from orig_order')
for row in cur:
    ADDRESSes.append(row[0])

cur = conn.execute('select distinct typeofcontents from orig_order')
for row in cur:
    CONTENTTYPEs.append(row[0])

cur = conn.execute('select distinct nameofcoutents from orig_order')
for row in cur:
    CONTENTNAMEs.append(row[0])

cur = conn.execute('select distinct mailcode from orig_order')
for row in cur:
    CONTENTCODEs.append(row[0])

# cur = conn.execute('select distinct recdatetime from orig_order')
# for row in cur:
    # DATEs.append(row[0])

cur = conn.execute('select distinct insurancevalue from orig_order')
for row in cur:
    INSURANCEs.append(row[0])

conn.close()

def _check_and_create(datapath, date):
    HOME = datapath + '/' + date
    file_no = 0
    line_no = 0
    if not os.path.exists(HOME):
        os.makedirs(HOME)
    else:
        def get_file_no(arg, dirname, filelist):
            for filename in filelist:
                # TODO: Get the biggest file name
                # TODO: Check if this file's line number
        os.path.walk(HOME, get_file_no, [])
    return HOME, file_no, line_no

def _run(home, file_no):
    with open(home) as fd:
        while True:
            data = {
                'info': {
                    'logisticproviderid': random.choice(PROVIDERIDs),
                    'mailno': random.choice(MAILNOs),
                    'mailtype': random.choice(MAILTYPEs),
                    'weight': random.choice(WEIGHTs),
                    'sencitycode': random.choice(CITYCODEs),
                    'reccitycode': random.choice(CITYCODEs),
                    'senareacode': random.choice(COUNTRYCODEs),
                    'recareacode': random.choice(COUNTRYCODEs),
                    'inserttime': recdatetime
                },
                'send': {
                    'senname': random.choice(NAMEs),
                    'senmobile': random.choice(MOBILEs),
                    'senphone': random.choice(PHONEs),
                    'senprov': random.choice(PROVINCEs),
                    'sencity': random.choice(CITYs),
                    'sencounty': random.choice(LOCATIONs),
                    'senaddress': random.choice(ADDRESSes)
                },
                'recv': {
                    'recname': random.choice(NAMEs),
                    'recmobile': random.choice(MOBILEs),
                    'recphone': random.choice(PHONEs),
                    'recprov': random.choice(PROVINCEs),
                    'reccity': random.choice(CITYs),
                    'reccounty': random.choice(LOCATIONs),
                    'recaddress': random.choice(ADDRESSes)
                },
                'pkg': {
                    'typeofcontents': random.choice(CONTENTTYPEs),
                    'nameofcoutents': random.choice(CONTENTNAMEs),
                    'mailcode': random.choice(CONTENTCODEs),
                    'recdatetime': random.choice(DATEs),
                    'insurancevalue': random.choice(INSURANCEs)
                }
            }
            fd.write(json.dumps(data))

def main():
    usage = 'Usage: %prog [options]'
    parser = OptionParser(usage=usage)
    parser.add_option('-d', '--date', dest='date', \
            default=datetime.datetime.now().strftime('%Y/%m/%d'), \
            help="the date of data to be generated(e.g. 2017/06/12), \
            default is today")
    parser.add_option('-p', '--path', dest='datapath', default='.', \
            help="the path to store data files")
    options, args = parser.parse_args()

    store_path = _check_and_create(options.datapath, options.date)

    DATEs.append(options.date)
    _run(store_path)

if __name__ == '__main__':
    main()

