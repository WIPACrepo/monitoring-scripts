#!/usr/bin/env python3


import os
import glob
import gzip
from optparse import OptionParser

from pymongo import MongoClient

parser = OptionParser('usage: %prog [options] history_files')
parser.add_option('-m','--mongo',help='mongodb host')
parser.add_option('--clear', default=False, action='store_true',
                  help='clear db table before import')
(options, args) = parser.parse_args()
if not args:
    parser.error('no condor history files')

mongo_args = {}
if options.mongo:
    mongo_args['host'] = options.mongo

db = MongoClient(**mongo_args).condor
if options.clear:
    db.condor_history.drop()
    db.condor_history.create_index("GlobalJobId")
    db.condor_history.create_index("JobStatus")

def get_type(val):
    if val == 'false':
        return False
    elif val == 'true':
        return True
    elif val.startswith('"') and val.endswith('"') and '"' not in val[1:-1]:
        return val[1:-1]
    try:
        return int(val)
    except:
        try:
            return float(val)
        except:
            return val

good_keys = set(['JobStatus','Cmd','Owner','AccountingGroup',
'ImageSize_RAW','DiskUsage_RAW','ExecutableSize_RAW',
'BytesSent','BytesRecvd',
'ResidentSetSize_RAW',
'RequestCpus','Requestgpus','RequestMemory','RequestDisk',
'NumJobStarts','NumShadowStarts',
'GlobalJobId','ClusterId','ProcId','RemoteWallClockTime',
'ExitBySignal','ExitCode','ExitSignal','ExitStatus',
'CumulativeSlotTime','LastRemoteHost',
'QDate','JobStartDate','JobCurrentStartDate','EnteredCurrentStatus',
'RemoteUserCpu','RemoteSysCpu','CompletionDate',
'CommittedTime','RemoteWallClockTime',
'MATCH_EXP_JOBGLIDEIN_ResourceName','StartdPrincipal','DAGManJobId',
'LastJobStatus','LastHoldReason','LastRemotePool',
])

def filter_keys(data):
    for k in list(data.keys()):
        if k not in good_keys:
            del data[k]

def insert(data):
    ret = db.condor_history.find_one({'GlobalJobId':data['GlobalJobId']})
    if not ret:
        db.condor_history.insert_one(entry)
    else:
        diff = {}
        for k in set(data).difference(ret):
            diff[k] = data[k]
        if diff:
            db.condor_history.update_one({'GlobalJobId':data['GlobalJobId']},
                                         {'$set':diff})

for path in args:
    for filename in glob.iglob(path):
        with (gzip.open(filename) if filename.endswith('.gz') else open(filename)) as f:
            entry = {}
            for line in f.readlines():
                if line.startswith('***'):
                    filter_keys(entry)
                    insert(entry)
                    entry = {}
                else:
                    name,value = line.split('=',1)
                    entry[name.strip()] = get_type(value.strip())
        print('.',end='')

