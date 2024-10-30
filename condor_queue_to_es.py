#!/usr/bin/env python3


import os
import glob
from argparse import ArgumentParser
import logging
from functools import partial
from rest_tools.client import ClientCredentialsAuth

parser = ArgumentParser('usage: %prog [options] history_files')
parser.add_argument('-a','--address',help='elasticsearch address')
parser.add_argument('-n','--indexname',default='condor',
                  help='index name (default condor)')
parser.add_argument('--dailyindex', default=False, action='store_true',
                  help='Index pattern daily')
parser.add_argument("-y", "--dry-run", default=False,
                  action="store_true",
                  help="query jobs, but do not ingest into ES",)
parser.add_argument('--collectors', default=False, action='store_true',
                  help='Args are collector addresses, not files')
parser.add_argument('--client_id',help='oauth2 client id',default=None)
parser.add_argument('--client_secret',help='oauth2 client secret',default=None)
parser.add_argument('--token_url',help='oauth2 realm token url',default=None)
parser.add_argument("positionals", nargs='+')

options = parser.parse_args()
if not options.positionals:
    parser.error('no condor history files or collectors')


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

import htcondor
from condor_utils import *

# daily index manditory
options.indexname += '-'+now.strftime("%Y.%m.%d")

# key filter
keys = {
    'RequestCpus','Requestgpus', 'RequestMemory', 'RequestDisk',
    'NumJobStarts', 'NumShadowStarts',
    'GlobalJobId', '@timestamp', 'queue_time', 'Owner',
    'JobStatus','MATCH_EXP_JOBGLIDEIN_ResourceName',
    'IceProdDataset', 'IceProdTaskName'
}

def es_generator(entries):
    for data in entries:
        add_classads(data)
        data = {k:data[k] for k in keys if k in data} # do filtering
        data['_index'] = options.indexname
        data['_id'] = data['GlobalJobId'].replace('#','-').replace('.','-') + data['@timestamp']
        yield data

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from rest_tools.client import ClientCredentialsAuth

prefix = 'http'
address = options.address



if '://' in address:
    prefix,address = address.split('://')

url = '{}://{}'.format(prefix, address)

token = None

if None not in (options.token_url, options.client_secret, options.client_id):
    api = ClientCredentialsAuth(address='https://elasticsearch.icecube.aq',
                                token_url=options.token_url,
                                client_secret=options.client_secret,
                                client_id=options.client_id)
    token = api.make_access_token()

logging.info('connecting to ES at %s',url)
es = Elasticsearch(hosts=[url],
                   timeout=5000,
                   bearer_auth=token)
es_import = partial(bulk, es, max_retries=20, initial_backoff=2, max_backoff=3600)

failed = False
if options.collectors:
    for coll_address in options.positionals:
        try:
            gen = es_generator(read_from_collector(coll_address))
            success, _ = es_import(gen)
        except htcondor.HTCondorIOError as e:
            failed = e
            logging.error('Condor error', exc_info=True)
else:
    for path in options.args:
        for filename in glob.iglob(path):
            gen = es_generator(read_from_file(filename))
            success, _ = es_import(gen)
            logging.info('finished processing %s', filename)

if failed:
    raise failed
