#!/usr/bin/env python3
"""
Read from condor history and write to elasticsearch
"""


import os
import glob
from optparse import OptionParser
import logging
from functools import partial

parser = OptionParser('usage: %prog [options] history_files')
parser.add_option('-a','--address',help='elasticsearch address')
parser.add_option('-n','--indexname',default='condor',
                  help='index name (default condor)')
parser.add_option('--dailyindex', default=False, action='store_true',
                  help='Index pattern daily')
parser.add_option('--collectors', default=False, action='store_true',
                  help='Args are collector addresses, not files')
(options, args) = parser.parse_args()
if not args:
    parser.error('no condor history files or collectors')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')


from condor_utils import *

def es_generator(entries):
    for data in entries:
        add_classads(data)
        data['_index'] = options.indexname
        if options.dailyindex:
            data['_index'] += '-'+(data['date'].split('T')[0].replace('-','.'))
        data['_type'] = 'job_ad'
        data['_id'] = data['GlobalJobId'].replace('#','-').replace('.','-')
        if not data['_id']:
            continue
        yield data

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

prefix = 'http'
address = options.address
if '://' in address:
    prefix,address = address.split('://')

url = '{}://{}'.format(prefix, address)
logging.info('connecting to ES at %s',url)
es = Elasticsearch(hosts=[url],
                   timeout=5000)
es_import = partial(bulk, es, max_retries=20, initial_backoff=2, max_backoff=3600)

if options.collectors:
    for coll_address in args:
        gen = es_generator(read_from_collector(coll_address, history=True))
        success, _ = es_import(gen)
else:
    for path in args:
        for filename in glob.iglob(path):
            gen = es_generator(read_from_file(filename))
            success, _ = es_import(gen)
            logging.info('finished processing %s', filename)
