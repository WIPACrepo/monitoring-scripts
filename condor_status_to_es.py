#!/usr/bin/env python
"""
Read from condor history and write to elasticsearch
"""

from __future__ import print_function
import os
import glob
from argparse import ArgumentParser
import logging
from functools import partial

import re
from datetime import datetime, timedelta
from time import mktime

regex = re.compile(r'((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

def parse_time(time_str):
    parts = regex.match(time_str)
    if not parts:
        raise ValueError
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.iteritems():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)

parser = ArgumentParser('usage: %prog [options] collector_addresses')
parser.add_argument('-a','--address',help='elasticsearch address')
parser.add_argument('-n','--indexname',default='condor_status',
                  help='index name (default condor_status)')
parser.add_argument('--after', default=timedelta(hours=1),
                  help='time to look back', type=parse_time)
parser.add_argument('-y', '--dry-run', default=False, action='store_true',
                  help='query status, but do not ingest into ES')
parser.add_argument('collectors', nargs='+')
options = parser.parse_args()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')


from condor_utils import *

def es_generator(entries):
    for data in entries:
        data['_index'] = options.indexname
        data['_type'] = 'machine_ad'
        data['_id'] = '{:.0f}-{:s}'.format(time.mktime(data['DaemonStartTime'].timetuple()),data['Name'])
        if not data['_id']:
            continue
        yield data

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

prefix = 'http'
address = options.address
if '://' in address:
    prefix,address = address.split('://')

url = '{}://{}'.format(prefix, address)
logging.info('connecting to ES at %s',url)
es = Elasticsearch(hosts=[url],
                   timeout=5000)
es_import = partial(bulk, es, max_retries=20, initial_backoff=2, max_backoff=3600)

for coll_address in options.collectors:
    gen = es_generator(read_status_from_collector(coll_address, datetime.now()-options.after))
    if options.dry_run:
        for hit in gen:
            logging.info(hit)
    else:
        success, _ = es_import(gen)
