#!/usr/bin/env python3
"""
Read from gridftp transfer logs and write to elasticsearch
"""


import os
import glob
import gzip
import decimal
from optparse import OptionParser
import logging
from functools import partial

parser = OptionParser('usage: %prog [options] transfer_files')
parser.add_option('-a','--address',help='elasticsearch address')
parser.add_option('-n','--indexname',default='gridftp',
                  help='index name (default gridftp)')
(options, args) = parser.parse_args()
if not args:
    parser.error('no gridftp transfer log files')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

def date_convert(d):
    return d[:4]+'-'+d[4:6]+'-'+d[6:8]+'T'+d[8:10]+':'+d[10:12]+':'+d[12:]

def read_from_file(filename):
    with (gzip.open(filename) if filename.endswith('.gz') else open(filename)) as f:
        for line in f:
            try:
                data = {p.split('=',1)[0]:p.split('=',1)[1] for p in line.split() if '=' in p}
                if data['NL.EVNT'] == 'PROG':
                    continue
                data['start_date'] = date_convert(data['START'])
                data['end_date'] = date_convert(data['DATE'])
                data['DEST'] = data['DEST'].strip('[]')
                for k in ('NBYTES','BLOCK','BUFFER','STREAMS'):
                    data[k] = int(data[k])
                data['duration'] = float(decimal.Decimal(data['DATE']) - decimal.Decimal(data['START']))
                data['bandwidth_mbps'] = data['NBYTES'] * 8 / 1000000. / data['duration']
                for k in ('START','DATE','PROG','NL.EVNT','VOLUME','CODE','TASKID'):
                    del data[k]
            except Exception:
                print(data)
                continue
            yield data

def es_generator(entries):
    for data in entries:
        data['_index'] = options.indexname
        data['_type'] = 'transfer_log'
        data['_id'] = data['end_date'].replace('#','-').replace('.','-')
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

for path in args:
    for filename in glob.iglob(path):
        gen = es_generator(read_from_file(filename))
        success, _ = es_import(gen)
        logging.info('finished processing %s', filename)
