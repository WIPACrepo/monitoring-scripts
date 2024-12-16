#!/usr/bin/env python3
"""
Read from condor status and write to elasticsearch
"""

import glob
import json
import logging
import os
import re
import textwrap
from argparse import ArgumentParser
from datetime import datetime, timedelta
from functools import partial
from time import mktime

import elasticsearch_dsl as edsl
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import MultiSearch, Search
import htcondor
from rest_tools.client import ClientCredentialsAuth

from condor_utils import *

regex = re.compile(
    r"((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?"
)

# note different capitalization conventions for GPU and Cpu
RESOURCES = ("GPUs", "Cpus", "Memory", "Disk")
STATUSES = ("evicted", "removed", "finished", "failed")

class Dry:
    """Helper class for debugging"""
    _dryrun = False
    
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        if self._dryrun:
            logging.info(self.func.__name__)
            logging.info(args)
            logging.info(kwargs)
            
        else:
            return self.func(*args,**kwargs)

def parse_time(time_str):
    parts = regex.match(time_str)
    if not parts:
        raise ValueError
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.items():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)

@Dry
def es_import(gen, es):
    success, _ = bulk(es, gen, max_retries=20, initial_backoff=2, max_backoff=3600)
    return success

def update_machines(entries):
    """
    Generate upsert ops from machine classad dictionaries
    """
    for data in entries:
        yield {
            "_index": options.indexname,
            "_op_type": "update",
            "_id": "{:.0f}-{:s}".format(
                time.mktime(data["DaemonStartTime"].timetuple()), data["Name"]
            ),
            "upsert": data,
            "script": {
                "id": options.indexname + "-update-machine",
                "params": {
                    "duration": data["duration"],
                    "LastHeardFrom": data["LastHeardFrom"],
                },
            },
        }


def main(options):
    prefix = "http"
    address = options.address
    if "://" in address:
        prefix, address = address.split("://")

    token = None

    if options.token is not None:
        token = options.token
    elif None not in (options.token_url, options.client_secret, options.client_id):
        api = ClientCredentialsAuth(address='https://elasticsearch.icecube.aq',
                                    token_url=options.token_url,
                                    client_secret=options.client_secret,
                                    client_id=options.client_id)
        token = api.make_access_token()

    url = "{}://{}".format(prefix, address)
    logging.info("connecting to ES at %s", url)
    es = Elasticsearch(hosts=[url], 
                    timeout=5000,
                    bearer_auth=token,
                    sniff_on_node_failure=True)
    
    for coll_address in options.collectors:
        try:
            gen = update_machines(
                read_status_from_collector(coll_address, datetime.now() - options.after)
            )
            success = es_import(gen, es)

        except htcondor.HTCondorIOError as e:
            failed = e
            logging.error('Condor error', exc_info=True)
    

if __name__ == '__main__':
    parser = ArgumentParser("usage: %prog [options] collector_addresses")
    parser.add_argument("-a", "--address", help="elasticsearch address")
    parser.add_argument(
        "-n",
        "--indexname",
        default="condor_status",
        help="index name (default condor_status)",
    )
    parser.add_argument(
        "--after", default=timedelta(hours=1), help="time to look back", type=parse_time
    )
    parser.add_argument(
        "-y",
        "--dry-run",
        default=False,
        action="store_true",
        help="query status, but do not ingest into ES",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help="use verbose logging in ES",
    )
    parser.add_argument('--client_id',help='oauth2 client id',default=None)
    parser.add_argument('--client_secret',help='oauth2 client secret',default=None)
    parser.add_argument('--token_url',help='oauth2 realm token url',default=None)
    parser.add_argument('--token',help='oauth2 token',default=None)
    parser.add_argument("collectors", nargs="+")
    options = parser.parse_args()

    Dry._dryrun  = options.dryrun

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s"
    )
    if options.verbose:
        logging.getLogger("elasticsearch").setLevel("DEBUG")
