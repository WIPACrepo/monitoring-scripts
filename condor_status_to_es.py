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
from elasticsearch.helpers import bulk, BulkIndexError
import htcondor2 as htcondor
from rest_tools.client import ClientCredentialsAuth

from condor_utils import *

REGEX = re.compile(
    r"((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?"
)
# note different capitalization conventions for GPU and Cpu
RESOURCES = ("GPUs", "Cpus", "Memory", "Disk")
STATUSES = ("evicted", "removed", "finished", "failed")
INDEX = "condor_status"

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
    parts = REGEX.match(time_str)
    if not parts:
        raise ValueError
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.items():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)

def update_machines(entries):
    """
    Generate upsert ops from machine classad dictionaries
    """
    for data in entries:
        yield {
            "_index": INDEX,
            "_op_type": "update",
            "_id": "{:.0f}-{:s}".format(
                time.mktime(data["DaemonStartTime"].timetuple()), data["Name"]
            ),
            "upsert": data,
            "script": {
                "id": INDEX + "-update-machine",
                "params": {
                    "duration": data["duration"],
                    "LastHeardFrom": data["LastHeardFrom"],
                },
            },
        }


def update_jobs(entries, history=False):
    """
    Generate updates to claims.* from job classad dictionaries
    """

    def parent_slot_name(dynamic_slot_name):
        parts = dynamic_slot_name.split("@")
        match = re.match(r"(slot\d+)_\d+", parts[0])
        if match:
            parts[0] = match.group(1)
        return "@".join(parts)

    # MultiSearch will fail if there are no queries to run
    jobs = list(entries)
    if not jobs:
        return

    # glidein names are not necessarily unique on long time scales. look up the
    # last glidein that started with the advertised name _before_ the evicted
    # job was started
    ms = MultiSearch(using=es, index=INDEX)
    for hit in jobs:
        try:
            if history:
                t0 = hit["JobCurrentStartDate"]
            else:
                if hit["JobStatus"] == 5:
                    t0 = hit["JobCurrentStartDate"]
                else:
                    t0 = hit["JobLastStartDate"]
            ms = ms.add(
                Search()
                .filter("term", Name__keyword=parent_slot_name(hit["LastRemoteHost"]))
                .filter("range", DaemonStartTime={"lte": datetime.fromtimestamp(t0)},)
                .sort({"DaemonStartTime": {"order": "desc"}})
                .source(["nuthin"])[:1]
            )
        except Exception:
            logging.warning('failed to process job, %r', hit)
            continue

    for hit, match in zip(jobs, ms.execute()):
        if not match.hits:
            continue
        if history:
            if hit["JobStatus"] == 3:
                category = "removed"
            elif hit.get("ExitCode", -1) == 0:
                category = "finished"
            else:
                category = "failed"
            walltime = float(hit["EnteredCurrentStatus"] - hit["JobCurrentStartDate"])
        else:
            # NB: if a job is evicted from one slot, held on another, and then
            # removed from the queue, there's no way to recover the time that
            # may have elapsed between hold and removal. To handle this case,
            # we treat held jobs as a subcategory of removed jobs, so that they
            # will not be counted again when encountered in the history.
            if hit["JobStatus"] == 5:
                walltime = float(hit["EnteredCurrentStatus"] - hit["JobCurrentStartDate"])
                category = "removed"
            else:
                walltime = float(hit["LastVacateTime"] - hit["JobLastStartDate"])
                category = "evicted"

        # normalize capitalization of requests
        requests = {resource: 0 for resource in RESOURCES}
        for k in hit:
            if k.startswith("Request"):
                requests[k[7:]] = walltime * hit[k]

        doc = {
            "_op_type": "update",
            "_index": match.hits[0].meta.index,
            "_id": match.hits[0].meta.id,
            "script": {
                "id": INDEX + "-update-jobs",
                "params": {
                    "job": hit["GlobalJobId"].replace("#", "-").replace(".", "-"),
                    "category": category,
                    "requests": requests,
                },
            },
        }
        yield doc

@Dry
def es_import(gen, es):
    try:
        success, _ = bulk(es, gen, max_retries=20, initial_backoff=2, max_backoff=3600)
        return success
    except BulkIndexError as e:
        for error in e.errors:
            logging.info(json.dumps(error, indent=2, default=str))


def put_scripts(es, index):
    es.put_script(
        id=index + "-update-jobs",
        body={
            "script": {
                "source": textwrap.dedent(
                    """
                    def RESOURCES = """
                    + repr(list(RESOURCES))
                    + """;
                    def STATUSES = """
                    + repr(list(STATUSES))
                    + """;
                    if(ctx._source["jobs."+params.category] == null) {
                        ctx._source["jobs."+params.category] = [];
                        for (resource in RESOURCES) {
                            ctx._source["claims."+params.category+"."+resource] = 0.0;
                        }
                    }
                    if(!ctx._source["jobs."+params.category].contains(params.job)) {
                        ctx._source["jobs."+params.category].add(params.job);
                        for (resource in RESOURCES) {
                            if (params.requests.containsKey(resource)) {
                                ctx._source["claims."+params.category+"."+resource] += params.requests[resource].doubleValue();
                            }
                            if (!ctx._source.containsKey("Total"+resource) || ctx._source.duration == null) {
                                continue;
                            }
                            double norm = (ctx._source.duration.doubleValue())*(ctx._source["Total"+resource].doubleValue());
                            if (!(norm > 0)) {
                                continue;
                            }
                            double total = 0;
                            for (status in STATUSES) {
                                def key = status+"."+resource;
                                if (ctx._source["claims."+key] != null) {
                                    ctx._source["occupancy."+key] = ctx._source["claims."+key]/norm;
                                    total += ctx._source["claims."+key]/norm;
                                } else {
                                    ctx._source["occupancy."+key] = 0;
                                }
                            }
                            ctx._source["occupancy.total."+resource] = total;
                        }
                    } else {
                        ctx.op = "none";
                    }
                    """
                ),
                "lang": "painless",
                "context": "update",
            }
        },
    )

    # Occupancy update, triggered every time a glidein increments LastHeardFrom
    es.put_script(
        id=index + "-update-machine",
        body={
            "script": {
                "source": textwrap.dedent(
                    """
                    def RESOURCES = """
                    + repr(list(RESOURCES))
                    + """;
                    def STATUSES = """
                    + repr(list(STATUSES))
                    + """;
                    if(ctx._source.duration == null
                        || ctx._source.duration < params.duration) {
                        ctx._source.duration = params.duration;
                        if (!(ctx._source["@timestamp"] instanceof ArrayList)) {
                            ctx._source["@timestamp"] = [ctx._source.DaemonStartTime, ctx._source.LastHeardFrom];
                        }
                        ctx._source["@timestamp"].add(params.LastHeardFrom);
                        ctx._source.LastHeardFrom = params.LastHeardFrom;
                        for (resource in RESOURCES) {
                            if (!ctx._source.containsKey("Total"+resource)) {
                                continue;
                            }
                            double norm = (ctx._source.duration.doubleValue())*(ctx._source["Total"+resource].doubleValue());
                            if (!(norm > 0)) {
                                continue;
                            }
                            double total = 0;
                            for (status in STATUSES) {
                                def key = status+"."+resource;
                                if (ctx._source["claims."+key] != null) {
                                    ctx._source["occupancy."+key] = ctx._source["claims."+key]/norm;
                                    total += ctx._source["claims."+key]/norm;
                                } else {
                                    ctx._source["occupancy."+key] = 0;
                                }
                            }
                            ctx._source["occupancy.total."+resource] = total;
                        }
                    } else {
                        ctx.op = "none";
                    }
                    """
                ),
                "lang": "painless",
                "context": "update",
            }
        },
    )
if __name__ == "__main__":
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
        "-d",
        "--dry-run",
        default=False,
        action="store_true",
        help="query status, but do not ingest into ES",
    )
    parser.add_argument(
        "-y",
        "--put-script",
        default=False,
        action="store_true",
        help="put/update ES update scripts at index",
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

    INDEX = options.indexname

    Dry._dryrun  = options.dry_run

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s"
    )
    if options.verbose:
        logging.getLogger("elasticsearch").setLevel("DEBUG")


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

    if options.put_script:
        put_scripts(options.index)

    failed = False
    for coll_address in options.collectors:
        try:
            gen = update_machines(
                read_status_from_collector(coll_address, datetime.now() - options.after)
            )
            success = es_import(gen, es)

            # Update claims from evicted and held jobs
            after = time.mktime((datetime.now() - timedelta(minutes=10)).timetuple())
            gen = update_jobs(
                read_from_collector(
                    coll_address,
                    constraint=(
                        "((LastVacateTime > {}) && ((LastVacateTime-JobLastStartDate))>60)"
                        + " || ((JobStatus == 5) && (EnteredCurrentStatus > {}))"
                    ).format(after, after),
                    projection=[
                        "GlobalJobId",
                        "NumJobStarts",
                        "JobStatus",
                        "JobLastStartDate",
                        "JobCurrentStartDate",
                        "EnteredCurrentStatus",
                        "LastVacateTime",
                        "LastRemoteHost",
                    ]
                    + ["Request" + resource for resource in RESOURCES],
                ),
                history=False,
            )
            success = es_import(gen, es)

            # Update claims from finished jobs
            gen = update_jobs(
                read_from_collector(
                    coll_address,
                    constraint="!isUndefined(LastRemoteHost)",
                    projection=[
                        "GlobalJobId",
                        "NumJobStarts",
                        "JobLastStartDate",
                        "JobCurrentStartDate",
                        "EnteredCurrentStatus",
                        "JobStatus",
                        "ExitCode",
                        "LastRemoteHost",
                    ]
                    + ["Request" + resource for resource in RESOURCES],
                    history=True,
                ),
                history=True,
            )
            success = es_import(gen, es)

        except htcondor.HTCondorIOError as e:
            failed = e
            logging.error('Condor error', exc_info=True)

    if failed:
        raise failed
