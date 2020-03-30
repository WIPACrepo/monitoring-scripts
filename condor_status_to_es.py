#!/usr/bin/env python
"""
Read from condor status and write to elasticsearch
"""

from __future__ import print_function

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

from condor_utils import *

regex = re.compile(
    r"((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?"
)


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
parser.add_argument("collectors", nargs="+")
options = parser.parse_args()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s"
)


# note different capitalization conventions for GPU and Cpu
RESOURCES = ("GPUs", "Cpus", "Memory", "Disk")
STATUSES = ("evicted", "removed", "finished")


def es_generator(entries):
    """
    Generate upsert ops from machine classad dictionaries
    """
    for data in entries:
        yield {
            "_index": options.indexname,
            "_op_type": "update",
            "_type": "machine_ad",
            "_id": "{:.0f}-{:s}".format(
                time.mktime(data["DaemonStartTime"].timetuple()), data["Name"]
            ),
            "doc": data,
            "doc_as_upsert": True,
        }


def update_claims(entries, history=False):
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
    ms = MultiSearch(using=es, index=options.indexname)
    for hit in jobs:
        if history:
            t0 = hit["JobCurrentStartDate"]
        else:
            t0 = hit["JobLastStartDate"]
        ms = ms.add(
            Search()
            .filter("term", Name__keyword=parent_slot_name(hit["LastRemoteHost"]))
            .filter("range", DaemonStartTime={"lte": datetime.utcfromtimestamp(t0)},)
            .sort({"DaemonStartTime": {"order": "desc"}})
            .source(["nuthin"])[:1]
        )

    for hit, match in zip(jobs, ms.execute()):
        if not match.hits:
            continue
        if history:
            if hit["JobStatus"] == 3:
                category = "removed"
            else:
                category = "finished"
            walltime = float(hit["EnteredCurrentStatus"] - hit["JobCurrentStartDate"])
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
            "_type": match.hits[0].meta.doc_type,
            "_id": match.hits[0].meta.id,
            "script": {
                "id": options.indexname + "-update-claims",
                "params": {
                    "job": hit["GlobalJobId"].replace("#", "-").replace(".", "-"),
                    "category": category,
                    "requests": requests,
                },
            },
        }
        yield doc


prefix = "http"
address = options.address
if "://" in address:
    prefix, address = address.split("://")

url = "{}://{}".format(prefix, address)
logging.info("connecting to ES at %s", url)
es = Elasticsearch(hosts=[url], timeout=5000)


def es_import(document_generator):
    if options.dry_run:
        for hit in document_generator:
            logging.info(hit)
        success = True
    else:
        success, _ = bulk(es, gen, max_retries=20, initial_backoff=2, max_backoff=3600)
    return success


machine_ad = edsl.Mapping.from_es(
    doc_type="machine_ad", index=options.indexname, using=es
)
if not "claims" in machine_ad:
    machine_ad.field(
        "jobs",
        edsl.Object(properties={status: edsl.Text(multi=True) for status in STATUSES}),
    )
    machine_ad.field(
        "claims",
        edsl.Object(
            properties={
                status: edsl.Object(
                    properties={resource: edsl.Float() for resource in RESOURCES}
                )
                for status in STATUSES
            }
        ),
    )
    machine_ad.save(options.indexname, using=es)
    machine_ad.field(
        "occupancy",
        edsl.Object(
            properties={
                status: edsl.Object(
                    properties={resource: edsl.Float() for resource in RESOURCES}
                )
                for status in STATUSES + ("total",)
            }
        ),
    )
    machine_ad.field("duration", edsl.Integer())
    machine_ad.save(options.indexname, using=es)
    # Claim update, triggered each time a job is removed from a machine
    es.put_script(
        options.indexname + "-update-claims",
        {
            "script": {
                "source": textwrap.dedent(
                    """
                    def resources = """
                    + repr(list(RESOURCES))
                    + """;
                    if(ctx._source["jobs."+params.category] == null) {
                        ctx._source["jobs."+params.category] = [];
                        for (resource in resources) {
                            ctx._source["claims."+params.category+"."+resource] = 0;
                        }
                    }
                    if(!ctx._source["jobs."+params.category].contains(params.job)) {
                        ctx._source["jobs."+params.category].add(params.job);
                        for (resource in resources) {
                            if (params.requests.containsKey(resource)) {
                                ctx._source["claims."+params.category+"."+resource] += params.requests[resource];
                            }
                        }
                        // reset duration so that occupancy will be recalculated in
                        // the next run
                        ctx._source.duration = null;
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

    # Occupancy update, once per run
    es.put_script(
        options.indexname + "-update-occupancy",
        {
            "script": {
                "source": textwrap.dedent(
                    """
                    def RESOURCES = """
                    + repr(list(RESOURCES))
                    + """;
                    def STATUSES = """
                    + repr(list(STATUSES))
                    + """;
                    long current_duration = Duration.between(
                        LocalDateTime.parse(ctx._source.DaemonStartTime),
                        LocalDateTime.parse(ctx._source.LastHeardFrom)
                        ).toMillis()/1000;
                    if(ctx._source.duration == null
                        || ctx._source.duration != current_duration) {
                        ctx._source.duration = current_duration;
                        for (resource in RESOURCES) {
                            if (!ctx._source.containsKey("Total"+resource)) {
                                continue;
                            }
                            double norm = (current_duration*ctx._source["Total"+resource]).doubleValue();
                            if (!(norm > 0)) {
                                continue;
                            }
                            double total = 0;
                            for (status in STATUSES) {
                                def key = status+"."+resource;
                                // def blerh = ctx._source["claims."+key]/norm;
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
                        ctx.op = "noop";
                    }
                    """
                ),
                "lang": "painless",
                "context": "update",
            }
        },
    )

for coll_address in options.collectors:

    gen = es_generator(
        read_status_from_collector(coll_address, datetime.now() - options.after)
    )
    success = es_import(gen)

    # Update claims from evicted jobs
    gen = update_claims(
        read_from_collector(
            coll_address,
            constraint="(LastVacateTime > {}) && ((LastVacateTime-JobLastStartDate))>60".format(
                time.mktime((datetime.now() - timedelta(minutes=10)).timetuple())
            ),
            projection=[
                "GlobalJobId",
                "NumJobStarts",
                "JobLastStartDate",
                "LastVacateTime",
                "LastRemoteHost",
            ]
            + ["Request" + resource for resource in RESOURCES],
        ),
        history=False,
    )
    success = es_import(gen)

    # Update claims from finished jobs
    gen = update_claims(
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
                "LastRemoteHost",
            ]
            + ["Request" + resource for resource in RESOURCES],
            history=True,
        ),
        history=True,
    )
    success = es_import(gen)

# Normalize claims to running time to get occupancy of each slot. Note that
# these updates can conflict with the ones issued just before by update_claims.
# Skip these with conflicts=proceed, as they will get picked up in the next run
# anyhow.
if not options.dry_run:
    (
        edsl.UpdateByQuery(index=options.indexname, using=es)
        .filter("range", LastHeardFrom={"gte": datetime.utcnow() - options.after},)
        .script(id=options.indexname + "-update-occupancy",)
        .params(conflicts="proceed")
    ).execute()
