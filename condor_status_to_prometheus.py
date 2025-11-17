#!/usr/bin/env python3

import os
import glob
from optparse import OptionParser
import logging
from functools import partial
import htcondor2 as htcondor
from condor_utils import *
import prometheus_client
from condor_metrics import *
from itertools import chain

def get_job_state(ad):
    jobstatus = None

    if ad["LastJobStatus"] == 1:
        jobstatus = 'Idle'
    elif ad["LastJobStatus"] == 2:
        jobstatus = 'Running'
    elif ad["LastJobStatus"] == 5:
        jobstatus = 'Held'

    return jobstatus

def generate_ads(entries):
    for data in entries:
        add_classads(data)
        yield data

def compose_ad_metrics(ads):
    for ad in ads:
        labels = {key: None for key in metrics.labels}

        labels['schedd'] = ad['GlobalJobId'].split('#')[0]
        labels['state'] = get_job_state(ad)

        try:
            acct_group = ad['AccountingGroup']
            group = acct_group.split('.')[0]
        except Exception:
            group = "None"

        if group == 'Undefined': group = 'None'

        labels['group'] = group
        labels['owner'] = ad['Owner']

        metrics.condor_jobs_count.labels(**{'exit_code': ad['ExitCode'],**labels}).inc()
        metrics.condor_jobs_cpu_request.labels(**labels).inc(ad['RequestCpus'])
        metrics.condor_jobs_cputime.labels(**labels).inc(ad['RemoteUserCpu'])
        metrics.condor_jobs_disk_request_bytes.labels(**labels).inc(ad['RequestDisk']*1024)
        metrics.condor_jobs_disk_usage_bytes.labels(**labels).inc(ad['DiskUsage_RAW']*1024)
        metrics.condor_jobs_memory_request_bytes.labels(**labels).inc(ad['RequestMemory']*1024*1024)
        metrics.condor_jobs_memory_usage_bytes.labels(**labels).inc(ad['ResidentSetSize_RAW']*1024)
        metrics.condor_jobs_walltime.labels(**labels).inc(ad['walltimehrs']*3600)
        metrics.condor_jobs_wastetime.labels(**labels).inc((ad['walltimehrs']*3600)-ad['RemoteUserCpu'])

        if 'RequestGpus' in ad:
            metrics.condor_jobs_gpu_request.labels(**labels).inc(ad['RequestGpus'])

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

    parser = OptionParser('usage: %prog [options] history_files')

    parser.add_option('-c','--collectors',default=False, action='store_true',
                    help='read history from')

    parser.add_option('-p','--port', default=9100,
                    action='store', type='int',
                    help='port number for prometheus exporter')
    parser.add_option('-i','--interval', default=300,
                    action='store', type='int',
                    help='collector query interval in seconds')
    (options, args) = parser.parse_args()
    if not args:
        parser.error('no condor history files or collectors')

    metrics = JobMetrics()

    prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

    prometheus_client.start_http_server(options.port)

    if options.collectors:
        while True:
            gens = []
            start = time.time()
            for coll_address in args:
                try:
                    gens.append(read_from_collector(coll_address))
                except htcondor.HTCondorIOError as e:
                    failed = e
                    logging.error('Condor error', exc_info=True)
            gen = chain(*gens)
            metrics.slot_metrics.clear()
            compose_ad_metrics(generate_ads(gen))
            delta = time.time() - start
            
            if delta < options.interval:
                time.sleep(options.interval - delta)