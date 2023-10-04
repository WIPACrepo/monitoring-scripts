#!/usr/bin/env python3
import os, sys
import glob
from optparse import OptionParser
import logging
import htcondor, classad
from condor_utils import *
from condor_job_metrics import JobMetrics
import datetime
import time
import prometheus_client
from datetime import datetime
import re

utc_format = '%Y-%m-%dT%H:%M:%S'

def generate_ads(entries):
    for data in entries:
        add_classads(data)
        yield data

def compose_ad_metrics(ad, metrics):
    ''' Parse condor job classad and update metrics

        Args:
            ad (classad): an HTCondor job classad
            metrics (JobMetrics): JobMetrics object 
    '''
    labels = {'owner': None,
              'site': None,
              'schedd': None,
              'GPUDeviceName': None,
              'usage': None,
              'kind': None}

    labels['owner'] = ad['Owner']
    labels['site'] = ad['MATCH_EXP_JOBGLIDEIN_ResourceName']
    labels['schedd'] = ad['GlobalJobId'][0:ad['GlobalJobId'].find('#')]
    labels['GPUDeviceName'] = None

    # ignore this ad if walltimehrs is negative
    if ad['walltimehrs'] < 0:
        return
    
    metrics.condor_job_walltime_hours.labels(**labels).inc(ad['walltimehrs'])
    
    if ad['ExitCode'] == 0 and ad['ExitBySignal'] is False and ad['JobStatus'] == 4:
        labels['usage'] = 'goodput'
    else:
        labels['usage'] = 'badput'

    if ad['Requestgpus'] > 0:
        labels['kind'] = 'GPU'
        labels['GPUDeviceName'] = ad['MachineAttrGPUs_DeviceName0']
        resource_hrs = ad['gpuhrs']
        resource_request = ad['Requestgpus']
    else:
        labels['kind'] = 'CPU'
        resource_hrs = ad['cpuhrs']
        resource_request = ad['RequestCpus']

    metrics.condor_job_count.labels(**labels).inc()
    metrics.condor_job_resource_hours.labels(**labels).inc(resource_hrs)
    metrics.condor_job_resource_req.labels(**labels).observe(resource_request)
    metrics.condor_job_mem_req.labels(**labels).observe(ad['RequestMemory']/1024)
    metrics.condor_job_mem_used.labels(**labels).observe(ad['ResidentSetSize_RAW']/1048576)

def query_collectors(collectors, metrics, options):
    if options.histfile:
        for path in collectors:
            for filename in glob.iglob(path):
                ads = read_from_file(options.histfile)

    if options.collectors:
        for collector in collectors:
            try:
                ads = read_from_collector(collector, history=True, since=last_job['ClusterId'])
            except htcondor.HTCondorIOError as e:
                failed = e
                logging.error(f'Condor error: {e}')

            for ad in generate_ads(ads):
                if last_job['ClusterId'] is None:
                    last_job['ClusterId'] = int(ad['ClusterId'])
                    last_job['EnteredCurrentStatus'] = ad['EnteredCurrentStatus']

                if datetime.strptime(ad['EnteredCurrentStatus'],utc_format) > datetime.strptime(last_job['EnteredCurrentStatus'],utc_format):
                    last_job['ClusterId'] = int(ad['ClusterId'])
                    last_job['EnteredCurrentStatus'] = ad['EnteredCurrentStatus']

                compose_ad_metrics(ad, metrics)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

    parser = OptionParser('usage: %prog [options] history_files')

    parser.add_option('-c','--collectors',default=False, action='store_true',
                    help='read history from')
    parser.add_option('-f','--histfile',
                    help='history file to read from')
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

    last_job = {'ClusterId': None, 'EnteredCurrentStatus': None}
    
    while True:
        query_collectors(args, metrics, options)
        time.sleep(options.interval)