#!/usr/bin/env python3
import os, sys
import glob
from optparse import OptionParser
import logging
import htcondor, classad
from condor_utils import *
from condor_job_metrics import JobMetrics
import prometheus_client
import time

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
    kind = 'CPU'
    device_name = ''
    owner = ad['Owner']
    site = ad['MATCH_EXP_JOBGLIDEIN_ResourceName']
    walltimehrs = ad['cpuhrs']

    if ad['Requestgpus'] > 0:
        kind = 'GPU'
        device_name = ad['MachineAttrGPUs_DeviceName0']
        walltimehrs = ad['gpuhrs']

    # ignore this ad if walltimehrs is negative
    if walltimehrs < 0:
        return
    
    labels = [owner,site,kind,device_name]

    metrics.condor_total_job_hours.labels(*labels).inc(walltimehrs)
    metrics.condor_job_count.labels(*labels).inc()
    metrics.condor_total_mem_req.labels(*labels).inc(ad['RequestMemory'])

    if ad['ExitCode'] == 0 or ['adExitBySignal'] is False or ['adJobStatus'] == 4:
        metrics.condor_bad_job_hours.labels(*labels).inc(walltimehrs)
        metrics.condor_bad_mem_req.labels(*labels).inc(ad['RequestMemory'])
    else:
        metrics.condor_good_job_hours.labels(*labels).inc(walltimehrs)
        metrics.condor_good_mem_req.labels(*labels).inc(ad['RequestMemory'])

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

    parser = OptionParser('usage: %prog [options] history_files')

    parser.add_option('-d','--daemon',default=False, action='store_true',
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

    if options.histfile:
        for path in args:
            for filename in glob.iglob(path):
                ads = read_from_file(options.histfile)

    if options.daemon:
        try:
            ads = read_from_collector(args, history=True)
        except htcondor.HTCondorIOError as e:
            failed = e
            logging.error(f'Condor error: {e}')

    metrics = JobMetrics()

    prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

    prometheus_client.start_http_server(options.port)

    last_job = {'ClusterId': None, 'EnteredCurrentStatus': None}
    
    while True:
        if options.histfile:
            for path in args:
                for filename in glob.iglob(path):
                    ads = read_from_file(options.histfile)

        if options.daemon:
            try:
                ads = read_from_collector(args, history=True, since=last_job['ClusterId'])
            except htcondor.HTCondorIOError as e:
                failed = e
                logging.error(f'Condor error: {e}')
        
        for ad in generate_ads(ads):
            if last_job['ClusterId'] is not None:
                if ad['EnteredCurrentStatus'] > last_job['EnteredCurrentStatus']:
                    last_job['ClusterId'] = ad['ClusterId']
                    last_job['EnteredCurrentStatus'] = ad['EnteredCurrentStatus']
            compose_ad_metrics(ad, metrics)
        time.sleep(options.interval)

        