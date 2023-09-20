#!/usr/bin/env python3
import os, sys
import glob
from optparse import OptionParser
import logging
import htcondor, classad
from condor_utils import *
from condor_job_metrics import JobMetrics
from prometheus_client import start_http_server

def generate_ads(entries):
    for data in entries:
        add_classads(data)
        yield data

def compose_ad_metrics(ad, metrics):
    kind = 'CPU'
    device_name = ''
    owner = ad['Owner']
    site = ad['MATCH_EXP_JOBGLIDEIN_ResourceName']

    if ad['Requestgpus'] > 0:
        kind = 'GPU'
        device_name = ad['MachineAttrGPUs_DeviceName0']
        walltimehrs = ad['gpuhrs']

    #ignore this ad if walltimehrs is negative
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

    start_http_server(options.port)
    
    while True:
        for ad in generate_ads(ads):
            compose_ad_metrics(ad, metrics)
        time.sleep(options['interval'])

        