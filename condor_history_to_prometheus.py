#!/usr/bin/env python3
from optparse import OptionParser
import logging
import htcondor
from condor_utils import *
from condor_job_metrics import JobMetrics
import datetime
import time
import prometheus_client
from datetime import datetime
from collections import defaultdict

utc_format = '%Y-%m-%dT%H:%M:%S'

def generate_ads(entries):
    for data in entries:
        add_classads(data)
        yield data

def last_jobs_dict(collector):
    last_job = defaultdict(dict)
    
    for collector in args:
        schedd_ads = locate_schedds(collector)
        if schedd_ads is None:
            return None
    
        for s in schedd_ads:
            last_job[s.get('Name')] = {'ClusterId': None, 'EnteredCurrentStatus': None}

    return last_job
    

def locate_schedds(collector):
    try:
        coll = htcondor.Collector(collector)
        return coll.locateAll(htcondor.DaemonTypes.Schedd)
    except htcondor.HTCondorIOError as e:
        failed = e
        logging.error(f'Condor error: {e}')

def compose_ad_metrics(ad, metrics):
    ''' Parse condor job classad and update metrics

        Args:
            ad (classad): an HTCondor job classad
            metrics (JobMetrics): JobMetrics object 
    '''
    # ignore this ad if walltimehrs is negative or a dagman
    if ad['walltimehrs'] < 0 or ad['Cmd'] == '/usr/bin/condor_dagman':
        return

    labels = {'owner': None,
              'site': None,
              'schedd': None,
              'GPUDeviceName': None,
              'usage': None,
              'kind': None}

    labels['owner'] = ad['Owner']
    labels['site'] = ad['site']
    labels['schedd'] = ad['GlobalJobId'][0:ad['GlobalJobId'].find('#')]
    labels['GPUDeviceName'] = None
    
    if ad['ExitCode'] == 0 and ad['ExitBySignal'] is False and ad['JobStatus'] == 4:
        labels['usage'] = 'goodput'
    else:
        labels['usage'] = 'badput'

    resource_hrs = 0
    resource_request = 0

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
    metrics.condor_job_walltime_hours.labels(**labels).inc(ad['walltimehrs'])
    metrics.condor_job_resource_hours.labels(**labels).inc(resource_hrs)
    metrics.condor_job_resource_req.labels(**labels).observe(resource_request)
    metrics.condor_job_mem_req.labels(**labels).observe(ad['RequestMemory']/1024)
    metrics.condor_job_mem_used.labels(**labels).observe(ad['ResidentSetSize_RAW']/1048576)

def query_collector(collector, metrics, last_job):
    """Query schedds for job ads

    Args:
        collector (str): address for a collector to query
        metrics (JobMetrics): JobMetrics instance
        last_job (dict): dictionary for tracking last ClusterId by schedd
    """
    for schedd_ad in locate_schedds(collector):
        name = schedd_ad.get('Name')

        ads = read_from_schedd(schedd_ad, history=True, since=last_job[name]['ClusterId'])
        if last_job[name]['EnteredCurrentStatus'] is not None:
            logging.info(f'{name} - read ads since {last_job[name]["ClusterId"]}:{last_job[name]["EnteredCurrentStatus"]} at timestamp {datetime.strptime(last_job[name]["EnteredCurrentStatus"],utc_format)}')

        for ad in generate_ads(ads):
            if last_job[name]['ClusterId'] is None:
                last_job[name]['ClusterId'] = int(ad['ClusterId'])
                last_job[name]['EnteredCurrentStatus'] = ad['EnteredCurrentStatus']

            if datetime.strptime(ad['EnteredCurrentStatus'],utc_format) > datetime.strptime(last_job[name]['EnteredCurrentStatus'],utc_format):
                last_job[name]['ClusterId'] = int(ad['ClusterId'])
                last_job[name]['EnteredCurrentStatus'] = ad['EnteredCurrentStatus']

            compose_ad_metrics(ad, metrics)

def read_from_schedd(schedd_ad, history=False, constraint='true', projection=[],match=10000,since=None):
        """Connect to schedd and pull ads directly.

        A generator that yields condor job dicts.

        Args:
            schedd (ClassAd): location_add of a schedd, from either htcondor.Colletor locate() or locateAll() 
            history (bool): read history (True) or active queue (default: False)
            constraint (string): string representation of a classad expression
            match (int): number of job ads to return
            since (int): JobId to return job ads after
        """
        import htcondor
        logging.info('getting job ads from %s', schedd_ad['Name'])
        schedd = htcondor.Schedd(schedd_ad)
        try:
            i = 0
            if history:
                if since is None:
                    start_dt = datetime.now()-timedelta(minutes=10)
                    start_stamp = time.mktime(start_dt.timetuple())
                    constraint = f'(EnteredCurrentStatus >= {start_stamp}) && ({constraint})'

                gen = schedd.history(constraint,projection,match=match,since=since)
            else:
                gen = schedd.query(constraint, projection)
            for i,entry in enumerate(gen):
                yield classad_to_dict(entry)
            logging.info('got %d entries', i)
        except Exception:
            logging.info('%s failed', schedd_ad['Name'], exc_info=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

    parser = OptionParser('usage: %prog [options] history_files')

    parser.add_option('-c','--collectors',default=False, action='store_true',
                    help='read history from')
    # TODO: Add file tail function for condor history files
    #parser.add_option('-f','--histfile',
    #                help='history file to read from')
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
        last_job = last_jobs_dict(args)

        if last_job is None:
            logging.error(f'No schedds found')
            exit()

        while True:
            start = datetime.now()
            for collector in args:
                query_collector(collector, metrics, last_job)

            delta = datetime.now() - start
            # sleep for interval minus scrape duration
            # if scrape duration was longer than interval, run right away
            if delta.seconds < options.interval:
                time.sleep(options.interval - delta.seconds)
