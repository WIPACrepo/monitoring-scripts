
import os
import glob
import gzip
from optparse import OptionParser
from datetime import datetime,timedelta
import time
import logging
from collections import OrderedDict, Sequence
import re

import classad

now = datetime.utcnow()
zero = datetime.utcfromtimestamp(0).isoformat()

good_keys = {
    'JobStatus':0.,
    'Cmd':'',
    'Owner':'',
    'AccountingGroup':'',
    'ImageSize_RAW':0.,
    'DiskUsage_RAW':0.,
    'ExecutableSize_RAW':0.,
    'BytesSent':0.,
    'BytesRecvd':0.,
    'ResidentSetSize_RAW':0.,
    'RequestCpus':1.,
    'Requestgpus':0.,
    'RequestMemory':1000.,
    'RequestDisk':1000000.,
    'NumJobStarts':0.,
    'NumShadowStarts':0.,
    'GlobalJobId':'',
    'ClusterId':0.,
    'ProcId':0.,
    'ExitBySignal':False,
    'ExitCode':0.,
    'ExitSignal':0.,
    'ExitStatus':0.,
    'CumulativeSlotTime':0.,
    'LastRemoteHost':'',
    'QDate':now,
    'JobStartDate':now,
    'JobCurrentStartDate':now,
    'EnteredCurrentStatus':now,
    'RemoteUserCpu':0.,
    'RemoteSysCpu':0.,
    'CompletionDate':now,
    'CommittedTime':0.,
    'RemoteWallClockTime':0.,
    'MATCH_EXP_JOBGLIDEIN_ResourceName':'other',
    'MachineAttrGLIDEIN_SiteResource0':'other',
    'MachineAttrGPU_NAMES0':'',
    'PYGLIDEIN_METRIC_TIME_PER_PHOTON':-1., # need an int or float default for `filter_keys`
    'StartdPrincipal':'',
    'DAGManJobId':0.,
    'LastJobStatus':0.,
    'LastVacateTime':0.,
    'LastMatchTime':0.,
    'JobLastStartDate':0.,
    'LastHoldReason':'',
    'LastRemotePool':'',
    'PRESIGNED_GET_URL':'',
    'OriginalTime':0.,
    'MemoryUsage':0.,
}

site_key = 'MATCH_EXP_JOBGLIDEIN_ResourceName'

key_types = {
    'number': ['AutoClusterId','BlockReadBytes','BlockReadKbytes','BlockReads',
               'BlockWriteBytes','BlockWriteKbytes','BufferBlockSize','BufferSize',
               'BytesRecvd','BytesSent','ClusterId','CommittedSlotTime','CommittedSuspensionTime',
               'CommittedTime','CompletionDate','CoreSize','CumulativeSlotTime','CumulativeSuspensionTime',
               'CurrentHosts','DiskUsage','DiskUsage_RAW','EnteredCurrentStatus','ExecutableSize',
               'ExecutableSize_RAW','ExitCode','ExitStatus','ImageSize','ImageSize_RAW',
               'JobCurrentStartDate','JobCurrentStartExecutingDate','JobFinishedHookDone',
               'JobLeaseDuration','JobLeaseExpiration','JobNotification','JobPrio','JobRunCount','JobStartDate',
               'JobStatus','JobUniverse','LastJobLeaseRenewal','LastJobStatus','LastMatchTime','JobLastStartDate',
               'LastSuspensionTime','LastVacateTime','LocalSysCpu','LocalUserCpu','MachineAttrCpus0','MachineAttrSlotWeight0',
               'MaxHosts','MinHosts','NumCkpts','NumCkpts_RAW','NumJobMatches','NumJobStarts',
               'NumRestarts','NumShadowStarts','NumSystemHolds','OrigMaxHosts','ProcId', 'PYGLIDEIN_METRIC_TIME_PER_PHOTON', 'QDate',
               'Rank','RecentBlockReadBytes','RecentBlockReadKbytes','RecentBlockReads',
               'RecentBlockWriteBytes','RecentBlockWriteKbytes','RecentBlockWrites',
               'RecentStatsLifetimeStarter','RecentStatsTickTimeStarter','RecentWindowMaxStarter',
               'RemoteSysCpu','RemoteUserCpu','RemoteWallClockTime','RequestCpus','RequestDisk',
               'RequestMemory','Requestgpus','ResidentSetSize','ResidentSetSize_RAW',
               'StatsLastUpdateTimeStarter','StatsLifetimeStarter','TotalSuspensions',
               'TransferInputSizeMB'],
    'bool': ['EncryptExecuteDirectory','ExitBySignal','LeaveJobInQueue','NiceUser','OnExitHold',
             'OnExitRemove','PeriodicHold','PeriodicRelease','StreamErr','StreamOut',
             'TerminationPending','TransferIn','WantCheckpoint','WantRemoteIO',
             'WantRemoteSyscalls','wantglidein','wantrhel6'],
}


reserved_ips = {
    '18.12': 'MIT',
    '23.22': 'AWS',
    '35.9': 'MSU',
    '40.78': 'Azure',
    '40.112': 'Azure',
    '50.16': 'AWS',
    '50.17': 'AWS',
    '54.144': 'AWS',
    '54.145': 'AWS',
    '54.157': 'AWS',
    '54.158': 'AWS',
    '54.159': 'AWS',
    '54.161': 'AWS',
    '54.163': 'AWS',
    '54.166': 'AWS',
    '54.167': 'AWS',
    '54.197': 'AWS',
    '54.204': 'AWS',
    '54.205': 'AWS',
    '54.211': 'AWS',
    '54.227': 'AWS',
    '54.243': 'AWS',
    '72.36': 'Illinois',
    '128.9': 'osgconnect',
    '128.55': 'Berkeley',
    '128.84': 'NYSGRID_CORNELL_NYS1',
    '128.104': 'CHTC',
    '128.105': 'CHTC',
    '128.118': 'Bridges',
    '128.120': 'UCD',
    '128.205': 'osgconnect',
    '128.211': 'Purdue-Hadoop',
    '128.227': 'FLTech',
    '128.230': 'Syracuse',
    '129.74': 'NWICG_NDCMS',
    '129.93': 'Nebraska',
    '129.105': 'NUMEP-OSG',
    '129.107': 'UTA_SWT2',
    '129.119': 'SU-OG',
    '129.128': 'illume',
    '129.130': 'Kansas',
    '129.217': 'LIDO_Dortmund',
    '130.74': 'Miss',
    '130.127': 'Clemson-Palmetto',
    '130.199': 'BNL-ATLAS',
    '131.94': 'FLTECH',
    '131.215': 'CIT_CMS_T2',
    '131.225': 'USCMS-FNAL-WC1',
    '132.206': 'CA-MCGILL-CLUMEQ-T2',
    '133.82': 'Chiba',
    '134.93': 'mainz',
    '136.145': 'osgconnect',
    '137.99': 'UConn-OSG',
    '137.135': 'Azure',
    '138.23': 'UCRiverside',
    '138.91': 'Azure',
    '141.34': 'DESY-HH',
    '142.150': 'CA-SCINET-T2',
    '142.244': 'Alberta',
    '144.92': 'HEP_WISC',
    '149.165': 'Indiana',
    '155.101': 'Utah',
    '163.118': 'FLTECH',
    '169.228': 'UCSDT2',
    '171.67': 'HOSTED_STANFORD',
    '174.129': 'AWS',
    '184.73': 'AWS',
    '192.5': 'Boston',
    '192.12': 'Colorado',
    '192.41': 'AGLT2',
    '192.84': 'Ultralight',
    '192.168': None,
    '192.170': 'MWT2',
    '193.58': 'T2B_BE_IIHE',
    '193.190': 'T2B_BE_IIHE',
    '198.32': 'osgconnect',
    '198.48': 'Hyak',
    '198.202': 'Comet',
    '200.136': 'SPRACE',
    '200.145': 'SPRACE',
    '206.12': 'CA-MCGILL-CLUMEQ-T2',
    '216.47': 'MWT2',
}
reserved_ips.update({'10.%d'%i:None for i in range(256)})
reserved_ips.update({'172.%d'%i:None for i in range(16,32)})

reserved_domains = {
    'aglt2.org': 'AGLT2',
    'bridges.psc.edu': 'Bridges',
    'campuscluster.illinois.edu': 'Illinois',
    'cl.iit.edu': 'MWT2',
    'cm.cluster': 'LIDO_Dortmund',
    'cmsaf.mit.edu': 'MIT',
    'colorado.edu': 'Colorado',
    'cpp.ualberta.ca': 'Alberta',
    'crc.nd.edu': 'NWICG_NDCMS',
    'cci.wisc.edu': 'CHTC',
    'chtc.wisc.edu': 'CHTC',
    'cs.wisc.edu': 'CS_WISC',
    'cse.buffalo.edu': 'osgconnect',
    'discovery.wisc.edu': 'CHTC',
    'ec2.internal': 'AWS',
    'ember.arches': 'Utah',
    'fnal.gov': 'USCMS-FNAL-WC1',
    'grid.tu-dortmund.de': 'LIDO_Dortmund',
    'guillimin.clumeq.ca': 'Guillimin',
    'hcc.unl.edu': 'Crane',
    'hep.caltech.edu': 'CIT_CMS_T2',
    'hep.int': 'osgconnect',
    'hep.olemiss.edu': 'Miss',
    'hep.wisc.edu': 'HEP_WISC',
    'icecube.wisc.edu': 'NPX',
    'ics.psu.edu': 'Bridges',
    'iihe.ac.be': 'T2B_BE_IIHE',
    'illume.systems': 'illume',
    'internal.cloudapp.net': 'osgconnect',
    'isi.edu': 'osgconnect',
    'iu.edu': 'Indiana',
    'lidocluster.hp': 'LIDO_Dortmund',
    'math.wisc.edu': 'MATH_WISC',
    'mwt2.org': 'MWT2',
    'msu.edu': 'MSU',
    'nut.bu.edu': 'Boston',
    'palmetto.clemson.edu': 'Clemson-Palmetto',
    'panther.net': 'FLTECH',
    'phys.uconn.edu': 'UConn-OSG',
    'rcac.purdue.edu': 'Purdue-Hadoop',
    'research.northwestern.edu': 'NUMEP-OSG',
    'sdsc.edu': 'Comet',
    'stat.wisc.edu': 'CHTC',
    't2.ucsd.edu': 'UCSDT2',
    'tier3.ucdavis.edu':'UCD',
    'unl.edu': 'Nebraska',
    'uppmax.uu.se': 'Uppsala',
    'usatlas.bnl.gov': 'BNL-ATLAS',
    'wisc.cloudlab.us': 'CLOUD_WISC',
    'wisc.edu': 'WISC',
    'zeuthen.desy.de': 'DESY-ZN',
}

def get_site_from_resource(resource):
    site_names = {
        'DESY-ZN': 'DE-DESY',
        'DESY-HH': 'DE-DESY',
        'DESY': 'DE-DESY',
        'Brussels': 'BE-IIHE',
        'T2B_BE_IIHE': 'BE-IIHE',
        'BEgrid-ULB-VUB': 'BE-IIHE',
        'Guillimin': 'CA-McGill',
        'CA-MCGILL-CLUMEQ-T2': 'CA-McGill',
        'mainz': 'DE-Mainz',
        'mainzgrid': 'DE-Mainz',
        'Mainz_MogonI': 'DE-Mainz',
        'CA-SCINET-T2': 'CA-Toronto',
        'Alberta': 'CA-Alberta',
        'parallel': 'CA-Alberta',
        'jasper': 'CA-Alberta',
        'Illume': 'CA-Alberta',
        'illume': 'CA-Alberta',
        'illume-new': 'CA-Alberta',
        'Cedar': 'CA-SFU',
        'RWTH-Aachen': 'DE-Aachen',
        'RWTHaachen': 'DE-Aachen',
        'aachen': 'DE-Aachen',
        'wuppertalprod': 'DE-Wuppertal',
        'TUM': 'DE-Munich',
        'Uppsala': 'SE-Uppsala',
        'Bartol': 'US-Bartol',
        'UNI-DORTMUND': 'DE-Dortmund',
        'LIDO_Dortmund': 'DE-Dortmund',
        'PHIDO_Dortmund': 'DE-Dortmund',
        'LIDO3_Dortmund_TEST': 'DE-Dortmund',
        'LiDO3_Dortmund': 'DE-Dortmund',
        'UKI-NORTHGRID-MAN-HEP': 'UK-Manchester',
        'UKI-LT2-QMUL': 'UK-Manchester',
        'Bridges': 'US-XSEDE-PSC',
        'Comet': 'US-XSEDE-SDSC',
        'HOSTED_STANFORD': 'XSEDE-XStream',
        'Xstream': 'US-XSEDE-Stanford',
        'xstream': 'US-XSEDE-Stanford',
        'NPX': 'US-NPX',
        'GZK': 'US-GZK',
        'CHTC': 'US-CHTC',
        'Marquette': 'US-Marquette',
        'UMD': 'US-UMD',
        'MSU': 'US-MSU',
        'msu': 'US-MSU',
        'PSU': 'US-PSU',
        'Japan': 'JP-Chiba',
        'Chiba': 'JP-Chiba',
        'nbi': 'DK-NBI',
        'NBI': 'DK-NBI',
        'NBI_T3': 'DK-NBI',
        'SDSC-PRP': 'US-OSG-UCSD',
        'SU-ITS-CE3': 'US-OSG-Syracuse',
        'SU-ITS-CE2': 'US-OSG-Syracuse',
        'Syracuse': 'US-OSG-Syracuse',
        'Crane': 'US-OSG-Crane',
        'UCSDT2': 'US-OSG-UCSDT2',
        'BNL-ATLAS': 'US-OSG-BNL-ATLAS',
        'CIT_CMS_T2': 'US-OSG-Caltech-HEP',
        'Indiana': 'US-OSG-MWT2',
        'MWT2':  'US-OSG-MWT2',
        'Stampede2': 'US-XSEDE-TACC',
        'Clemson-Palmetto': 'US-OSG-Clemson',
        'SLATE_US_UIUC_HTC': 'US-OSG-UIUC',
        'SLATE_US_UUTAH_KINGSPEAK': 'US-OSG-UUTAH',
        'SLATE_US_UUTAH_LONEPEAK': 'US-OSG-UUTAH',
        'SLATE_US_UUTAH_NOTCHPEAK': 'US-OSG-UUTAH',
    }
    if resource in site_names:
        return site_names[resource]
    elif resource.startswith('SLATE'):
        parts = resource.split('_')[1:]
        return '-'.join([parts[0], 'OSG', parts[1]])
    else:
        return 'other'

def get_country_from_site(site):
    if site == 'other':
        return 'other'
    else:
        return site.split('-')[0]

def get_institution_from_site(site):
    if site == 'other':
        return 'other'
    else:
        parts = site.split('-')
        if 'OSG' in parts:
            return 'OSG'
        elif 'XSEDE' in parts:
            return 'XSEDE'
        else:
            return '-'.join(parts[1:])

# pre-estimated values
gpu_ns_photon = OrderedDict([
    ('680', 44.0),
    ('750 ti', 72.73),
    ('980', 22.8),
    ('1080 ti', 7.39),
    ('1080', 12.32),
    ('2080 ti', 4.37),
    ('titan xp', 8.69),
    ('titan x', 7.46),
    ('k20', 33.01),
    ('k40', 29.67),
    ('k80', 23.97),
    ('m2075', 133.0), # this is a guess
    ('m60', 13.31),
    ('m40', 9.20),
    ('p100', 7.04),
    ('p40', 5.37),
    ('p4', 16.77),
    ('v100', 3.02),
])

def normalize_gpu(job, key='gpuhrs',
    site_key='MATCH_EXP_JOBGLIDEIN_ResourceName',
    gpunames_key='MachineAttrGPU_NAMES0'
    ):
    """
    Will try to normalize GPUhrs found in `job` dictionary
    w.r.t. a reference model defined below. If all methods
    fail, no normalization is applied. Instead, the
    unnormalized GPUhrs will be assumed to be normalized.
    """
    gpu_ns_per_photon_key = 'PYGLIDEIN_METRIC_TIME_PER_PHOTON'
    gpu_ns_photon_ref = gpu_ns_photon['1080']
    norm_key = '{}_normalized'.format(key)
    raw_key = key
    nonnorm_key = '{}_nonnormalized'.format(key)
    # start off by assuming no normalization is necessary
    if job.get(raw_key, 0) == 0:
        return
    job[norm_key] = job[raw_key]

    def normalize_gpuhrs(job, gpu_identifier=None):
        """
        Actual logic for normalizing GPUhrs

        Parameters:
        -----------
        job : dict
        gpu_identifier : string or list of strings

        """
        if job.get(gpu_ns_per_photon_key, 0) > 0.:
            # glidein reported a (sensical) value, takes preference
            norm_factor = job[gpu_ns_per_photon_key]/gpu_ns_photon_ref
            job[norm_key] = float(job[raw_key]/norm_factor)
        elif gpu_identifier is not None:
            # value not reported, look up GPU model spec. in data base
            # prepare for taking averages of multiple gpu types
            if isinstance(gpu_identifier, str):
                gpu_identifier = [gpu_identifier]
            all_known = all(id in gpu_ns_photon for id in gpu_identifier)
            if not all_known:
                return
            # weights for each model could easily be created as additional parameters
            weight_factors = [1. for id in gpu_identifier]
            weighted_ns_per_photon = 0.
            for (weight, id) in zip(weight_factors, gpu_identifier):
                weighted_ns_per_photon += weight * gpu_ns_photon[id]
            norm_factor = weighted_ns_per_photon/(sum(weight_factors) * gpu_ns_photon_ref)
            job[norm_key] = float(job[raw_key]/norm_factor)
        # otherwise, nothing to do here
        return

    if job.get(gpu_ns_per_photon_key, 0) > 0.:
        normalize_gpuhrs(job)
        return
    if gpunames_key in job:
        # glidein reported gpu name, try to find a match
        gpu_name = job[gpunames_key].split(',')[0].lower()
        for name in gpu_ns_photon:
            if name in gpu_name:
                normalize_gpuhrs(job, gpu_identifier=name)
                return
    if site_key in job:
        site_resource = job[site_key].lower()
        if site_resource == 'crane': # Nebraska has k20
            normalize_gpuhrs(job, gpu_identifier='k20')
            return
        elif 'su-its' in site_resource: # SU has 750 ti
            normalize_gpuhrs(job, gpu_identifier='750 ti')
            return
        elif site_resource == 'sdsccompinfrastructure': # likely to be 1080 ti (~95% odds)
            normalize_gpuhrs(job, gpu_identifier='1080 ti')
            return
        elif site_resource == 'sdsc-prp': # likely to be 1080 ti (~95% odds)
            normalize_gpuhrs(job, gpu_identifier='1080 ti')
            return
        elif site_resource == 'ucsdt2': # comet has k80 and p100
            normalize_gpuhrs(job, gpu_identifier=['k80', 'p100'])
            return
        elif 'su-og' in site_resource: # SU has 750 ti
            normalize_gpuhrs(job, gpu_identifier='750 ti')
            return
        elif 'aachen' in site_resource: # now has p100
            normalize_gpuhrs(job, gpu_identifier='p100')
            return
        elif 't2b_be_iihe' in site_resource: # Tesla M2075
            normalize_gpuhrs(job, gpu_identifier='m2075')
            return
    if 'MachineAttrGLIDEIN_SiteResource0' in job:
        site_resource = job['MachineAttrGLIDEIN_SiteResource0'].lower()
        if site_resource == 'umd': # UMD has 1080
            normalize_gpuhrs(job, gpu_identifier='1080')
            return
        elif site_resource == 'msu': # the older msu cluster, k40
            normalize_gpuhrs(job, gpu_identifier='k40')
            return
    if 'LastRemoteHost' in job:
        host = job['LastRemoteHost'].lower()
        if host.endswith('.icecube.wisc.edu'):
            if 'rad' in host or ('gtx' in host and int(host.split('gtx-',1)[-1].split('.',1)[0]) < 10):
                # this is a 1080
                normalize_gpuhrs(job, gpu_identifier='1080')
                return
            else:
                # this is a 980
                normalize_gpuhrs(job, gpu_identifier='980')
                return
        elif host.endswith('crane.hcc.unl.edu'):
            # Nebraska has k20
            normalize_gpuhrs(job, gpu_identifier='k20')
            return
        elif host.endswith('syr.edu'): # SU has 750 ti
            normalize_gpuhrs(job, gpu_identifier='750 ti')
            return

    job[nonnorm_key] = job[raw_key]

def get_site_from_domain(hostname):
    parts = hostname.lower().split('.')
    ret = None
    if '.'.join(parts[-3:]) in reserved_domains:
        ret = reserved_domains['.'.join(parts[-3:])]
    elif '.'.join(parts[-2:]) in reserved_domains:
        ret = reserved_domains['.'.join(parts[-2:])]
    return ret

def get_site_from_ip_range(ip):
    parts = ip.split('.')
    ret = None
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        if '.'.join(parts[:2]) in reserved_ips:
            ret = reserved_ips['.'.join(parts[:2])]
    return ret

def date_from_string(s):
    if '.' in s:
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f')
    else:
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')

def filter_keys(data):
    # RequestGPUs comes in many cases
    for k in list(data):
        if k.lower() == 'requestgpus' and k != 'Requestgpus':
            data['Requestgpus'] = data[k]
            del data[k]

    for k in list(data.keys()):
        if not (k in good_keys or ('IceProd' in k)):
            del data[k]
    for k in good_keys:
        if k not in data:
            data[k] = good_keys[k]
        if isinstance(good_keys[k],bool):
            try:
                data[k] = bool(data[k])
            except:
                logging.info('bad bool [%s]: %r', k, data[k], exc_info=True)
                data[k] = good_keys[k]
        elif isinstance(good_keys[k],(float,int)):
            try:
                data[k] = float(data[k])
            except:
                logging.info('bad float/int [%s]: %r', k, data[k], exc_info=True)
                data[k] = good_keys[k]
        elif isinstance(good_keys[k],datetime):
            if isinstance(data[k], datetime):
                data[k] = data[k].isoformat()
            else:
                try:
                    data[k] = datetime.utcfromtimestamp(data[k]).isoformat()
                except:
                    logging.info('bad date [%s]: %r', k, data[k], exc_info=True)
                    data[k] = zero
        else:
            data[k] = str(data[k])

def is_bad_site(data, site_key='MATCH_EXP_JOBGLIDEIN_ResourceName'):
    if site_key not in data:
        if 'MachineAttrGLIDEIN_SiteResource0' in data:
            data[site_key] = data['MachineAttrGLIDEIN_SiteResource0']
        else:
            return True
    site = data[site_key]
    if site in ('other','osgconnect','xsede-osg','WIPAC','wipac'):
        return True
    if '.' in site:
        return True
    if site.startswith('gzk9000') or site.startswith('gzk-'):
        return True
    return False

def add_classads(data):
    """Add extra classads to a condor job

    Args:
        data (dict): a classad dict for a single job
    """
    filter_keys(data)
    # fix site
    bad_site = is_bad_site(data)
    if bad_site:
        if 'LastRemoteHost' in data:
            site = get_site_from_domain(data['LastRemoteHost'].split('@')[-1])
            if site:
                data[site_key] = site
            elif 'StartdPrincipal' in data:
                site = get_site_from_ip_range(data['StartdPrincipal'].split('/')[-1])
                if site:
                    data[site_key] = site

    data['@timestamp'] = datetime.utcnow().isoformat()
    # add completion date
    if data['CompletionDate'] != zero and data['CompletionDate']:
        data['date'] = data['CompletionDate']
    elif data['EnteredCurrentStatus'] != zero and data['EnteredCurrentStatus']:
        data['date'] = data['EnteredCurrentStatus']
    else:
        data['date'] = datetime.utcnow().isoformat()
    # add queued time
    if data['JobCurrentStartDate'] != zero and data['JobCurrentStartDate']:
        data['queue_time'] = date_from_string(data['JobCurrentStartDate']) - date_from_string(data['QDate'])
    else:
        data['queue_time'] = datetime.now() - date_from_string(data['QDate'])
    data['queue_time'] = data['queue_time'].total_seconds()/3600.
    # add used time
    if 'RemoteWallClockTime' in data:
        data['totalwalltimehrs'] = data['RemoteWallClockTime']/3600.
    else:
        data['totalwalltimehrs'] = 0.
    if 'CommittedTime' in data and data['CommittedTime']:
        data['walltimehrs'] = data['CommittedTime']/3600.
    elif ('LastVacateTime' in data and data['LastVacateTime']
          and 'JobLastStartDate' in data and data['JobLastStartDate']):
        data['walltimehrs'] = (data['LastVacateTime']-data['JobLastStartDate'])/3600.
    else:
        data['walltimehrs'] = 0.

    # fix Illume-MSU mixup
    if site_key in data and data[site_key] == 'MSU' and data['date'] <= '2019-03-30' and data['date'] >= '2018-12-01':
        data[site_key] = 'Illume'

    # add site
    data['site'] = get_site_from_resource(data[site_key])

    # add countries
    data['country'] = get_country_from_site(data['site'])

    # add institution
    data['institution'] = get_institution_from_site(data['site'])

    # Add gpuhrs and cpuhrs
    data['gpuhrs'] = data.get('Requestgpus', 0.) * data['walltimehrs']
    data['cpuhrs'] = data.get('RequestCpus', 0.) * data['walltimehrs']

    # add normalized gpu hours
    normalize_gpu(data)

    # add retry hours
    data['retrytimehrs'] = data['totalwalltimehrs'] - data['walltimehrs']

def classad_to_dict(c):
    ret = {}
    for k in c.keys():
        try:
            ret[k] = c.eval(k)
        except TypeError:
            ret[k] = c[k]
    return ret

def read_from_file(filename):
    """Read condor classads from file.

    A generator that yields condor job dicts.

    Args:
        filename (str): filename to read
    """
    with (gzip.open(filename) if filename.endswith('.gz') else open(filename)) as f:
        entry = ''
        for line in f.readlines():
            if line.startswith('***'):
                try:
                    c = classad.parseOne(entry)
                    yield classad_to_dict(c)
                    entry = ''
                except:
                    entry = ''
            else:
                entry += line+'\n'

def read_from_collector(address, history=False, constraint='true', projection=[]):
    """Connect to condor collectors and schedds to pull job ads directly.

    A generator that yields condor job dicts.

    Args:
        address (str): address of collector
        history (bool): read history (True) or active queue (default: False)
    """
    import htcondor
    coll = htcondor.Collector(address)
    schedd_ads = coll.locateAll(htcondor.DaemonTypes.Schedd)
    for schedd_ad in schedd_ads:
        logging.info('getting job ads from %s', schedd_ad['Name'])
        schedd = htcondor.Schedd(schedd_ad)
        try:
            i = 0
            if history:
                start_dt = datetime.now()-timedelta(minutes=10)
                start_stamp = time.mktime(start_dt.timetuple())
                gen = schedd.history('(EnteredCurrentStatus >= {0}) && ({1})'.format(start_stamp,constraint),projection,10000)
            else:
                gen = schedd.query(constraint, projection)
            for i,entry in enumerate(gen):
                yield classad_to_dict(entry)
            logging.info('got %d entries', i)
        except Exception:
            logging.info('%s failed', schedd_ad['Name'], exc_info=True)

def read_status_from_collector(address, after=datetime.now()-timedelta(hours=1)):
    """Connect to condor collectors and schedds to pull job ads directly.

    A generator that yields condor job dicts.

    Args:
        address (str): address of collector
        history (bool): read history (True) or active queue (default: False)
    """
    import htcondor
    coll = htcondor.Collector(address)
    start_stamp = time.mktime(after.timetuple())
    final_keys = [
        "Name",
        "DaemonStartTime",
        "LastHeardFrom",
        "TotalCpus",
        "TotalDisk",
        "TotalMemory",
        "TotalGPUs",
        "Arch",
        "OpSysAndVer",
        "GLIDEIN_Site",
        "GLIDEIN_SiteResource",
    ]
    temp_keys = [
        "AddressV1",
        "GPU_NAMES",
    ]
    site_key = "GLIDEIN_SiteResource"
    try:
        gen = coll.query(
            htcondor.AdTypes.Startd,
            (
                "SlotType isnt \"Dynamic\" && LastHeardFrom>={}"
                .format(start_stamp)
            ),
            final_keys + temp_keys
        )
        i = 0
        for i,entry in enumerate(gen):
            data = classad_to_dict(entry)
            for k in "DaemonStartTime", "LastHeardFrom":
                data[k] = datetime.utcfromtimestamp(data[k])
            data["@timestamp"] = [data["DaemonStartTime"]]
            if data["LastHeardFrom"] > data["DaemonStartTime"]:
                data["@timestamp"].append(data["LastHeardFrom"])
            data["duration"] = int((data["LastHeardFrom"]-data["DaemonStartTime"]).total_seconds())
            # Pick up resource names from OSG glideins (GLIDEIN_SiteResource) and pyglidein (usually GLIDEIN_ResourceName="ResourceName")
            if data.get(site_key, 'ResourceName') == 'ResourceName':
                data[site_key] = data['GLIDEIN_Site']
            data['resource'] = data[site_key]

            # add site
            if is_bad_site(data, site_key):
                site = get_site_from_domain(data['Name'].split('@')[-1])
                if site:
                    data['site'] = site
                else:
                    ip = re.match(r'.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*', data['AddressV1']).group(1)
                    site = get_site_from_ip_range(ip)
                    if site:
                        data['site'] = site
                    else:
                        data['site'] = 'other'
            else:
                data['site'] = get_site_from_resource(data[site_key])

            # add countries
            data['country'] = get_country_from_site(data['site'])

            # add institution
            data['institution'] = get_institution_from_site(data['site'])

            for k in list(data.keys()):
                if k.startswith('GLIDEIN'):
                    del data[k]
            normalize_gpu(data, 'TotalGPUs', 'GLIDEIN_SiteResource', 'GPU_NAMES')
            for k in temp_keys:
                if k in data:
                    del data[k]
            yield data
        logging.info('got %d entries', i)
    except Exception:
        logging.info('failed', exc_info=True)
