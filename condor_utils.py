from __future__ import print_function
import os
import glob
import gzip
from optparse import OptionParser
from datetime import datetime,timedelta
import time
import logging
from collections import OrderedDict, Sequence

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
    'PRESIGNED_GET_URL':''
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
    'CA-SCINET-T2': 'CA-Toronto',
    'Alberta': 'CA-Alberta',
    'parallel': 'CA-Alberta',
    'jasper': 'CA-Alberta',
    'Illume': 'CA-Alberta',
    'illume': 'CA-Alberta',
    'illume-new': 'CA-Alberta',
    'Cedar': 'CA-SFU',
    'RWTH-Aachen': 'DE-Aachen',
    'aachen': 'DE-Aachen',
    'wuppertalprod': 'DE-Wuppertal',
    'TUM': 'DE-Munich',
    'Uppsala': 'SE-Uppsala',
    'Bartol': 'US-Bartol',
    'UNI-DORTMUND': 'DE-Dortmund',
    'LIDO_Dortmund': 'DE-Dortmund',
    'PHIDO_Dortmund': 'DE-Dortmund',
    'LIDO3_Dortmund_TEST': 'DE-Dortmund',
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
    'Stampede2': 'US-XSEDE-TACC',
    'Clemson-Palmetto': 'US-OSG-Clemson'
}

countries = {
    'DESY-ZN': 'DE',
    'DESY-HH': 'DE',
    'DESY': 'DE',
    'Brussels': 'BE',
    'T2B_BE_IIHE': 'BE',
    'BEgrid-ULB-VUB': 'BE',
    'Guillimin': 'CA',
    'CA-MCGILL-CLUMEQ-T2': 'CA',
    'mainz': 'DE',
    'mainzgrid': 'DE',
    'CA-SCINET-T2': 'CA',
    'Alberta': 'CA',
    'parallel': 'CA',
    'jasper': 'CA',
    'Illume': 'CA',
    'illume': 'CA',
    'illume-new': 'CA',
    'Cedar': 'CA',
    'RWTH-Aachen': 'DE',
    'aachen': 'DE',
    'wuppertalprod': 'DE',
    'TUM': 'DE',
    'Uppsala': 'SE',
    'Bartol': 'US',
    'UNI-DORTMUND': 'DE',
    'LIDO_Dortmund': 'DE',
    'PHIDO_Dortmund': 'DE',
    'LIDO3_Dortmund_TEST': 'DE',
    'UKI-NORTHGRID-MAN-HEP': 'UK',
    'UKI-LT2-QMUL': 'UK',
    'Bridges': 'US',
    'Comet': 'US',
    'HOSTED_STANFORD': 'US',
    'Xstream': 'US',
    'xstream': 'US',
    'NPX': 'US',
    'GZK': 'US',
    'CHTC': 'US',
    'Marquette': 'US',
    'UMD': 'US',
    'MSU': 'US',
    'msu': 'US',
    'PSU': 'US',
    'Japan': 'JP',
    'Chiba': 'JP',
    'nbi': 'DK',
    'NBI': 'DK',
    'NBI_T3': 'DK',
    'SDSC-PRP': 'US',
    'SU-ITS-CE3': 'US',
    'SU-ITS-CE2': 'US',
    'Crane': 'US',
    'UCSDT2': 'US',
    'BNL-ATLAS': 'US',
    'CIT_CMS_T2': 'US',
    'Indiana': 'US',
    'Stampede2': 'US',
    'Clemson-Palmetto': 'US'
}

institutions = {
    'DESY-ZN': 'DESY',
    'DESY-HH': 'DESY',
    'DESY': 'DESY',
    'Brussels': 'VUB',
    'T2B_BE_IIHE': 'VUB',
    'BEgrid-ULB-VUB': 'VUB',
    'Guillimin': 'UAlberta',
    'CA-MCGILL-CLUMEQ-T2': 'UAlberta',
    'mainz': 'Mainz',
    'mainzgrid': 'Mainz',
    'CA-SCINET-T2': 'UAlberta',
    'Alberta': 'UAlberta',
    'parallel': 'UAlberta',
    'jasper': 'UAlberta',
    'Illume': 'UAlberta',
    'illume': 'UAlberta',
    'illume-new': 'UAlberta',
    'Cedar': 'UAlberta',
    'RWTH-Aachen': 'Aachen',
    'aachen': 'Aachen',
    'wuppertalprod': 'Wuppertal',
    'TUM': 'TUM',
    'Uppsala': 'Uppsala',
    'Bartol': 'Bartol',
    'UNI-DORTMUND': 'Dortmund',
    'LIDO_Dortmund': 'Dortmund',
    'PHIDO_Dortmund': 'Dortmund',
    'LIDO3_Dortmund_TEST': 'Dortmund',
    'UKI-NORTHGRID-MAN-HEP': 'QML',
    'UKI-LT2-QMUL': 'QML',
    'Bridges': 'XSEDE',
    'Comet': 'XSEDE',
    'HOSTED_STANFORD': 'OSG',
    'Xstream': 'XSEDE',
    'xstream': 'XSEDE',
    'NPX': 'UW',
    'GZK': 'UW',
    'CHTC': 'UW',
    'Marquette': 'Marquette',
    'UMD': 'UMD',
    'MSU': 'MSU',
    'msu': 'MSU',
    'PSU': 'PSU',
    'Japan': 'Chiba',
    'Chiba': 'Chiba',
    'nbi': 'NBI',
    'NBI': 'NBI',
    'NBI_T3': 'NBI',
    'SDSC-PRP': 'OSG',
    'SU-ITS-CE3': 'OSG',
    'SU-ITS-CE2': 'OSG',
    'Crane': 'OSG',
    'UCSDT2': 'OSG',
    'BNL-ATLAS': 'OSG',
    'CIT_CMS_T2': 'OSG',
    'Indiana': 'OSG',
    'Stampede2': 'XSEDE',
    'Clemson-Palmetto': 'OSG'
}

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

def normalize_gpu(job):
    """
    Will try to normalize GPUhrs found in `job` dictionary
    w.r.t. a reference model defined below. If all methods
    fail, no normalization is applied. Instead, the
    unnormalized GPUhrs will be assumed to be normalized.
    """
    gpu_ns_per_photon_key = 'PYGLIDEIN_METRIC_TIME_PER_PHOTON'
    gpu_ns_photon_ref = gpu_ns_photon['1080']
    norm_key = 'gpuhrs_normalized'
    raw_key = 'gpuhrs'
    nonnorm_key = 'gpuhrs_nonnormalized'
    # start off by assuming no normalization is necessary
    job[norm_key] = job[raw_key]

    def normalize_gpuhrs(job, gpu_identifier=None):
        """
        Actual logic for normalizing GPUhrs

        Parameters:
        -----------
        job : dict
        gpu_identifier : string or list of strings

        """
        if job[gpu_ns_per_photon_key] > 0.:
            # glidein reported a (sensical) value, takes preference
            norm_factor = job[gpu_ns_per_photon_key]/gpu_ns_photon_ref
            job[norm_key] = job[raw_key]/norm_factor
        elif gpu_identifier is not None:
            # value not reported, look up GPU model spec. in data base
            # prepare for taking averages of multiple gpu types
            if isinstance(gpu_identifier, basestring):
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
            job[norm_key] = job[raw_key]/norm_factor
        # otherwise, nothing to do here
        return

    if job[gpu_ns_per_photon_key] > 0.:
        normalize_gpuhrs(job)
        return
    if 'MachineAttrGPU_NAMES0' in job:
        # glidein reported gpu name, try to find a match
        gpu_name = job['MachineAttrGPU_NAMES0'].split(',')[0].lower()
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

    for k in data.keys():
        if k not in good_keys:
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

def is_bad_site(data):
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
    if data[site_key] in site_names:
        data['site'] = site_names[data[site_key]]
    else:
        data['site'] = 'other'

    # add countries
    if data[site_key] in countries:
        data['country'] = countries[data[site_key]]
    else:
        data['country'] = 'other'

    # add institution
    if data[site_key] in institutions:
        data['institution'] = institutions[data[site_key]]
    else:
        data['institution'] = 'other'

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

def read_from_collector(address, history=False):
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
                gen = schedd.history('EnteredCurrentStatus >= {0}'.format(start_stamp),[],10000)
            else:
                gen = schedd.query()
            for i,entry in enumerate(gen):
                yield classad_to_dict(entry)
            logging.info('got %d entries', i)
        except Exception:
            logging.info('%s failed', schedd_ad['Name'], exc_info=True)
