import re
from prometheus_client import Gauge

class JobMetrics():
    '''
    condor_job_resource_totals:
      labels: schedd, group, user, state
    condor_job_counts:
      labels: schedd, group, user, state
    '''
    
    def __init__(self):
        self.labels = ['schedd','group','owner','state']
        self.condor_jobs_walltime =             Gauge('condor_jobs_walltime',
                                                    'Total allocated CPU time from start by jobs',
                                                    self.labels)
        self.condor_jobs_cputime =              Gauge('condor_jobs_cputime',
                                                    'Total observed CPU user time from jobs',
                                                    self.labels)
        self.condor_jobs_wastetime =            Gauge('condor_jobs_wastetime',
                                                    'Total of condor job resource attributes',
                                                    self.labels)
        self.condor_jobs_cpu_request =          Gauge('condor_jobs_cpu_request',
                                                    'Total of condor job resource attributes',
                                                    self.labels)
        self.condor_jobs_memory_request_bytes = Gauge('condor_jobs_memory_request_bytes',
                                                    'Total of condor job resource attributes',
                                                    self.labels)
        self.condor_jobs_memory_usage_bytes =   Gauge('condor_jobs_memory_usage_bytes',
                                                    'Total of condor job resource attributes',
                                                    self.labels)
        self.condor_jobs_disk_request_bytes =   Gauge('condor_jobs_disk_request_bytes',
                                                    'Total of condor job resource attributes',
                                                    self.labels)
        self.condor_jobs_disk_usage_bytes =     Gauge('condor_jobs_disk_usage_bytes',
                                                    'Total of condor job resource attributes',
                                                    self.labels)
        self.condor_jobs_gpu_request =          Gauge('condor_jobs_gpu_request',
                                                    'Total of condor job resource attributes',
                                                    self.labels)
        self.condor_jobs_count =                Gauge('condor_jobs_count',
                                                    'Count of condor jobs resource attributes',
                                                    self.labels + ['exit_code'])
    def clear(self):
      for key in self.__dict__.keys():
        if isinstance(self.__dict__[key],Gauge):
          self.__dict__[key].clear()

class SlotMetrics():
    def __init__(self):
        self.dynamic_slots_totals = Gauge('condor_pool_dynamic_slots_totals',
                                        'Condor Pool Dynamic Slot Resources',
                                        ['resource'])
        self.dynamic_slots_usage = Gauge('condor_pool_dynamic_slots_usage',
                                'Condor Pool Dynamic Slot Resources',
                                ['group','user','resource',])
        self.partitionable_slots_totals = Gauge('condor_pool_partitionable_slots_totals',
                                            'Condor Pool Partitionable Slot Resources',
                                            ['resource'])
        self.partitionable_slots_host_totals = Gauge('condor_pool_partitionable_slots_host_totals',
                                            'Condor Pool Partitionable Slot Host Resources',
                                            ['host','resource'])
        self.partitionable_slots_unusable = Gauge('condor_pool_partitionable_slots_unusable',
                                            'Condor Pool Partitionable Slot Unusable Resources',
                                            ['resource'])
    def clear(self):
        for key in self.__dict__.keys():
            if isinstance(self.__dict__[key],Gauge):
              self.__dict__[key].clear()