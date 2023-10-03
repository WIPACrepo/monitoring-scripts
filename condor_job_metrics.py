from prometheus_client import Counter, Histogram

class JobMetrics():
    '''
     Wrapper class for holding prometheus job metrics
    '''
 
    def __init__(self):
  
        labels = ['owner','site','schedd','GPUDeviceName','usage','kind']
        
        memory_buckets = (1, 2, 3, 4, 6, 8, 12, 20, 30, 50, 100, float('inf'))
        resource_buckets = (1, 2, 3, 4, 6, 8, 12, 16, 32, float('inf'))

        self.condor_job_walltime_hours = Counter(f'condor_job_walltime_hours',
                                                    'Total job hours',
                                                    labels)
        self.condor_job_resource_hours = Counter(f'condor_job_resource_hours',
                                                    'Total job resource kind hours',
                                                    labels)
        self.condor_job_count =          Counter(f'condor_job_count',
                                                    'Total job count with good exit status',
                                                    labels)
        self.condor_job_mem_req =        Histogram(f'condor_job_mem_req',
                                                    'Total memory request with good exit status',
                                                    labels,
                                                    buckets=memory_buckets)
        self.condor_job_mem_used =       Histogram(f'condor_job_mem_used',
                                                    'Total memory request with good exit status',
                                                    labels,
                                                    buckets=memory_buckets)
        self.condor_job_resource_req =   Histogram(f'condor_job_resource_req',
                                                    'Total memory request with bad exit status',
                                                    labels,
                                                    buckets=resource_buckets)