from prometheus_client import Counter

class JobMetrics():
    '''
     Wrapper class for holding prometheus job metrics
    '''
 
    def __init__(self):
  
        labels = ['owner','site','schedd','GPUDeviceName','kind']

        self.condor_job_walltime_hours =   Counter(f'condor_job_walltime_hours',
                                            'Total job hours',
                                            labels)
        self.condor_job_resource_hours =   Counter(f'condor_job_resource_hours',
                                            'Total job resource kind hours',
                                            labels)
        self.condor_job_good_resource_hours =       Counter(f'condor_job_good_resource_hours',
                                            'Job hours with good exit status',
                                            labels)
        self.condor_job_bad_resource_hours =        Counter(f'condor_job_bad_resource_hours',
                                            'Jobs hours with bad exit status',
                                            labels)
        self.condor_job_good_count =       Counter(f'condor_job_good_count',
                                            'Total job count with good exit status',
                                            labels)
        self.condor_job_bad_count =        Counter(f'condor_job_bad_count',
                                            'Total job count with bad exit status',
                                            labels)
        self.condor_job_total_mem_req =        Counter(f'condor_job_total_mem_req',
                                            'Total memory request with',
                                            labels)
        self.condor_job_good_mem_req =         Counter(f'condor_job_good_mem_req',
                                            'Total memory request with good exit status',
                                            labels)
        self.condor_job_bad_mem_req =          Counter(f'condor_job_bad_mem_req',
                                            'Total memory request with bad exit status',
                                            labels)
        