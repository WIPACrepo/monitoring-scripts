from prometheus_client import Counter
approved_groupings = [
    'User',
    'Site'
]
class JobMetrics():
    """
    Wrapper class for holding prometheus job metrics
    """
    def __init__(self):
  
        labels = ['Owner','Site','Kind','GPUDeviceName']

        self.condor_total_job_hours =   Counter(f'condor_total_job_hours',
                                            'Total job hours',
                                            labels)
        self.condor_good_job_hours =    Counter(f'condor_good_job_hours',
                                            'Job hours with good exit status',
                                            labels)
        self.condor_bad_job_hours =     Counter(f'condor_bad_job_hours',
                                            'Jobs hours with bad exit status',
                                            labels)
        self.condor_job_count =         Counter(f'condor_job_count',
                                            'Totals job count',
                                            labels)
        self.condor_total_mem_req =     Counter(f'condor_total_mem_req',
                                            'Totals memory request',
                                            labels)
        self.condor_good_mem_req =      Counter(f'condor_good_mem_req',
                                            'Totals memory request with good exit status',
                                            labels)
        self.condor_bad_mem_req =       Counter(f'condor_bad_mem_req',
                                            'Total memory request with bad exit status',
                                            labels)
        