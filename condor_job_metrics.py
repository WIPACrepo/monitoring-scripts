from prometheus_client import Counter
approved_groupings = [
    'User',
    'Site'
]
class JobMetrics():
    """
    
    """
    def __init__(self):
  
        labels = ['Owner','Site','Kind','GPUDeviceName']

        self.condor_total_job_hours =   Counter(f'condor_total_job_hours',
                                            'Totals of metrics by group',
                                            labels)
        self.condor_good_job_hours =    Counter(f'condor_good_job_hours',
                                            'Totals of metrics by group',
                                            labels)
        self.condor_bad_job_hours =     Counter(f'condor_bad_job_hours',
                                            'Totals of metrics by group',
                                            labels)
        self.condor_job_count =         Counter(f'condor_job_count',
                                            'Totals of metrics by group',
                                            labels)
        self.condor_total_mem_req =     Counter(f'condor_total_mem_req',
                                            'Totals of metrics by group',
                                            labels)
        self.condor_good_mem_req =      Counter(f'condor_good_mem_req',
                                            'Totals of metrics by group',
                                            labels)
        self.condor_bad_mem_req =       Counter(f'condor_bad_mem_req',
                                            'Totals of metrics by group',
                                            labels)
        