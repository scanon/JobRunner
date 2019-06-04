

from __future__ import print_function
from clients.NarrativeJobServiceClient import NarrativeJobService

class Logger(object):

    def __init__(self, njs_url, jobid):
        self.njs_url = njs_url
        self.njs = NarrativeJobService(self.njs_url)
        self.jobid = jobid

    def log(self, lines):
        for line in lines:
            if line['is_error']:
                print("ERR: " + line['line'])
            else:
                print("OUT: " + line['line'])
            self.njs.add_job_logs(self.jobid, [{'line': line}])
