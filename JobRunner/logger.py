

from __future__ import print_function
from clients.NarrativeJobServiceClient import NarrativeJobService

class Logger(object):

    def __init__(self, njs_url, job_id):
        self.njs_url = njs_url
        self.njs = NarrativeJobService(self.njs_url)
        self.job_id = job_id
        print("Logger initialized for %s" % (job_id))

    def log(self, lines):
        for line in lines:
            if line['is_error']:
                print("ERR: " + line['line'])
            else:
                print("OUT: " + line['line'])
            self.njs.add_job_logs(self.job_id, [{'line': line}])
