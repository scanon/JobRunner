

import sys
from clients.NarrativeJobServiceClient import NarrativeJobService


class Logger(object):

    def __init__(self, njs_url, job_id, njs=None):
        self.njs_url = njs_url
        if njs is None:
            self.njs = NarrativeJobService(self.njs_url)
        else:
            self.njs = njs
        self.job_id = job_id
        print("Logger initialized for %s" % (job_id))

    def log_lines(self, lines):
        for line in lines:
            print(line['line'], flush=True)
            # if line['is_error']:
            #     sys.stderr.write(line+'\n')
            # else:
            #     print(line['line'])
        self.njs.add_job_logs(self.job_id, lines)

    def log(self, line):
        print(line, flush=True)
        self.njs.add_job_logs(self.job_id, [{'line': line, 'is_error': 0}])

    def error(self, line):
        print(line, flush=True)
        self.njs.add_job_logs(self.job_id, [{'line': line, 'is_error': 1}])
