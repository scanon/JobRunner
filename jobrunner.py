#!/usr/bin/env python

import sys
import os
from socket import gethostname
from utils.logger import Logger
from clients.NarrativeJobServiceClient import NarrativeJobService as NJS
from clients.authclient import KBaseAuth
from runner.MethodRunner import MethodRunner, ServerError
from utils.callback_server2 import start_callback_server, stop_callback_server
import json
from time import sleep

class JobRunner(object):
    def __init__(self, config, njs_url, job_id):
        self.config = config
        self.hostname = gethostname()
        self.njs_url = njs_url
        self.njs = NJS(url=njs_url)
        self.auth = KBaseAuth(config.get('auth-service-url'))
        self.job_id = job_id
        self.host = "todo"
        self.token = self._get_token()
        self.workdir = config.get('workdir', '/mnt/awe/condor')
        self.logger = Logger(self.njs_url, self.job_id)
        self.cgroup = self._get_cgroup()
        self.admintoken = None
        if 'KB_ADMIN_AUTH_TOKEN' in os.environ:
            self.admintoken = os.environ['KB_ADMIN_AUTH_TOKEN']
        self.mr = MethodRunner(self.config, job_id, token = self.token,
                                admintoken=self.admintoken)
    
    _QUEUED = 1

    def _get_token(self):
    # Get the token from the environment or a file.
    # Set the KB_AUTH_TOKEN if not set.
        if 'KB_AUTH_TOKEN' in os.environ:
            token = os.environ['KB_AUTH_TOKEN']
        else:
            try:
                with open('token') as f:
                    token = f.read().rstrip()
                os.environ['KB_AUTH_TOKEN'] = token
            except:
                print("Failed to get token.")
                sys.exit(2)
        return token

    def _check_job_status(self):
        """
        returns True if the job is still okay to run.
        """
        try:
            status = self.njs.check_job_canceled({'job_id': self.job_id})
        except:
            print("Warning: Job cancel check failed.  Continuing")
            return True
        print(status)
        if status.get('finished', False):
            return False
        return True

    def _init_workdir(self):
        # Check to see for existence of /mnt/awe/condor
        if not os.path.exists(self.workdir):
            print("Missing workdir")
            raise OSError("Missing Working Directory")

    def _get_cgroup(self):
        pid = os.getpid()
        print("Looking up cgroup for %d" % (pid))
        cfile = "/proc/%d/cgroup" % (pid)
        if not os.path.exists(cfile):
            return None
        with open(cfile) as f:
            for lines in f:
                if line.find('htcondor') > 0:
                    items = line.split(':')
                    if len(items) == 3:
                        return items[2]
        
        # TODO: Log cgroup
        return cgroup

    def run(self):
        # Check to see if the job was run before or canceled already. If so, log it
        if not self._check_job_status():
            print("Job already run or canceled")
            sys.exit(1)



        # Get job inputs from njs db
        # TODO: Handle errors
        job_params = self.njs.get_job_params(self.job_id)
        params = job_params[0]
        config = job_params[1]

        # Validate token
        try:
            user = self.auth.get_user(self.token)
        except:
            print("Auth failed")
            raise Exception()

        config['user'] = user

        # Update job as started and log it
        self.njs.update_job({'job_id': self.job_id, 'is_started': True})

        self._init_workdir()
        config['workdir'] = self.workdir

        # TODO: Log the worker node / client group
        callback = start_callback_server(config, self.mr)
        print("Callback URL %s" % (callback))
        try:
            output = self.mr.run(config, params, self.job_id,
                             callback=callback, logger=self.logger)
        except ServerError as e:
            print("Application failed")
            print(e)
            return

        # TODO push results
        self.mr.cleanup(config, self.job_id)
        stop_callback_server()
        return output
        
        # Run cancellation / finish job checker
        # Run docker or shifter	and keep a record of container id and subjob container ids
        # Run a thread to check for expired token
        # Run a thread for 7 day max job runtime
        # Run a job shutdown hook
        # Check to see if job completes and returns too much data
        # Attempt to read output file and see if it is well formed
        # Throw errors if not
        # Remove shutdown hook and running threads
        # Attempt to clean up any running docker containers (if something crashed, for example)



def main(): 
    # Input job id and njs_service URL
    if len(sys.argv) == 3:
        job_id = sys.argv[1]
        njs_url = sys.argv[2]
    else:
        print("Incorrect usage")
        sys.exit(1)
    config = {}
    jr = JobRunner(config, njs_url, job_id)
    try:
        jr.run()
    except:
        print("An unhandled error was encountered")
        sys.exit(2)

if __name__ == '__main__':
    main()
