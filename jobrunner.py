#!/usr/bin/env python

import sys
import os
from socket import gethostname
from JobRunner.logger import Logger
from clients.NarrativeJobServiceClient import NarrativeJobService as NJS
from clients.authclient import KBaseAuth
from JobRunner.MethodRunner import MethodRunner, JobError
from JobRunner.callback_server import start_callback_server
import json
from time import sleep
from threading import Thread
from multiprocessing import process, Process, Queue
from queue import Empty
import socket

_TOKEN_ENV = "KB_AUTH_TOKEN"
_ADMIN_TOKEN_ENV = "KB_ADMIN_AUTH_TOKEN"

class JobRunner(object):
    def __init__(self, config, njs_url, job_id):
        self.config = self._init_config(config, job_id, njs_url)
        self.hostname = gethostname()
        self.auth = KBaseAuth(config.get('auth-service-url'))
        self.njs = NJS(url=njs_url)
        self.job_id = job_id
        self.workdir = config.get('workdir', '/mnt/awe/condor')
        self.logger = Logger(njs_url, self.job_id)
        self.run_queue = Queue()
        self.fin_queue = Queue()
        self._init_callback_url()
        self.mr = MethodRunner(self.config, job_id)
    
    _QUEUED = 1


    def _init_config(self, config, job_id, njs_url):
        """
        Initialize config dictionary
        """
        config['hostname'] = gethostname()
        config['job_id'] = job_id
        config['njs_url'] = njs_url
        config['cgroup'] = self._get_cgroup()
        token = self._get_token()
        config['token'] = token
        if _ADMIN_TOKEN_ENV not in os.environ:
            raise OSError("Missing admin token needed for volume mounts.")
        config['admin_token'] = os.environ.pop('KB_ADMIN_AUTH_TOKEN')
        if _ADMIN_TOKEN_ENV in os.environ:
            print("Failed to sanitize environment")
        return config

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

    def _submit(self, config, job_id, data):
        (module, method) = data['method'].split('.')
        data['method'] = '%s.%s' % (module, method[1:-7])
        print("Starting subjob %s %s" % (job_id, data['method']))
        if os.fork()==0:
            try:
                output = self.mr.run(config, data, job_id, callback=self.callback_url, subjob=True,
                            logger=self.logger, return_output=True)
                self.fin_queue.put([job_id, output])
                

            except Exception as e:
                print("Subjob %s failed" %(job_id))
                print(e)
            sys.exit()


    def watch(self, config):
        # Run a thread to check for expired token
        # Run a thread for 7 day max job runtime
        cont = True
        while cont:
            try:
                req=self.run_queue.get(timeout=1)
            except Empty:
                continue
            self._submit(config, req[0], req[1])


    def _init_callback_url(self):
        # Find a free port and Start up callback server
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("gmail.com",80))
        self.ip = s.getsockname()[0]
        s.close()
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', 0))
        self.port = sock.getsockname()[1]
        sock.close()
        url = 'http://%s:%s/' % (self.ip, self.port)
        print("Callback URL %s" % (url))
        self.callback_url = url


    def _validate_token(self):
        # Validate token and get user name
        try:
            user = self.auth.get_user(self.config['token'])
        except:
            print("Token validation failed")
            raise Exception()

        return user

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
        config['job_id'] = self.job_id


        # Update job as started and log it
        self.njs.update_job({'job_id': self.job_id, 'is_started': True})

        self._init_workdir()
        config['workdir'] = self.workdir
        config['user'] = self._validate_token()

        # TODO: Log the worker node / client group
        # Start the callback server
        cb_args = [self.ip, self.port, self.run_queue, self.fin_queue]
        p = Process(target=start_callback_server, args=cb_args)
        p.start()

        # Start the watcher thread
        w = Process(target=self.watch, args=[config])
        w.start()

        try:
            output = self.mr.run(config, params, self.job_id, return_output=True,
                             callback=self.callback_url, logger=self.logger)
        except JobError as e:
            print("Application failed")
            print(e)
            return

        # Remove shutdown hook and running threads
        p.kill()
        w.kill()
        self.mr.cleanup(config, self.job_id)
        # output = self.mr.get_output(job_id, subjob=False)
        # stop_callback_server()
        return output
        
        # Run cancellation / finish job checker
        # Run docker or shifter	and keep a record of container id and subjob container ids
        # Run a job shutdown hook
        # Check to see if job completes and returns too much data
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
