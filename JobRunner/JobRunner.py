import sys
import os
from .logger import Logger
from clients.NarrativeJobServiceClient import NarrativeJobService as NJS
from clients.authclient import KBaseAuth
from .MethodRunner import MethodRunner
from .callback_server import start_callback_server
import json
from socket import gethostname
from threading import Thread
from multiprocessing import process, Process, Queue
from .provenance import Provenance
from queue import Empty
import socket
import signal
from .CatalogCache import CatalogCache

class JobRunner(object):
    """
    This class provides the mechanisms to launch a KBase job
    on a container runtime.  It handles starting the callback service
    to support subjobs and provenenace calls.
    """

    def __init__(self, config, njs_url, job_id, token, admin_token):
        """
        inputs: config dictionary, NJS URL, Job id, Token, Admin Token
        """
        self.njs = NJS(url=njs_url)
        self.logger = Logger(njs_url, job_id, njs=self.njs)
        self.token = token
        self.client_group = "TODO"
        self.admin_token = admin_token
        self.config = self._init_config(config, job_id, njs_url)
        self.hostname = gethostname()
        self.auth = KBaseAuth(config.get('auth-service-url'))
        self.job_id = job_id
        self.workdir = config.get('workdir', '/mnt/awe/condor')
        self.jr_queue = Queue()
        self.callback_queue = Queue()
        self.prov = None
        signal.signal(signal.SIGINT, self.shutdown)
        print("Added signal handler")
        self._init_callback_url()
        self.mr = MethodRunner(self.config, job_id, logger=self.logger)
        self.cc = CatalogCache(config, logger=self.logger)
        print("Here")
    
    def _init_config(self, config, job_id, njs_url):
        """
        Initialize config dictionary
        """
        config['hostname'] = gethostname()
        config['job_id'] = job_id
        config['njs_url'] = njs_url
        config['cgroup'] = self._get_cgroup()
        token = self.token
        config['token'] = token
        config['admin_token'] = self.admin_token
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
                self.logger.error("Failed to get token.")
                sys.exit(2)
        return token

    def _check_job_status(self):
        """
        returns True if the job is still okay to run.
        """
        try:
            status = self.njs.check_job_canceled({'job_id': self.job_id})
        except:
            self.logger.error("Warning: Job cancel check failed.  Continuing")
            return True
        if status.get('finished', False):
            return False
        return True

    def _init_workdir(self):
        # Check to see for existence of /mnt/awe/condor
        if not os.path.exists(self.workdir):
            self.logger.error("Missing workdir")
            raise OSError("Missing Working Directory")

    def _get_cgroup(self):
        pid = os.getpid()
        cfile = "/proc/%d/cgroup" % (pid)
        if not os.path.exists(cfile):
            return None
        with open(cfile) as f:
            for line in f:
                if line.find('htcondor') > 0:
                    items = line.split(':')
                    if len(items) == 3:
                        return items[2]
        
        return "Unknown"

    def _submit(self, config, job_id, data, subjob=True):
        (module, method) = data['method'].split('.')
        version = data.get('service_ver')
        module_info = self.cc.get_module_info(module, version)
        vm = self.cc.get_volume_mounts(module, method, self.client_group)
        config['volume_mounts'] = vm
        action = self.mr.run(config, module_info, data, job_id, callback=self.callback_url, subjob=subjob,
                             fin_q=self.jr_queue)
        self._update_prov(action)        

    def _cancel(self):
        self.mr.cleanup_all()

    def shutdown(self, sig, bt):
        # TODO
        sys.exit()

    def _watch(self, config):
        # Run a thread to check for expired token
        # Run a thread for 7 day max job runtime
        cont = True
        ct = 1
        while cont:
            try:
                req=self.jr_queue.get(timeout=1)
                if req[0]=='submit':
                    print("submit " + req[1])
                    self._submit(config, req[1], req[2])
                    ct += 1
                elif req[0]=='finished':
                    subjob = True
                    job_id = req[1]
                    if job_id==self.job_id:
                        subjob=False
                    output = self.mr.get_output(job_id, subjob=subjob)
                    self.callback_queue.put(['output', job_id, output])
                    ct -= 1
                    if not subjob:
                        if ct > 0:
                            self.logger.error("Orphaned containers may be present")
                        return output
            except Empty:
                pass
            if ct == 0:
                # This shouldn't happen
                return
            # Run cancellation / finish job checker
            if not self._check_job_status():
                self.logger.error("Job canceled or unexpected error")
                self._cancel()
                return


    def _init_callback_url(self):
        # Find a free port and Start up callback server
        if os.environ.get('CALLBACK_IP') is not None:
            self.ip = os.environ.get('CALLBACK_IP')
            self.logger.log("Callback IP provided (%s)" % (self.ip))
        else:
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
        self.logger.log("Job runner recieved Callback URL %s" % (url))
        self.callback_url = url


    def _update_prov(self, action):
        self.prov.add_subaction(action)
        self.callback_queue.put(['prov', None, self.prov.get_prov()])

    def _validate_token(self):
        # Validate token and get user name
        try:
            user = self.auth.get_user(self.config['token'])
        except:
            self.logger.error("Token validation failed")
            raise Exception()

        return user

    def run(self):
        """
        This method starts the actual run.  This is a blocking operation and will
        not return until the job finishes or encounters and error.
        This method also handles starting up the callback server.
        """
        self.logger.log('Running on {} ({}) in {}'.format(self.hostname, self.ip, self.workdir))
        self.logger.log('Client group: {}'.format(self.client_group))

        # Check to see if the job was run before or canceled already. If so, log it
        if not self._check_job_status():
            self.logger.error("Job already run or canceled")
            sys.exit(1)

        # Get job inputs from njs db
        try:
            job_params = self.njs.get_job_params(self.job_id)
        except Exception as e:
            self.logger.error("Failed to get job parameters. Exiting.")
            raise e

        params = job_params[0]
        config = job_params[1]
        config['job_id'] = self.job_id

        server_version = config['ee.server.version']
        self.logger.log('Server version of Execution Engine: {}'.format(server_version))


        # Update job as started and log it
        self.njs.update_job({'job_id': self.job_id, 'is_started': True})

        self._init_workdir()
        config['workdir'] = self.workdir
        config['user'] = self._validate_token()

        self.prov = Provenance(params)

        # Start the callback server
        cb_args = [self.ip, self.port, self.jr_queue, self.callback_queue, self.token]
        cbs = Process(target=start_callback_server, args=cb_args)
        cbs.start()

        # Submit the main job
        self._submit(config, self.job_id, params, subjob=False)

        output = self._watch(config)
        # TODO: Check to see if job completes and returns too much data
        cbs.kill()
        self.logger.log('Job is done')
        # TODO: Attempt to clean up any running docker containers (if something crashed, for example)
        return output
        
        # Run docker or shifter	and keep a record of container id and subjob container ids
        # Run a job shutdown hook

