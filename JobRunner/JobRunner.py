import logging
import os
import signal
import socket
import sys
from multiprocessing import Process, Queue
from queue import Empty
from socket import gethostname
from time import sleep as _sleep
from time import time as _time

import requests

from clients.authclient import KBaseAuth
from clients.execution_engine2Client import execution_engine2 as EE2
from .CatalogCache import CatalogCache
from .MethodRunner import MethodRunner
from .SpecialRunner import SpecialRunner
from .callback_server import start_callback_server
from .logger import Logger
from .provenance import Provenance

logging.basicConfig(format="%(created)s %(levelname)s: %(message)s", level=logging.INFO)


class JobRunner(object):
    """
    This class provides the mechanisms to launch a KBase job
    on a container runtime.  It handles starting the callback service
    to support subjobs and provenenace calls.
    """

    def __init__(self, config, ee2_url, job_id, token, admin_token, debug=False):
        """
        inputs: config dictionary, EE2 URL, Job id, Token, Admin Token
        """

        self.ee2 = EE2(url=ee2_url, timeout=60)
        self.logger = Logger(ee2_url, job_id, ee2=self.ee2)
        self.token = token
        self.client_group = os.environ.get("CLIENTGROUP", "None")
        self.bypass_token = os.environ.get("BYPASS_TOKEN", True)
        self.admin_token = admin_token
        self.config = self._init_config(config, job_id, ee2_url)
        self.cgroup = self._get_cgroup()
        self.hostname = gethostname()
        self.auth = KBaseAuth(config.get("auth-service-url"))
        self.job_id = job_id
        self.workdir = config.get("workdir", "/mnt/awe/condor")
        self.jr_queue = Queue()
        self.callback_queue = Queue()
        self.prov = None
        self._init_callback_url()
        self.debug = debug
        self.mr = MethodRunner(
            self.config, job_id, logger=self.logger, debug=self.debug
        )
        self.sr = SpecialRunner(self.config, job_id, logger=self.logger)
        self.cc = CatalogCache(config)
        self.max_task = config.get("max_tasks", 20)
        self.cbs = None

        signal.signal(signal.SIGINT, self.shutdown)

    def _init_config(self, config, job_id, ee2_url):
        """
        Initialize config dictionary
        """
        config["hostname"] = gethostname()
        config["job_id"] = job_id
        config["ee2_url"] = ee2_url
        token = self.token
        config["token"] = token
        config["admin_token"] = self.admin_token
        return config

    def _check_job_status(self):
        """
        returns True if the job is still okay to run.
        """
        try:
            status = self.ee2.check_job_canceled({"job_id": self.job_id})
        except Exception as e:
            self.logger.error(
                f"Warning: Job cancel check failed due to {e}. However, the job will continue to run."
            )
            return True
        if status.get("finished", False):
            return False
        return True

    def _init_workdir(self):
        """ Check to see for existence of scratch dir: /mnt/awe/condor or /cdr/ """
        if not os.path.exists(self.workdir):
            self.logger.error("Missing workdir")
            raise OSError("Missing Working Directory")

    def _get_cgroup(self):
        """ Examine /proc/PID/cgroup to get the cgroup the runner is using """
        pid = os.getpid()
        cfile = "/proc/{}/cgroup".format(pid)
        # TODO REMOVE THIS OR FIGURE OUT FOR TESTING WHAT TO DO ABOUT THIS
        if not os.path.exists(cfile):
            raise Exception(f"Couldn't find cgroup {cfile}")
        else:
            with open(cfile) as f:
                for line in f:
                    if line.find("htcondor") > 0:
                        items = line.split(":")
                        if len(items) == 3:
                            return items[2].strip()

        raise Exception(f"Couldn't parse out cgroup from {cfile}")

    def _submit_special(self, config, job_id, job_params):
        """
        Handler for methods such as CWL, WDL and HPC
        """
        (module, method) = job_params["method"].split(".")
        self.logger.log("Submit %s as a %s:%s job" % (job_id, module, method))

        self.sr.run(
            config,
            job_params,
            job_id,
            callback=self.callback_url,
            fin_q=[self.jr_queue],
        )

    def _submit(self, config, job_id, job_params, subjob=True):
        (module, method) = job_params["method"].split(".")
        version = job_params.get("service_ver")
        module_info = self.cc.get_module_info(module, version)

        git_url = module_info["git_url"]
        git_commit = module_info["git_commit_hash"]
        if not module_info["cached"]:
            fstr = "Running module {}: url: {} commit: {}"
            self.logger.log(fstr.format(module, git_url, git_commit))
        else:
            version = module_info["version"]
            f = "WARNING: Module {} was already used once for this job. "
            f += "Using cached version: url: {} "
            f += "commit: {} version: {} release: release"
            self.logger.error(f.format(module, git_url, git_commit, version))

        vm = self.cc.get_volume_mounts(module, method, self.client_group)
        config["volume_mounts"] = vm

        action = self.mr.run(
            config,
            module_info,
            job_params,
            job_id,
            callback=self.callback_url,
            subjob=subjob,
            fin_q=self.jr_queue,
        )
        self._update_prov(action)

    def _cancel(self):
        self.mr.cleanup_all(debug=self.debug)

    def shutdown(self, sig, bt):
        print("Recieved an interrupt")
        # Send a cancel to the queue
        self.jr_queue.put(["cancel", None, None])

    def _watch(self, config):
        # Run a thread to check for expired token
        # Run a thread for 7 day max job runtime
        cont = True
        ct = 1
        exp_time = self._get_token_lifetime(config) - 600
        while cont:
            try:
                req = self.jr_queue.get(timeout=1)
                if _time() > exp_time:
                    err = "Token has expired"
                    self.logger.error(err)
                    self._cancel()
                    return {"error": err}
                if req[0] == "submit":
                    if ct > self.max_task:
                        self.logger.error("Too many subtasks")
                        self._cancel()
                        return {"error": "Canceled or unexpected error"}
                    if req[2].get("method").startswith("special."):
                        self._submit_special(
                            config=config, job_id=req[1], job_params=req[2]
                        )
                    else:
                        self._submit(config=config, job_id=req[1], job_params=req[2])
                    ct += 1
                elif req[0] == "finished_special":
                    job_id = req[1]
                    self.callback_queue.put(["output", job_id, req[2]])
                    ct -= 1
                elif req[0] == "finished":
                    subjob = True
                    job_id = req[1]
                    if job_id == self.job_id:
                        subjob = False
                    output = self.mr.get_output(job_id, subjob=subjob)
                    self.callback_queue.put(["output", job_id, output])
                    ct -= 1
                    if not subjob:
                        if ct > 0:
                            err = "Orphaned containers may be present"
                            self.logger.error(err)
                        return output
                elif req[0] == "cancel":
                    self._cancel()
                    return {}
            except Empty:
                pass
            if ct == 0:
                print("Count got to 0 without finish")
                # This shouldn't happen
                return
            # Run cancellation / finish job checker
            if not self._check_job_status():
                self.logger.error("Job canceled or unexpected error")
                self._cancel()
                _sleep(5)
                return {"error": "Canceled or unexpected error"}

    def _init_callback_url(self):
        # Find a free port and Start up callback server
        if os.environ.get("CALLBACK_IP") is not None:
            self.ip = os.environ.get("CALLBACK_IP")
            self.logger.log("Callback IP provided ({})".format(self.ip))
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("gmail.com", 80))
            self.ip = s.getsockname()[0]
            s.close()
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", 0))
        self.port = sock.getsockname()[1]
        sock.close()
        url = "http://{}:{}/".format(self.ip, self.port)
        self.logger.log("Job runner recieved Callback URL {}".format(url))
        self.callback_url = url

    def _update_prov(self, action):
        self.prov.add_subaction(action)
        self.callback_queue.put(["prov", None, self.prov.get_prov()])

    def _validate_token(self):
        # Validate token and get user name
        try:
            user = self.auth.get_user(self.config["token"])
        except Exception as e:
            self.logger.error("Token validation failed")
            raise Exception(e)

        return user

    def _get_token_lifetime(self, config):
        try:
            url = config.get("auth-service-url-v2")
            logging.info(f"About to get token lifetime from {url} for user token")
            header = {"Authorization": self.config["token"]}
            resp = requests.get(url, headers=header).json()
            return resp["expires"]
        except Exception as e:
            self.logger.error("Failed to get token lifetime")
            raise e

    def run(self):
        """
        This method starts the actual run.  This is a blocking operation and
        will not return until the job finishes or encounters and error.
        This method also handles starting up the callback server.
        """
        running_msg = f"Running job {self.job_id} ({os.environ.get('CONDOR_ID')}) on {self.hostname} ({self.ip}) in {self.workdir}"

        self.logger.log(running_msg)
        logging.info(running_msg)

        cg_msg = "Client group: {}".format(self.client_group)
        self.logger.log(cg_msg)
        logging.info(cg_msg)

        # Check to see if the job was run before or canceled already.
        # If so, log it
        logging.info("About to check job status")
        if not self._check_job_status():
            self.logger.error("Job already run or terminated")
            logging.error("Job already run or terminated")
            sys.exit(1)

        # Get job inputs from ee2 db
        # Config is not stored in job anymore, its a server wide config
        # I don't think this matters for reproducibility

        logging.info("About to get job params and config")
        try:
            job_params = self.ee2.get_job_params({"job_id": self.job_id})

        except Exception as e:
            self.logger.error("Failed to get job parameters. Exiting.")
            raise e

        try:
            config = self.ee2.list_config()
        except Exception as e:
            self.logger.error("Failed to config . Exiting.")
            raise e

        config["job_id"] = self.job_id
        self.logger.log(
            f"Server version of Execution Engine: {config.get('ee.server.version')}"
        )

        # Update job as started and log it
        logging.info("About to start job")
        try:
            self.ee2.start_job({"job_id": self.job_id})
        except Exception as e:
            self.logger.error(
                "Job already started once. Job restarts are not currently supported"
            )
            raise e

        logging.info("Initing work dir")
        self._init_workdir()
        config["workdir"] = self.workdir
        config["user"] = self._validate_token()
        config["cgroup"] = self.cgroup

        logging.info("Setting provenance")
        self.prov = Provenance(job_params)

        # Start the callback server
        logging.info("Starting callback server")
        cb_args = [
            self.ip,
            self.port,
            self.jr_queue,
            self.callback_queue,
            self.token,
            self.bypass_token,
            self.logger,
        ]
        self.cbs = Process(target=start_callback_server, args=cb_args)
        self.cbs.start()

        # Submit the main job
        self.logger.log(f"Job is about to run {job_params.get('app_id')}")
        self._submit(
            config=config, job_id=self.job_id, job_params=job_params, subjob=False
        )
        output = self._watch(config)
        self.cbs.kill()
        self.logger.log("Job is done")

        error = output.get("error")
        if error:
            error_message = "Job output contains an error"
            self.logger.error(f"{error_message} {error}")
            self.ee2.finish_job(
                {"job_id": self.job_id, "error_message": error_message, "error": error}
            )
        else:
            self.ee2.finish_job({"job_id": self.job_id, "job_output": output})

        # TODO: Attempt to clean up any running docker containers
        #       (if something crashed, for example)
        return output

        # Run docker or shifter	and keep a record of container id and
        #  subjob container ids
        # Run a job shutdown hook
