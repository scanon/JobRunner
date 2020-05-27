import json
import logging
import os
from configparser import ConfigParser
from datetime import datetime, timezone

from .DockerRunner import DockerRunner
from .ShifterRunner import ShifterRunner

logging.basicConfig(level=logging.INFO)


# Write out config file with all kbase endpoints / secure params


class MethodRunner:
    """
    This class marshalls request for jobs and launches the containers.

    It handles preparing the run area, monitoring the container execution,
    and returning output via a queue.
    """

    def __init__(self, config, job_id, logger=None, debug=False):
        """
        Inputs: config dictionary, Job ID, and optional logger
        """
        self.config = config
        self.job_id = job_id
        self.logger = logger
        self.token = config["token"]
        self.cgroup = config.get("cgroup")
        self.workdir = config.get("workdir", "/mnt/awe/condor")
        # self.basedir = os.path.join(self.workdir, 'job_%s' % (self.job_id))

        self.job_dir = os.path.join(self.workdir, "workdir")
        self.hostname = config.get("hostname")
        self.ee2_endpoint = config.get("ee2_url")
        self.debug = debug

        logging.info(f"Job dir is {self.job_dir}")
        runtime = config.get("runtime", "docker")
        self.containers = []
        if runtime == "shifter":
            self.runner = ShifterRunner(logger=logger)
        elif runtime == "docker":
            self.runner = DockerRunner(logger=logger, debug=self.debug)
        else:
            raise OSError("Unknown runtime")

    def _init_workdir(self, config, job_dir, params):
        # Create all the directories
        # if not os.path.exists(self.basedir):
        #     os.mkdir(self.basedir)
        # logging.info("Config is")
        # logging.info(config)

        if not os.path.exists(self.job_dir):
            os.mkdir(self.job_dir)
        self.subjobdir = os.path.join(self.workdir, "subjobs")
        if not os.path.exists(self.subjobdir):
            os.mkdir(self.subjobdir)
        # Create config.properties and inputs
        conf_prop = ConfigParser()

        conf_prop["global"] = {
            "kbase_endpoint": config["kbase-endpoint"],
            "workspace_url": config["workspace-url"],
            "external_url": config["external-url"],
            "shock_url": config["shock-url"],
            "handle_url": config["handle-url"],
            "srv_wiz_url": config["srv-wiz-url"],
            "auth_service_url": config["auth-service-url"],
            "auth_service_url-v2": config["auth-service-url-v2"],
            "auth_service_url_allow_insecure": config[
                "auth-service-url-allow-insecure"
            ],
            "scratch": config["scratch"],
        }

        with open(job_dir + "/config.properties", "w") as configfile:
            conf_prop.write(configfile)

        # Create input.json
        nowutc = datetime.utcnow().replace(tzinfo=timezone.utc)
        ts = nowutc.replace(microsecond=0).isoformat()

        ctx = {
            "call_stack": [
                {"method": params["method"], "time": ts, "job_id": self.job_id}
            ],
            "service_ver": params.get("service_ver"),
        }
        input = {
            "id": self.job_id,
            "version": "1.1",
            "method": params["method"],
            "params": params["params"],
            "context": ctx,
        }
        ijson = job_dir + "/input.json"
        with open(ijson, "w") as f:
            f.write(json.dumps(input))

        # Create token file
        with open(job_dir + "/token", "w") as f:
            f.write(self.token)

        wdt = os.path.join(job_dir, "tmp")
        if not os.path.exists(wdt):
            os.mkdir(wdt)

        return True

    def _get_job_dir(self, job_id, subjob=False):
        if subjob:
            return os.path.join(self.subjobdir, job_id)
        else:
            return self.job_dir

    def run(
        self,
        config,
        module_info,
        params,
        job_id,
        fin_q=None,
        callback=None,
        subjob=False,
    ):
        """
        Run the method.  This is used for subjobs too.
        This is a blocking call.  It will not return until the
        job/process exits.
        """
        # Mkdir workdir/tmp
        cgroup = config.get("cgroup")
        job_dir = self._get_job_dir(job_id, subjob=subjob)
        if not os.path.exists(job_dir):
            os.mkdir(job_dir)
        (module, method) = params["method"].split(".")
        version = params.get("service_ver")

        image = module_info["docker_img_name"]

        if subjob:
            self.logger.log(f"Subjob method: {params['method']} JobID: {job_id} ")

        run_docker_msg = f"Running docker container for image: {image}"
        logging.info(run_docker_msg)
        self.logger.log(run_docker_msg)

        # Initialize workdir
        self._init_workdir(config, job_dir, params)

        # Run the container
        vols = {job_dir: {"bind": "/kb/module/work", "mode": "rw"}}
        if subjob:
            parent_workdir = self._get_job_dir(job_id, subjob=False)
            wdt = os.path.join(parent_workdir, "tmp")
            vols[wdt] = {"bind": "/kb/module/work/tmp", "mode": "rw"}

        if "volume_mounts" in config:
            for v in config["volume_mounts"]:
                k = v["host_dir"]
                k = k.replace("${username}", config["user"])
                if not os.path.exists(k):
                    estr = "Volume mount ({}) doesn't exist.".format(k)
                    self.logger.error(estr)
                    raise OSError("Missing volume mount")
                vols[k] = {"bind": v["container_dir"]}
                if v["read_only"] or v["read_only"] == 1:
                    vols[k]["mode"] = "ro"
        # Check to see if that image exists, and if refdata exists
        # paths to tmp dirs, refdata, volume mounts/binds
        refbase = config["ref_data_base"]
        if "data_version" in module_info:
            ref_data = os.path.join(
                refbase, module_info["data_folder"], module_info["data_version"]
            )
            vols[ref_data] = {"bind": "/data", "mode": "ro"}

        env = {"SDK_CALLBACK_URL": callback}
        # Add secure params
        if module_info.get("secure_config_params") is not None:
            for p in module_info["secure_config_params"]:
                k = "KBASE_SECURE_CONFIG_PARAM_{}".format(p["param_name"])
                env[k] = p["param_value"]

        # Set up labels used for job administration purposes

        labels = {
            "app_id": "{}/{}".format(module, method),
            "app_name": method,
            "condor_id": os.environ.get("CONDOR_ID"),
            "image_name": image,
            "image_version": image.split(".")[-1],
            "job_id": os.environ.get("JOB_ID"),
            "runner_job_id": job_id,
            "job_dir": job_dir,
            "subjob": str(subjob),
            "method_name": method,
            "parent_job_id": params.get("PARENT_JOB_ID"),
            "user_name": config.get("user"),
            "wsid": str(params.get("wsid", "")),
            "ee2_endpoint": self.ee2_endpoint,
            "worker_hostname": self.hostname,
            "vols": f"{vols}",
        }

        # If there is a fin_q then run this async
        action = {
            "name": module,
            "ver": version,
            "code_url": module_info["git_url"],
            "commit": module_info["git_commit_hash"],
        }
        # Do we need to do more for error handling?
        c = self.runner.run(job_id, image, env, vols, labels, [fin_q], cgroup=cgroup)
        logging.info(f"Container id is {c.id}")

        self.containers.append(c)
        return action

    def get_output(self, job_id, subjob=True, max_size=1024 * 1024 * 1024):
        # Attempt to read output file and see if it is well formed
        # Throw errors if not
        of = os.path.join(self._get_job_dir(job_id, subjob=subjob), "output.json")
        if os.path.exists(of):
            size = os.stat(of).st_size
            if size > max_size:
                e = {
                    "code": -32601,
                    "name": "Too much output from a method",
                    "message": "Method returned too much output "
                    + "({} > {})".format(size, max_size),
                }
                return {"error": e}

            with open(of) as json_file:
                output = json.load(json_file)
        else:
            self.logger.error(
                f"No output for {job_id} subjob={subjob} when reading {of}"
            )
            error = {
                "code": -32601,
                "name": "Output not found",
                "message": "No output generated. Check logs for more details",
                "error": "No output generated",
            }
            return {"error": error, "result": error}

        if "error" in output:
            error = output.get("error")
            error_msg = error.get("message")
            error_code = error.get("code")
            error_name = error.get("name")
            error_error = error.get("error")
            self.logger.error(
                f"Job {job_id} ran, but {of} contained an error. Error in output job msg:{error_msg} code:{error_code} name:{error_name} error:{error_error}"
            )

        return output

    def cleanup_all(self, debug=False):

        if debug is True:
            message = "Debug mode is on, will not delete containers"
            for c in self.containers:
                message += f" cid={c.id} "
            self.logger.error(line=message)
            return

        for c in self.containers:
            try:
                self.runner.remove(c)
            except OSError:
                continue
        return True
