import os
from threading import Thread
from subprocess import Popen, PIPE
from time import sleep, time
from select import select


class SpecialRunner:
    """
    This class provides the interface to run special job types like
    CWL, WDL and HPC.

    """

    def __init__(self, config, job_id, logger=None):
        """
        Inputs: config dictionary, Job ID, and optional logger
        """
        self.config = config
        self.top_job_id = job_id
        self.logger = logger
        self.token = config["token"]
        self.workdir = config.get("workdir", "/mnt/awe/condor")
        self.shareddir = os.path.join(self.workdir, "workdir/tmp")
        self.containers = []
        self.threads = []
        self.allowed_types = ["slurm", "wdl"]
        self.WDL_RUN = 'wdl_run'

    _BATCH_POLL = 10
    _FILE_POLL = 10
    _MAX_RETRY = 5

    def run(self, config, data, job_id, callback=None, volumes=None, fin_q=[]):
        # TODO:
        # initialize working space
        # check job type against an allow list
        # submit the job and map the batch jobb to the job id
        # start a thread to monitor progress
        (module, method) = data["method"].split(".")

        if module != "special":
            err = "Attempting to run the wrong type of module. "
            err += "The module should be 'special'"
            raise ValueError(err)

        if method not in self.allowed_types:
            raise ValueError("Invalid special method type")

        if method == "slurm":
            return self._batch_submit(method, config, data, job_id, fin_q)
        elif method == "wdl":
            return self._wdl_run(method, config, data, job_id, volumes, fin_q)

    def _check_batch_job(self, check, slurm_jobid):
        cmd = [check, slurm_jobid]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        return stdout.decode("utf-8").rstrip()

    def _watch_batch(self, stype, job_id, slurm_jobid, outfile, errfile, queues):
        self.logger.log("Watching Slurm Job ID %s" % (slurm_jobid))
        check = "%s_checkjob" % (stype)
        cont = True
        retry = 0
        # Wait for job to start out output file to appear
        while cont:
            state = self._check_batch_job(check, slurm_jobid)
            if state == "Running":
                self.logger.log("Running")
            elif state == "Pending":
                self.logger.log("Pending")
            elif state == "Finished":
                cont = False
                self.logger.log("Finished")
            else:
                if retry > self._MAX_RETRY:
                    cont = False
                retry += 1
                self.logger.log("Unknown")

            if os.path.exists(outfile):
                cont = False

            sleep(self._BATCH_POLL)

        # Tail output
        rlist = []
        stdout = None
        stderr = None
        if os.path.exists(outfile):
            stdout = open(outfile)
            rlist.append(stdout)
        else:
            self.logger.error("No output file generated")
        if os.path.exists(errfile):
            stderr = open(errfile)
            rlist.append(stderr)
        else:
            self.logger.error("No error file generated")

        cont = True
        if len(rlist) == 0:
            cont = False
        next_check = 0
        while cont:
            if time() > next_check:
                state = self._check_batch_job(check, slurm_jobid)
                next_check = time() + self._BATCH_POLL
                if state != "Running":
                    cont = False
            r, w, e = select(rlist, [], [], 10)
            for f in r:
                for line in f:
                    if f == stdout and self.logger:
                        self.logger.log(line)
                    elif f == stderr and self.logger:
                        self.logger.error(line)
            sleep(self._FILE_POLL)
        # TODO: Extract real exit code
        resp = {"exit_status": 0, "output_file": outfile, "error_file": errfile}
        result = {"result": [resp]}
        for q in queues:
            q.put(["finished_special", job_id, result])

    def _batch_submit(self, stype, config, data, job_id, fin_q):
        """
        This subbmits the job to the batch system and starts
        a thread to monitor the progress.

        The assumptions are there is a submit script and the
        batch system will return a job id and log output to
        a specified file.
        """
        params = data["params"][0]
        submit = "%s_submit" % (stype)
        if "submit_script" not in params:
            raise ValueError("Missing submit script")
        os.chdir(self.shareddir)
        scr = params["submit_script"]
        if not os.path.exists(scr):
            raise OSError("Submit script not found at %s" % (scr))
        outfile = "%s.out" % (job_id)
        errfile = "%s.err" % (job_id)
        cmd = [submit, scr, outfile, errfile]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        slurm_jobid = stdout.decode("utf-8").rstrip()
        out = Thread(
            target=self._watch_batch,
            args=[stype, job_id, slurm_jobid, outfile, errfile, fin_q],
        )
        self.threads.append(out)
        out.start()
        self.containers.append(proc)
        return proc

    def _readio(self, p, job_id, queues):
        cont = True
        last = False
        while cont:
            rlist = [p.stdout, p.stderr]
            x = select(rlist, [], [], 1)[0]
            for f in x:
                if f == p.stderr:
                    error = 1
                else:
                    error = 0
                lines = []
                for line in f.read().decode("utf-8").split("\n"):
                    lines.append({"line": line, "is_error": error})
                if len(lines) > 0:
                    self.logger.log_lines(lines)
            if last:
                cont = False
            if p.poll() is not None:
                last = True
        resp = {"exit_status": p.returncode, "output_file": None, "error_file": None}
        result = {"result": [resp]}
        p.wait()
        for q in queues:
            q.put(["finished_special", job_id, result])

    def _wdl_run(self, stype, config, data, job_id, volumes, queues):
        """
        This subbmits the job to the batch system and starts
        a thread to monitor the progress.

        """
        params = data["params"][0]
        if volumes:
            index = 0
            for vol in volumes:
                index = 0
                key = 'VOL_MOUNT_{}'.format(index)
                if vol['read_only']:
                    vol['flag'] = ':ro'
                value = '{host_dir}:{container_dir}{flag}'.format(**vol)
                os.environ[key] = value
        if "workflow" not in params:
            raise ValueError("Missing workflow script")
        if "inputs" not in params:
            raise ValueError("Missing inputs")
        os.chdir(self.shareddir)
        wdl = params["workflow"]
        if not os.path.exists(wdl):
            raise OSError("Workflow script not found at %s" % (wdl))

        inputs = params["inputs"]
        if not os.path.exists(inputs):
            raise OSError("Inputs file not found at %s" % (inputs))
        cmd = [self.WDL_RUN, inputs, wdl]
        proc = Popen(cmd, bufsize=0, stdout=PIPE, stderr=PIPE)
        out = Thread(target=self._readio, args=[proc, job_id, queues])
        self.threads.append(out)
        out.start()
        self.containers.append(proc)
        return proc
