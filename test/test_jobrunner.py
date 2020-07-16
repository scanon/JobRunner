# -*- coding: utf-8 -*-
import os
import unittest
from copy import deepcopy
from time import time as _time
from unittest.mock import patch, MagicMock

from nose.plugins.attrib import attr
from requests import ConnectionError

from JobRunner.JobRunner import JobRunner
from .mock_data import (
    CATALOG_GET_MODULE_VERSION,
    EE2_JOB_PARAMS,
    EE2_LIST_CONFIG,
    CATALOG_LIST_VOLUME_MOUNTS,
    AUTH_V2_TOKEN,
)
from docker.errors import NotFound
from docker.models.containers import Container
from JobRunner.exceptions import CantRestartJob


class MockLogger(object):
    def __init__(self):
        self.lines = []
        self.errors = []
        self.all = []

    def log_lines(self, lines):
        self.all.extend(lines)

    def log(self, line):
        self.lines.append(line)
        self.all.append([line, 0])

    def error(self, line):
        self.errors.append(line)
        self.all.append([line, 1])


class MockAuth(object):
    def __init__(self, data):
        self.data = data

    def json(self):
        return self.data


class JobRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.token = os.environ.get("KB_AUTH_TOKEN", "")
        cls.admin_token = os.environ.get("KB_ADMIN_AUTH_TOKEN", "bogus")
        cls.cfg = {}
        base = "https://ci.kbase.us/services/"
        if "TEST_URL" in os.environ:
            base = "http://%s/services/" % (os.environ["TEST_URL"])
        cls.ee2_url = base + "ee2"
        cls.jobid = "1234"
        cls.workdir = "/tmp/jr/"
        cls.cfg["token"] = cls.token
        cls.future = _time() + 3600
        cls.config = {
            "catalog-service-url": base + "catalog",
            "auth-service-url": base + "auth/api/legacy/KBase/Sessions/Login",
            "auth2-url": base + "auth/api/V2/token",
            "workdir": "/tmp/jr",
        }
        if not os.path.exists("/tmp/jr"):
            os.mkdir("/tmp/jr")
        if "KB_ADMIN_AUTH_TOKEN" not in os.environ:
            os.environ["KB_ADMIN_AUTH_TOKEN"] = "bogus"



    def _cleanup(self, job):
        d = os.path.join(self.workdir, "workdir")
        if os.path.exists(d):
            for fn in ["config.properties", "input.json", "output.json", "token"]:
                if os.path.exists(os.path.join(d, fn)):
                    os.unlink(os.path.join(d, fn))
            try:
                os.rmdir(d)
            except:
                pass

    @attr("offline")
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_run_sub(self, mock_ee2, mock_auth):
        """
        This test is expected to run for 50-60 seconds?

        """
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "RunTester.run_RunTester"
        params["params"] = [{"depth": 3, "size": 1000, "parallel": 5}]
        config = deepcopy(EE2_LIST_CONFIG)
        config["auth-service-url"] = self.config["auth-service-url"]
        config["auth.service.url.v2"] = self.config["auth2-url"]
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "test/runtester:latest"
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr._get_cgroup = MagicMock(return_value=None)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = config
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.auth.get_user.return_value = "bogus"
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        out = jr.run()
        self.assertIn("result", out)
        self.assertNotIn("error", out)

    @attr("offline")
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_run(self, mock_ee2, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "mock_app.bogus"
        params["params"] = {"param1": "value1"}
        config = deepcopy(EE2_LIST_CONFIG)
        config["auth-service-url"] = self.config["auth-service-url"]
        config["auth.service.url.v2"] = self.config["auth2-url"]
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "mock_app:latest"
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr._get_cgroup = MagicMock(return_value=None)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = config
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.auth.get_user.return_value = "bogus"
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        out = jr.run()
        self.assertIn("result", out)
        self.assertNotIn("error", out)

    @attr("offline")
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_run_volume(self, mock_ee2, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "mock_app.voltest"
        params["params"] = {"param1": "value1"}
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "mock_app:latest"
        vols = deepcopy(CATALOG_LIST_VOLUME_MOUNTS)
        jr._get_cgroup = MagicMock(return_value=None)
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=vols)
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = EE2_LIST_CONFIG
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.auth.get_user.return_value = "bogus"
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        if not os.path.exists("/tmp/bogus"):
            os.mkdir("/tmp/bogus")
        with open("/tmp/bogus/input.fa", "w") as f:
            f.write(">contig-50_0 length_64486 read_count_327041\n")
            f.write("GTCGTGCTGCTGCCGATCGACCGCGCCTATGCGATGTTGCCGGACGGCATCC\n")
        out = jr.run()
        self.assertIn("result", out)
        self.assertNotIn("error", out)

    @attr("offline")
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_cancel(self, mock_ee2, mock_auth):
        """
        This test is expected to run for 30 seconds?

        """
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "RunTester.run_RunTester"
        params["params"] = [{"depth": 3, "size": 1000, "parallel": 4}]
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "test/runtester:latest"
        jr._get_cgroup = MagicMock(return_value='cgroup')
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = EE2_LIST_CONFIG
        nf = {"finished": False}
        jr.ee2.check_job_canceled.side_effect = [nf, nf, nf, nf, nf, {"finished": True}]
        jr.auth.get_user.return_value = "bogus"

        out = jr.run()
        self.assertIsNotNone(out)
        self.assertEqual({"error": "Canceled or unexpected error"}, out)

        # Check that all containers are gone
        for c in jr.mr.containers: # type: Container
            with self.assertRaises(expected_exception=NotFound):
                c.reload()


    @attr("offline")
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_max_jobs(self, mock_ee2, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "RunTester.run_RunTester"
        params["params"] = [{"depth": 2, "size": 1000, "parallel": 5}]
        config = deepcopy(self.config)
        config["max_tasks"] = 2
        jr = JobRunner(config, self.ee2_url, self.jobid, self.token, self.admin_token)
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "test/runtester:latest"
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr._get_cgroup = MagicMock(return_value=None)
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = EE2_LIST_CONFIG
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.auth.get_user.return_value = "bogus"
        out = jr.run()
        self.assertIn("error", out)
        # Check that all containers are gone

    @attr("online")
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_run_online(self, mock_ee2):
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "mock_app:latest"
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.ee2.get_job_params.return_value = params
        jr._get_cgroup = MagicMock(return_value=None)
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        out = jr.run()
        self.assertIn("result", out)
        self.assertNotIn("error", out)

    @patch("JobRunner.JobRunner.EE2", autospec=True)
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    def test_token(self, mock_ee2, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        os.environ["KB_AUTH_TOKEN"] = "bogus"
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.ee2.get_job_params.return_value = params
        jr.auth.get_user.side_effect = OSError()
        with self.assertRaises(Exception):
            jr.run()

    @patch("JobRunner.JobRunner.EE2", autospec=True)
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    def test_canceled_job(self, mock_ee2, mock_auth):

        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        mlog = MockLogger()
        os.environ["KB_AUTH_TOKEN"] = "bogus"
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        jr.logger = mlog
        jr.ee2.check_job_canceled.return_value = {"finished": True}
        with self.assertRaises(CantRestartJob):
            jr.run()

        self.assertEqual(mlog.errors[0], "Job already run or terminated")

    @patch("JobRunner.JobRunner.EE2", autospec=True)
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    def test_error_update(self, mock_ee2, mock_auth):
        self._cleanup(self.jobid)
        mlog = MockLogger()
        os.environ["KB_AUTH_TOKEN"] = "bogus"
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        jr.logger = mlog
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.ee2.start_job.side_effect = ConnectionError()
        jr.ee2.get_job_params.side_effect = ConnectionError()
        with self.assertRaises(ConnectionError):
            jr.run()
        emsg = "Failed to get job parameters. Exiting."
        self.assertEquals(mlog.errors[0], emsg)

    @attr("offline")
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.requests", autospec=True)
    def test_token_lifetime(self, mock_req, mock_auth, mock_ee2):
        # Test get token lifetime

        config = deepcopy(EE2_LIST_CONFIG)
        resp = AUTH_V2_TOKEN
        mock_req.get.return_value = MockAuth(resp)
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        mlog = MockLogger()
        jr.logger = mlog
        exp = jr._get_token_lifetime(config)
        self.assertGreater(exp, 0)

        mock_req.get.side_effect = OSError("bad request")
        with self.assertRaises(OSError):
            jr._get_token_lifetime(config)

    @attr("offline")
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    def test_expire_loop(self, mock_auth, mock_ee2):
        # Check that things exit when the token expires

        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "mock_app.bogus"
        params["params"] = {"param1": "value1"}
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "mock_app:latest"
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr._get_cgroup = MagicMock(return_value=None)
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = EE2_LIST_CONFIG
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.auth.get_user.return_value = "bogus"
        jr._get_token_lifetime = MagicMock(return_value=_time())
        out = jr.run()
        self.assertIn("error", out)
        self.assertEqual(out["error"], "Token has expired")

    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_special(self, mock_ee2, mock_auth):
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        jr._get_cgroup = MagicMock(return_value=None)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        params = deepcopy(EE2_JOB_PARAMS)
        submitscript = os.path.join(self.workdir, "workdir/tmp", "submit.sl")
        with open(submitscript, "w") as f:
            f.write("#!/bin/sh")
            f.write("echo hello")

        params["method"] = "special.slurm"
        params["params"] = [{"submit_script": "submit.sl"}]
        jr._submit_special(self.config, "1234", params)

    @attr("offline")
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_run_slurm(self, mock_ee2, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "RunTester.run_RunTester"
        params["params"] = [{"do_slurm": 1}]
        config = deepcopy(EE2_LIST_CONFIG)
        config["auth-service-url"] = self.config["auth-service-url"]
        config["auth.service.url.v2"] = self.config["auth2-url"]
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "test/runtester:latest"
        jr._get_cgroup = MagicMock(return_value=None)
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr.logger.ee2.add_job_logs = MagicMock(return_value=rv)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = config
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.auth.get_user.return_value = "bogus"
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        out = jr.run()
        self.assertNotIn("error", out)

    @attr("offline")
    @patch("JobRunner.JobRunner.KBaseAuth", autospec=True)
    @patch("JobRunner.JobRunner.EE2", autospec=True)
    def test_run_wdl(self, mock_ee2, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(EE2_JOB_PARAMS)
        params["method"] = "RunTester.run_RunTester"
        params["params"] = [{"do_wdl": 1}]
        config = deepcopy(EE2_LIST_CONFIG)
        config["auth-service-url"] = self.config["auth-service-url"]
        config["auth.service.url.v2"] = self.config["auth2-url"]
        jr = JobRunner(
            self.config, self.ee2_url, self.jobid, self.token, self.admin_token
        )
        mlog = MockLogger()
        jr.logger = mlog
        jr.sr.logger = mlog
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv["docker_img_name"] = "test/runtester:latest"
        jr._get_cgroup = MagicMock(return_value=None)
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catalog.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_secure_config_params = MagicMock(return_value=None)
        jr.ee2.get_job_params.return_value = params
        jr.ee2.list_config.return_value = config
        jr.ee2.check_job_canceled.return_value = {"finished": False}
        jr.auth.get_user.return_value = "bogus"
        jr._get_token_lifetime = MagicMock(return_value=self.future)
        out = jr.run()
        self.assertNotIn("error", out)
