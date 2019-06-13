# -*- coding: utf-8 -*-
import os
import unittest
from unittest.mock import patch
from mock import MagicMock

from JobRunner.JobRunner import JobRunner
from nose.plugins.attrib import attr
from copy import deepcopy
from mock_data import CATALOG_GET_MODULE_VERSION, NJS_JOB_PARAMS


class JobRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = os.environ.get('KB_AUTH_TOKEN', None)
        cls.admin_token = os.environ.get('KB_ADMIN_AUTH_TOKEN', None)
        cls.cfg = {}
        base = 'https://ci.kbase.us/services/'
        cls.njs_url = base + 'njs_wrapper'
        cls.jobid = '1234'
        cls.workdir = '/tmp/jr/'
        cls.cfg['token'] = cls.token
        cls.config = {
            'catalog-service-url': base + 'catalog',
            'auth-service-url': base + 'auth/api/legacy/KBase/Sessions/Login',
            'workdir': '/tmp/jr'
            }
        if not os.path.exists('/tmp/jr'):
            os.mkdir('/tmp/jr')
        if 'KB_ADMIN_AUTH_TOKEN' not in os.environ:
            os.environ['KB_ADMIN_AUTH_TOKEN'] = 'bogus'


    def _cleanup(self, job):
        d = os.path.join(self.workdir, job)
        if os.path.exists(d):
            for fn in ['config.properties', 'input.json', 'output.json', 'token']:
                if os.path.exists(os.path.join(d, fn)):
                    os.unlink(os.path.join(d, fn))
            os.rmdir(d)

    @attr('offline')
    @patch('JobRunner.JobRunner.KBaseAuth', autospec=True)
    @patch('JobRunner.JobRunner.NJS', autospec=True)
    def test_run_sub(self, mock_njs, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(NJS_JOB_PARAMS)
        params[0]['method'] = 'RunTester.run_RunTester'
        params[0]['params'] = [{'depth': 3, 'size': 1000, 'parallel': 5}]
        jr = JobRunner(self.config, self.njs_url, self.jobid, self.token, self.admin_token)
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv['docker_img_name'] = 'test/runtester:latest'
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catadmin.list_volume_mounts = MagicMock(return_value=[])
        jr.logger.njs.add_job_logs = MagicMock(return_value=rv)
        jr.njs.get_job_params.return_value = params
        jr.njs.check_job_canceled.return_value = {'finished': False}
        jr.auth.get_user.return_value = "bogus"
        out = jr.run()
        self.assertIn('result', out)
        self.assertNotIn('error', out)

    @attr('offline')
    @patch('JobRunner.JobRunner.KBaseAuth', autospec=True)
    @patch('JobRunner.JobRunner.NJS', autospec=True)
    def test_run(self, mock_njs, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(NJS_JOB_PARAMS)
        params[0]['method'] = 'mock_app.bogus'
        params[0]['params'] = {'param1': 'value1'}
        jr = JobRunner(self.config, self.njs_url, self.jobid, self.token, self.admin_token)
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv['docker_img_name'] = 'mock_app:latest'
        # jr.mr.catalog.get_module_version = MagicMock(return_value=rv)
        # jr.mr.catadmin.list_volume_mounts = MagicMock(return_value=[])
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catadmin.list_volume_mounts = MagicMock(return_value=[])
        jr.logger.njs.add_job_logs = MagicMock(return_value=rv)
        jr.njs.get_job_params.return_value = params
        jr.njs.check_job_canceled.return_value = {'finished': False}
        jr.auth.get_user.return_value = "bogus"
        out = jr.run()
        self.assertIn('result', out)
        self.assertNotIn('error', out)

    @attr('offline')
    @patch('JobRunner.JobRunner.KBaseAuth', autospec=True)
    @patch('JobRunner.JobRunner.NJS', autospec=True)
    def test_cancel(self, mock_njs, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(NJS_JOB_PARAMS)
        params[0]['method'] = 'RunTester.run_RunTester'
        params[0]['params'] = [{'depth': 3, 'size': 1000, 'parallel': 5}]
        jr = JobRunner(self.config, self.njs_url, self.jobid, self.token, self.admin_token)
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv['docker_img_name'] = 'test/runtester:latest'
        jr.cc.catalog.get_module_version = MagicMock(return_value=rv)
        jr.cc.catadmin.list_volume_mounts = MagicMock(return_value=[])
        jr.logger.njs.add_job_logs = MagicMock(return_value=rv)
        jr.njs.get_job_params.return_value = params
        nf = {'finished': False}
        jr.njs.check_job_canceled.side_effect = [nf, nf, nf, nf, nf, {'finished': True}]
        jr.auth.get_user.return_value = "bogus"
        out = jr.run()
        # Check that all containers are gone


    @attr('online')
    @patch('JobRunner.JobRunner.NJS', autospec=True)
    #@patch('JobRunner.KBaseAuth', autospec=True)
    def test_run_online(self, mock_njs):
        self._cleanup(self.jobid)
        params = deepcopy(NJS_JOB_PARAMS)
        jr = JobRunner(self.config, self.njs_url, self.jobid, self.token, self.admin_token)
        rv = deepcopy(CATALOG_GET_MODULE_VERSION)
        rv['docker_img_name'] = 'mock_app:latest'
        jr.logger.njs.add_job_logs = MagicMock(return_value=rv)
        jr.njs.check_job_canceled.return_value = {'finished': False}
        jr.njs.get_job_params.return_value = params
        out = jr.run()
        self.assertIn('result', out)
        self.assertNotIn('error', out)

