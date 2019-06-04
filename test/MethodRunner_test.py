# -*- coding: utf-8 -*-
import os
import unittest
from unittest.mock import patch
from configparser import ConfigParser

from runner.MethodRunner import MethodRunner


class MethodRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = os.environ.get('KB_AUTH_TOKEN', None)
        # WARNING: don't call any logging metholsds on the context object,
        # it'll result in a NoneType error
        cls.cfg = {
            'catalog-service-url': 'http://localhost',
            'workdir': '/tmp/'
        }
        cls.mr = MethodRunner(cls.cfg, '1234', token=cls.token)
        base = 'https://ci.kbase.us/services/'
        cls.conf = {
            'kbase.endpoint': base,
            'workspace.srv.url': base,
            'shock.url': base,
            'handle.url': base,
            'auth-service-url': base,
            'auth-service-url-allow-insecure': True,
            'scratch': '/kb/module/work/tmp',
            'user': 'mrbogus'
        }

    JOB_PARAMS = [
        {
            u'app_id': u'mock_app/bogus',
            u'meta': {
                u'cell_id': u'c7fd3baa-69de-4858-90b9-e0332b8371ad',
                u'run_id': u'6331236f-be75-425f-8982-3565cd50242d',
                u'tag': u'beta',
                u'token_id': u'082fc6f3-3d22-48d5-8f9e-bb76ff1022bd'
                },
            u'method': u'mock_app.bogus',
            u'params': [{
                u'message': u'test',
                u'workspace_name': u'scanon:narrative_1559498772483'
                }],
            u'requested_release': None,
            u'service_ver': u'4b5a37e6fed857c199df65191ba3344a467b8aab',
            u'wsid': 42906
        },
        {
            u'auth-service-url': u'https://ci.kbase.us/services/auth/api/legacy/KBase/Sessions/Login',
            u'auth-service-url-allow-insecure': u'false',
            u'auth.service.url.v2': u'https://ci.kbase.us/services/auth/api/V2/token',
            u'awe.client.callback.networks': u'docker0,eth0',
            u'awe.client.docker.uri': u'unix:///var/run/docker.sock',
            u'catalog.srv.url': u'https://ci.kbase.us/services/catalog',
            u'condor.docker.job.timeout.seconds': u'604800',
            u'condor.job.shutdown.minutes': u'10080',
            u'docker.registry.url': u'dockerhub-prod.kbase.us',
            u'ee.server.version': u'0.2.11',
            u'handle.url': u'https://ci.kbase.us/services/handle_service',
            u'jobstatus.srv.url': u'https://ci.kbase.us/services/userandjobstate',
            u'kbase.endpoint': u'https://ci.kbase.us/services',
            u'ref.data.base': u'/kb/data',
            u'self.external.url': u'https://ci.kbase.us/services/njs_wrapper',
            u'shock.url': u'https://ci.kbase.us/services/shock-api',
            u'srv.wiz.url': u'https://ci.kbase.us/services/service_wizard',
            u'time.before.expiration': u'10',
            u'workspace.srv.url': u'https://ci.kbase.us/services/ws'
        }]

    @patch('runner.MethodRunner.Catalog', autospec=True)
    def test_run(self, mock_cat):
        params = {'message': 'Hi', 'workspace_name': 'bogus'}
        mr = MethodRunner(self.cfg, '1234', token=self.token)
        mr.catalog.get_module_version.return_value = {'docker_img_name': 'mock_app:latest'}
        job_dir = '/tmp/mr/'
        if not os.path.exists(job_dir):
            os.mkdir(job_dir)
        res = mr.run(self.conf, MethodRunnerTest.JOB_PARAMS[0], job_dir, '1234')
        self.assertIsNotNone(res)
        mr.cleanup(self.conf, '1234')
