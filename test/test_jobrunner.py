# -*- coding: utf-8 -*-
import os
import unittest
from unittest.mock import patch
from mock import MagicMock
from configparser import ConfigParser

from jobrunner import JobRunner
from nose.plugins.attrib import attr
from copy import deepcopy


class JobRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = os.environ.get('KB_AUTH_TOKEN', None)
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

    CAT_RESP = {
        "registration_id": "1553870236585_bab74ed6-4699-47b5-b865-d7130b90f542", 
        "release_timestamp": 1554153143124, 
        "data_folder": "rast_sdk", 
        "released": 1, 
        "local_functions": [], 
        "timestamp": 1553870236585, 
        "notes": "", 
        "docker_img_name": "dockerhub-prod.kbase.us/kbase:rast_sdk.50b012d9b41b71ba31b30355627cf85f2611bc3e", 
        "git_commit_hash": "50b012d9b41b71ba31b30355627cf85f2611bc3e", 
        "data_version": "0.2", 
        "version": "0.1.1", 
        "narrative_methods": [
            "reannotate_microbial_genomes", 
            "annotate_contigset", 
            "annotate_plant_transcripts", 
            "annotate_contigsets", 
            "reannotate_microbial_genome"
        ], 
        "git_url": "https://github.com/kbaseapps/RAST_SDK", 
        "released_timestamp": None, 
        "release_tags": [
            "release", 
            "beta"
        ], 
        "module_name": "RAST_SDK", 
        "dynamic_service": 0, 
        "git_commit_message": "Merge pull request #57 from landml/master\n\nAdd the GenomeSet to the objects created"
    }

    JOB_PARAMS = [
    {
        u'app_id': u'echo_test/echo_test',
        u'meta': {
            u'cell_id': u'c7fd3baa-69de-4858-90b9-e0332b8371ad',
            u'run_id': u'6331236f-be75-425f-8982-3565cd50242d',
            u'tag': u'beta',
            u'token_id': u'082fc6f3-3d22-48d5-8f9e-bb76ff1022bd'
            },
        u'method': u'echo_test.echo',
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

    def _cleanup(self, job):
        d = os.path.join(self.workdir, job)
        if os.path.exists(d):
            for fn in ['config.properties', 'input.json', 'output.json', 'token']:
                if os.path.exists(os.path.join(d, fn)):
                    os.unlink(os.path.join(d, fn))
            os.rmdir(d)

    @attr('offline')
    @patch('jobrunner.KBaseAuth', autospec=True)
    @patch('jobrunner.NJS', autospec=True)
    def test_run(self, mock_njs, mock_auth):
        self._cleanup(self.jobid)
        params = deepcopy(JobRunnerTest.JOB_PARAMS)
        params[0]['method'] = 'mock_app.bogus'
        params[0]['params'] = {'param1': 'value1'}
        jr = JobRunner(self.config, self.njs_url, self.jobid)
        rv = JobRunnerTest.CAT_RESP
        rv['docker_img_name'] = 'mock_app:latest'
        jr.mr.catalog.get_module_version = MagicMock(return_value=rv)
        jr.mr.catadmin.list_volume_mounts = MagicMock(return_value=[])
        jr.logger.njs.add_job_logs = MagicMock(return_value=rv)
        jr.njs.get_job_params.return_value = params
        jr.njs.check_job_canceled.return_value = {'finished': False}
        jr.auth.get_user.return_value = "bogus"
        jr.run()

    @attr('online')
    @patch('jobrunner.NJS', autospec=True)
    #@patch('jobrunner.KBaseAuth', autospec=True)
    def test_run_online(self, mock_njs):
        self._cleanup(self.jobid)
        params = JobRunnerTest.JOB_PARAMS
        jr = JobRunner(self.config, self.njs_url, self.jobid)
        rv = JobRunnerTest.CAT_RESP
        rv['docker_img_name'] = 'mock_app:latest'
        jr.logger.njs.add_job_logs = MagicMock(return_value=rv)
        jr.njs.check_job_canceled.return_value = {'finished': False}
        jr.njs.get_job_params.return_value = params
        jr.run()

