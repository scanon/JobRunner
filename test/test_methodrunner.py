# -*- coding: utf-8 -*-
import os
import unittest
from unittest.mock import patch
from copy import deepcopy
from queue import Queue

from JobRunner.MethodRunner import MethodRunner
from JobRunner.logger import Logger
from mock_data import NJS_JOB_PARAMS, CATALOG_GET_MODULE_VERSION

class MockLogger(object):
    def __init__(self):
        self.lines = []
        self.errors = []
    
    def log_lines(self, lines):
        self.lines.append(lines)

    def log(self, line):
        self.lines.append(line)
        print(line)

    def error(self, line):
        self.errors.append(line)


class MethodRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = os.environ.get('KB_AUTH_TOKEN', None)
        # WARNING: don't call any logging metholsds on the context object,
        # it'll result in a NoneType error
        cls.cfg = {
            'catalog-service-url': 'http://localhost',
            'token': cls.token,
            'admin_token': os.environ.get('KB_ADMIN_AUTH_TOKEN'),
            'workdir': '/tmp/'
        }
        cls.logger = MockLogger()
        cls.mr = MethodRunner(cls.cfg, '1234', logger=cls.logger)
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

    def test_run(self):
        mr = MethodRunner(self.cfg, '1234', logger=MockLogger())
        module_info = deepcopy(CATALOG_GET_MODULE_VERSION)
        module_info['docker_img_name'] = 'mock_app:latest'
        job_dir = '/tmp/mr/'
        if not os.path.exists(job_dir):
            os.mkdir(job_dir)
        q = Queue()
        action = mr.run(self.conf, module_info, NJS_JOB_PARAMS[0], '1234', fin_q=q)
        self.assertIn('name', action)
        out = q.get(timeout=10)
        self.assertEqual(out[0], 'finished')
        self.assertEqual('1234', out[1])
        print(self.logger.lines)

