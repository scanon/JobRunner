# -*- coding: utf-8 -*-
import os
import unittest

from .mock_data import CATALOG_LIST_VOLUME_MOUNTS
from JobRunner.SpecialRunner import SpecialRunner
from queue import Queue
import json


class MockLogger(object):
    def __init__(self):
        self.reset()

    def reset(self):
        self.lines = []
        self.errors = []
        self.all = []
        self.ct = 0

    def log_lines(self, lines):
        self.ct += 1
        self.all.extend(lines)

    def log(self, line):
        self.lines.append(line)
        self.all.append([line, 0])

    def error(self, line):
        self.errors.append(line)
        self.all.append([line, 1])


class SpecialRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = MockLogger()
        cls.config = {"token": "bogus", "workdir": "/tmp/"}
        cls.sr = SpecialRunner(cls.config, "123", logger=cls.logger)

    _wdl = """
    task hello_world {
        String name = "World"
        command {
            echo $VOL_MOUNT_0
            echo 'Hello, ${name}'
        }
        output {
            Array[String] out = read_lines(stdout())
        }
        #runtime {
        #    docker: 'ubuntu:latest'
        #}
    }

    workflow hello {
        call hello_world
    }
    """

    _wdl_inputs = """
    {
        "name": "KBase"
    }
    """

    def test_run(self):
        self.logger.reset()
        config = {}
        data = {"method": "special.slurm", "params": [{"submit_script": "submit.sl"}]}
        job_id = "1234"
        if not os.path.exists("/tmp/workdir/tmp"):
            os.makedirs("/tmp/workdir/tmp/")
        with open("/tmp/workdir/tmp/submit.sl", "w") as f:
            f.write("#!/bin/sh")
            f.write("echo Hello")
        q = Queue()
        self.sr._FILE_POLL = 0.1
        self.sr._BATCH_POLL = 0.3
        self.sr.run(config, data, job_id, fin_q=[q])
        result = q.get(timeout=10)
        self.assertEquals(result[0], "finished_special")
        self.assertEquals(len(result), 3)
        self.assertIn(["line1\n", 0], self.logger.all)
        self.assertIn(["line2\n", 1], self.logger.all)
        self.assertIn(["line3\n", 0], self.logger.all)

    def test_run_wdl(self):
        self.logger.reset()
        config = {}
        data = {
            "method": "special.wdl",
            "params": [{"workflow": "workflow.wdl", "inputs": "inputs.json"}],
        }
        job_id = "1234"
        vm = [{
            'host_dir': '/tmp/data',
            'container_dir': '/data',
            'read_only': 1
            }]

        if not os.path.exists("/tmp/workdir/tmp"):
            os.makedirs("/tmp/workdir/tmp/")
        with open("/tmp/workdir/tmp/workflow.wdl", "w") as f:
            f.write(self._wdl)
        with open("/tmp/workdir/tmp/inputs.json", "w") as f:
            f.write(self._wdl_inputs)
        q = Queue()
        self.sr._FILE_POLL = 0.1
        self.sr._BATCH_POLL = 0.3
        self.sr.run(config, data, job_id, volumes=vm, fin_q=[q])
        result = q.get(timeout=60)
        self.assertEquals(result[0], "finished_special")
        self.assertEquals(len(result), 3)
        output = self.logger.all
        count = False
        for line in output:
            if line["line"].find("workflow finished with status 'Succeeded'") > 0:
                count += 1
        self.assertTrue(count)
        self.assertLess(self.logger.ct, 10)
        with open('/tmp/workdir/tmp/meta.json') as f:
            data = json.loads(f.read())['outputs']['hello.hello_world.out']
        self.assertEquals(data[0], '/tmp/data:/data:ro')
        self.assertEquals(data[1], 'Hello, World')
