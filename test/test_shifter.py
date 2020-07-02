# -*- coding: utf-8 -*-
import unittest

from JobRunner.ShifterRunner import ShifterRunner
from queue import Queue


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


class ShifterRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = MockLogger()
        cls.sr = ShifterRunner(logger=cls.logger)

    def test_get_image(self):
        self.sr.get_image("mock_app:latest")

    def test_run(self):
        env = {"FOO": "BAR"}
        vols = {"/tmp": {"bind": "/kb/module/work", "mode": "rw"}}
        labels = {}
        q = Queue()
        app = "mock_app:latest"
        self.sr.run(app, app, env, vols, labels, [q])
        result = q.get()
        self.assertEquals(result[0], "finished")
        self.assertEquals(len(result), 3)
        self.assertIn("line", self.logger.all[0])
