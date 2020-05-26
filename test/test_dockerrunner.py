# -*- coding: utf-8 -*-
import os
import unittest
from JobRunner.DockerRunner import DockerRunner
import json
from time import sleep as _sleep
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


class MockDocker(object):
    def __init__(self, reload=False, log=False):
        self.status = "stopped"
        self.id = "1234"
        self.err_reload = reload
        self.err_log = log

    def logs(self, stdout=True, stderr=False, since=None, until=None, timestamps=False):
        if self.err_log:
            raise ValueError()
        return []

    def reload(self):
        if self.err_reload:
            raise ValueError()


class DockerRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def test_run(self):
        mlog = MockLogger()
        dr = DockerRunner(logger=mlog)
        inp = {"method": "mock_app.bogus"}
        with open("/tmp/input.json", "w") as f:
            f.write(json.dumps(inp))
        vols = {"/tmp": {"bind": "/kb/module/work", "mode": "rw"}}
        of = "/tmp/output.json"
        if os.path.exists(of):
            os.remove(of)
        c = dr.run("1234", "mock_app", {}, vols, {}, [])
        _sleep(2)
        self.assertTrue(os.path.exists(of))

        self.assertGreaterEqual(len(mlog.all), 2)
        self.assertLessEqual(len(mlog.all), 4)

        dr.remove(c)

    def test_sort(self):
        dr = DockerRunner()
        sout = u"2019-07-08T23:21:32.508696500Z 1\n"
        sout += u"2019-07-08T23:21:32.508896500Z 4\n"
        serr = u"2019-07-08T23:21:32.508797700Z 3\n"
        serr += u"2019-07-08T23:21:32.508797600Z 2\n"
        lines = dr._sort_lines_by_time(sout.encode("utf-8"), serr.encode("utf-8"))
        self.assertEquals(lines[0]["line"], "1")
        self.assertEquals(lines[1]["line"], "2")
        self.assertEquals(lines[2]["line"], "3")
        self.assertEquals(lines[3]["line"], "4")
        self.assertEquals(lines[1]["is_error"], 1)

    def test_sort_empty(self):
        dr = DockerRunner()
        sout = u"2019-07-08T23:21:32.508696500Z \n"
        serr = u"2019-07-08T23:21:32.508797700Z \n"
        lines = dr._sort_lines_by_time(sout.encode("utf-8"), serr.encode("utf-8"))
        self.assertEquals(lines[0]["line"], "")
        self.assertEquals(lines[1]["line"], "")
        self.assertEquals(lines[1]["is_error"], 1)

    def test_exceptions(self):
        dr = DockerRunner()
        c = MockDocker(reload=True)
        q = Queue()
        dr._shepherd(c, "1234", [q])
        result = q.get()

        c = MockDocker(log=True)
        with self.assertRaises(ValueError):
            dr._shepherd(c, "1234", [q])
        result = q.get()
        print(result)
