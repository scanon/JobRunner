import docker
import os
import json
from threading import Thread
from time import time as _time
from time import sleep as _sleep
import sys

class DockerRunner:
    """
    This class provides the container interface for Docker.

    """

    def __init__(self, logger=None):
        """
        Inputs: config dictionary, Job ID, and optional logger
        """
        self.docker = docker.from_env()
        self.logger = logger
        self.containers = []
        self.threads = []

    def _sort_logs(self, sout, serr):
        """
        This is an internal function to sort and interlace output for NJS.
        This is not fully implemented yet and sould be rethought.
        """
        # TODO: Fix sorting

        lines = []
        if len(sout) > 0:
            for line in sout.decode("utf-8").split('\n'):
                if len(line) > 0:
                    lines.append({'line': line, 'is_error': 0})
        if len(serr) > 0:
            for line in serr.decode("utf-8").split('\n'):
                if len(line) > 0:
                    lines.append({'line': line, 'is_error': 1})
        return lines

    def _shepherd(self, c, job_id, subjob, queues):
        last = 1
        try:
            while c.status in ['created', 'running']:
                c.reload()
                now = int(_time())
                sout = c.logs(stdout=True, stderr=False, since=last, until=now, timestamps=True)
                serr = c.logs(stdout=False, stderr=True, since=last, until=now, timestamps=True)
                lines = self._sort_logs(sout, serr)
                if self.logger is not None:
                    self.logger.log_lines(lines)
                last=now
                _sleep(1)
            c.remove()
            self.containers.remove(c)
            for q in queues:
                q.put(['finished', job_id, None])
        except:
            self.logger.error("Unexpected failure")

    def get_image(self, image):
        # Pull the image from the hub if we don't have it
        pulled = False
        for im in self.docker.images.list():
            if image in im.tags:
                id = im.id
                pulled = True
                break

        if not pulled:
            self.logger.log("Pulling image {}".format(image))
            id = self.docker.images.pull(image).id
        return id


    def run(self, job_id, image, env, vols, labels, subjob, queues):
        c = self.docker.containers.run(image, 'async',
                                   environment=env,
                                   detach=True,
                                   labels=labels,
                                   volumes=vols)
        self.containers.append(c)
        # Start a thread to monitor output and handle finished containers
        # 
        t=Thread(target=self._shepherd, args=[c, job_id, subjob, queues])
        self.threads.append(t)
        t.start()
        return c


    def remove(self, c):
        try:
            c.kill()
        except:
            pass

        try:
            c.remove
        except:
            pass

    # def cleanup_all(self):
    #     for c in self.containers:
    #         try:
    #             c.kill()
    #         except:
    #             continue
    #     _sleep(1)
    #     for c in self.containers:
    #         try:
    #             c.remove
    #         except:
    #             continue
    #     return True
