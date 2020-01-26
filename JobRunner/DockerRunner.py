import atexit
import logging
from threading import Thread
from time import sleep as _sleep
from time import time as _time
from typing import List

import docker
from docker.models.containers import Container

logging.basicConfig(level=logging.INFO)


class DockerRunner:
    """
    This class provides the container interface for Docker.

    """

    def _cleanup_docker_containers(self):
        """
        At exit, attempt to clean up all docker containers.
        Suppress errors in case they don't exist
        """
        if self.debug is True:
            return

        for container in self.containers:
            self.remove(container)

    def __init__(self, logger=None, debug=False):
        """
        Inputs: config dictionary, Job ID, and optional logger
        """
        self.docker = docker.from_env()
        self.logger = logger
        self.containers = []  # type: List[Container]
        self.threads = []  # type: List[Thread]
        self.log_interval = 1
        self.debug = debug
        atexit.register(self._cleanup_docker_containers)

    def _sort_lines_by_time(self, sout, serr):
        """
        This is an internal function to sort and interlace output for NJS.
        This is not fully implemented yet and sould be rethought.
        """
        lines_by_time = dict()
        ierr = 0
        for stream in [sout, serr]:
            if len(sout) == 0:
                continue
            for line in stream.decode("utf-8").split('\n'):
                if len(line) > 0:
                    elements = line.split(maxsplit=1)
                    if len(elements) == 2:
                        (ts, txt) = elements
                    else:
                        ts = elements[0]
                        txt = ''
                    if ts not in lines_by_time:
                        lines_by_time[ts] = []
                    lines_by_time[ts].append({'line': txt, 'is_error': ierr})
            ierr += 1
        nlines = []
        for ts in sorted(lines_by_time.keys()):
            nlines.extend(lines_by_time[ts])
        return nlines

    def _shepherd_logs(self, c, now, last):
        sout = c.logs(stdout=True, stderr=False, since=last, until=now,
                      timestamps=True)
        serr = c.logs(stdout=False, stderr=True, since=last, until=now,
                      timestamps=True)
        lines = self._sort_lines_by_time(sout, serr)
        if self.logger is not None:
            self.logger.log_lines(lines)

    def _shepherd(self, c, job_id, queues):
        last = 1
        try:
            dolast = False
            while True:
                now = int(_time())
                self._shepherd_logs(c, now, last)
                last = now
                if dolast:
                    break
                _sleep(self.log_interval)
                try:
                    c.reload()
                    if c.status not in ["created", "running"]:
                        dolast = True
                except Exception:
                    dolast = True
        except Exception as e:
            if self.logger is not None:
                self.logger.error("Unexpected failure")
            else:
                print(f"Exception in docker logging for {c.id}")
                raise e
        finally:

            # Capture the last few seconds of logs
            try:
                now = int(_time())
                self._shepherd_logs(c, now, last)
            except Exception:
                pass

            try:
                if self.debug is True:
                    msg = f"Not going to delete container {c.id} because debug mode is on"
                    print(msg)
                    self.logger.log(msg)
                    logging.info(msg)
                    msg2="About to try to capture last few logs again"
                    logging.info(msg2)
                    self.logger.log(msg2)
                    # Capture the last few seconds of logs
                    try:
                        now = int(_time())
                        self._shepherd_logs(c, now, last)
                    except Exception:
                        pass
                    logging.info("Did it work?")
                    self.logger.log("Did it work?")

                else:
                    msg = f"Going to delete container {c.id}"
                    print(msg)
                    self.logger.log(msg)
                    logging.info(msg)
                    c.remove()
                    self.containers.remove(c)
            except Exception:
                # Maybe something already cleaned it up.  Move on.
                pass
            for q in queues:
                q.put(['finished', job_id, None])

    def get_image(self, image):
        """
        Retrieve an image by ID, and pull it if we don't already have it
        locally on the current worker node.
        :param image: The image name to pull from from dockerhub
        :return: ID of the pulled image
        :param image:
        :return:
        """

        # Pull the image from the hub if we don't have it
        pulled = False
        for im in self.docker.images.list():
            if image in im.tags:
                image_id = im.id
                pulled = True
                break

        if not pulled:
            self.logger.log("Pulling image {}".format(image))
            image_id = self.docker.images.pull(image).id

        return image_id

    def run(self, job_id, image, env, vols, labels, queues):
        """
        Start a docker container for the main job or subjobs
        and append it to the list of docker containers
        :param job_id: The ExecutionEngine2 Job ID
        :param image: The docker image name
        :param env: Environment for the docker container
        :param vols: Volumes for the docker container
        :param labels: Labels for the docker container
        :param queues: If there is a fin_q then whether or not to run it async
        :return:
        """

        c = self.docker.containers.run(image, 'async',
                                       environment=env,
                                       detach=True,
                                       labels=labels,
                                       volumes=vols)
        self.containers.append(c)
        # Start a thread to monitor output and handle finished containers
        t = Thread(target=self._shepherd, args=[c, job_id, queues])
        self.threads.append(t)
        t.start()
        return c

    def remove(self, c):
        """
        Wrapper to kill and remove a docker container.
        :param c: A reference to docker container object
        :return:
        """
        try:
            c.kill()
        except Exception:
            pass

        try:
            c.remove()
        except Exception:
            pass
