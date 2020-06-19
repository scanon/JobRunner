import atexit
import logging
from threading import Thread
from time import sleep as _sleep
from time import time as _time
from typing import List

import docker
from docker.errors import ImageNotFound
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

    @staticmethod
    def _sort_lines_by_time(sout, serr):
        """
        This is an internal function to sort and interlace output for NJS.
        This is not fully implemented yet and sould be rethought.
        """
        lines_by_time = dict()
        ierr = 0
        for stream in [sout, serr]:
            if len(sout) == 0 and len(serr) == 0:
                continue
            for line in stream.decode("utf-8").split("\n"):
                if len(line) > 0:
                    elements = line.split(maxsplit=1)
                    if len(elements) == 2:
                        (ts, txt) = elements
                    else:
                        ts = elements[0]
                        txt = ""
                    if ts not in lines_by_time:
                        lines_by_time[ts] = []
                    lines_by_time[ts].append({"line": txt, "is_error": ierr})
            ierr += 1
        nlines = []
        for ts in sorted(lines_by_time.keys()):
            nlines.extend(lines_by_time[ts])
        return nlines

    def _shepherd_logs(self, c, now, last):
        sout = c.logs(stdout=True, stderr=False, since=last, until=now, timestamps=True)
        serr = c.logs(stdout=False, stderr=True, since=last, until=now, timestamps=True)
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
                self.logger.error(f"Unexpected failure in docker logging. {e}")
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
                    msg = (
                        f"Not going to delete container {c.id} because debug mode is on"
                    )
                    self.logger.log(msg)
                    logging.info(msg)
                else:
                    c.remove()
                    self.containers.remove(c)
            except Exception:
                # Maybe something already cleaned it up.  Move on.
                pass
            for q in queues:
                q.put(["finished", job_id, None])

    def _pull_and_run(self, image, env, labels, vols, cgroup_parent=None):
        """
        Pull an image and then attempt to run it
        :param image: Image to pull
        :param env: Env for the docker container
        :param labels: Labels for the docker container
        :param vols: Vols for the docker container
        :return: Container ID
        """
        image_id = None

        try:
            # See if the image exists
            image_id = self.docker.images.get(name=image).id
        except docker.errors.ImageNotFound as e:
            self.logger.error(e)

        try:
            # If no tag is specified, will return a list
            image_id = self.docker.images.pull(image).id
        except docker.errors.ImageNotFound as e:
            self.logger.error(e)

        if image_id is None:
            raise docker.errors.ImageNotFound(f"Couldn't find image for {image}")

        return self.docker.containers.run(
            image,
            "async",
            environment=env,
            detach=True,
            labels=labels,
            volumes=vols,
            cgroup_parent=cgroup_parent,
        )

    def run(self, job_id, image, env, vols, labels, queues, cgroup=None):
        """
        Start a docker container for the main job or subjobs
        and append it to the list of docker containers
        :param job_id: The ExecutionEngine2 Job ID
        :param image: The docker image name
        :param env: Environment for the docker container
        :param vols: Volumes for the docker container
        :param labels: Labels for the docker container
        :param queues: If there is a fin_q then whether or not to run it async
        :param cgroup: The optional cgroup to use as a cgroup parent
        :return: Container ID
        """
        logging.info(f"About to run {job_id} {image}")
        try:
            c = self._pull_and_run(
                image=image, env=env, labels=labels, vols=vols, cgroup_parent=cgroup
            )
        except ImageNotFound:
            _sleep(5)
            c = self._pull_and_run(
                image=image, env=env, labels=labels, vols=vols, cgroup_parent=cgroup
            )

        self.containers.append(c)
        # Start a thread to monitor output and handle finished containers
        t = Thread(target=self._shepherd, args=[c, job_id, queues])
        self.threads.append(t)
        t.start()
        return c

    @staticmethod
    def remove(c):
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
