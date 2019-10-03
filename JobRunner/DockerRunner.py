import docker
from threading import Thread
from time import time as _time
from time import sleep as _sleep


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
        self.log_interval = 1

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

    def _shepherd(self, c, job_id, queues):
        last = 1
        try:
            dolast = False
            while True:
                now = int(_time())
                sout = c.logs(stdout=True, stderr=False, since=last, until=now,
                              timestamps=True)
                serr = c.logs(stdout=False, stderr=True, since=last, until=now,
                              timestamps=True)
                lines = self._sort_lines_by_time(sout, serr)
                if self.logger is not None:
                    self.logger.log_lines(lines)
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
                print("Exception in docker logging for %s" % (c.id))
                raise(e)
        finally:
            try:
                c.remove()
                self.containers.remove(c)
            except Exception:
                # Maybe something already cleaned it up.  Move on.
                pass
            for q in queues:
                q.put(['finished', job_id, None])

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

    def run(self, job_id, image, env, vols, labels, queues):
        print("Running container with env")
        print(env)
        print("Volumes")
        print(vols)
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
        try:
            c.kill()
        except Exception:
            pass

        try:
            c.remove()
        except Exception:
            pass
