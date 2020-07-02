import os
from threading import Thread
from subprocess import Popen, PIPE
from select import select


class ShifterRunner:
    """
    This class provides the container interface for Docker.

    """

    def __init__(self, logger=None):
        """
        Inputs: config dictionary, Job ID, and optional logger
        """
        self.logger = logger
        self.containers = []
        self.threads = []

    def _readio(self, p, job_id, queues):
        cont = True
        last = False
        while cont:
            rlist = [p.stdout, p.stderr]
            x = select(rlist, [], [], 1)[0]
            for f in x:
                if f == p.stderr:
                    error = 1
                else:
                    error = 0
                l = f.readline().decode('utf-8')
                if len(l) > 0:
                    try:
                        self.logger.log_lines([{'line': l, 'is_error': error}])
                    except Exception as e:
                        print(e)
                        continue
            if last:
                cont = False
            if p.poll() is not None:
                last = True
        p.wait()
        for q in queues:
            q.put(['finished', job_id, None])

    def get_image(self, image):
        # Do a shifterimg images
        lookcmd = ['myshifter', 'lookup', image]
        proc = Popen(lookcmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        id = stdout.decode('utf-8').rsplit()
        if id == '':
            cmd = ['myshifter', 'pull', image]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            stdout, stderr = proc.communicate()
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            stdout, stderr = proc.communicate()
            id = stdout.decode('utf-8').rsplit()

        return id

    def run(self, job_id, image, env, vols, labels, queues, cgroup=None):
        cmd = [
            'myshifter',
            'run',
            '--image=%s' % (image)
            ]
        # TODO: Do somehting with the labels
        for hd in vols.keys():
           cmd.extend(['--volume', '%s:%s' % (hd, vols[hd]['bind'])])
        newenv = os.environ
        for e in env.keys():
            newenv[e] = env[e]
        proc = Popen(cmd, bufsize=0, stdout=PIPE, stderr=PIPE, env=newenv)
        out = Thread(target=self._readio, args=[proc, job_id, queues])
        self.threads.append(out)
        out.start()
        self.containers.append(proc)
        return proc

    def remove(self, c):
        # Kill process
        c.kill()
