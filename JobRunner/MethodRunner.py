from clients.CatalogClient import Catalog
import docker
from configparser import ConfigParser
import os
import json
import logging
from time import time, sleep

# Get the image version from the catalog
# Grab that image from dockerhub using that image version and tag
# TODO: Set up a log flushing thread
# TODO: Get secure params (e.g. username and password)
# Write out config file with all kbase endpoints / secure params

class JobError(Exception):

    def __init__(self, name, code, message, data=None, error=None):
        super(Exception, self).__init__(message)
        self.name = name
        self.code = code
        self.message = '' if message is None else message
        self.data = data or error or ''
        # data = JSON RPC 2.0, error = 1.1

    def __str__(self):
        return self.name + ': ' + str(self.code) + '. ' + self.message + \
            '\n' + self.data


class MethodRunner:

    def __init__(self, config, job_id):
        self.token = config['token']
        self.catalog_url = config.get('catalog-service-url')
        self.catalog = Catalog(self.catalog_url, token=self.token)
        self.docker = docker.from_env()
        self.config = config
        self.log = logging.getLogger('runner')
        self.refbase = '/tmp/ref'
        self.workdir = config.get('workdir', '/mnt/awe/condor')
        self.job_id = job_id
        self.basedir = os.path.join(self.workdir, 'job_%s' % (self.job_id))
        if not os.path.exists(self.basedir):
            os.mkdir(self.basedir)
        self.job_dir = os.path.join(self.basedir, 'workdir')
        if not os.path.exists(self.job_dir):
            os.mkdir(self.job_dir)
        self.subjobdir = os.path.join(self.basedir, 'subjobs')
        if not os.path.exists(self.subjobdir):
            os.mkdir(self.subjobdir)
        self.catadmin = Catalog(self.catalog_url, token=config['admin_token'])


    def _init_workdir(self, config, job_dir, params):
        # Create config.properties and inputs
        conf_prop = ConfigParser()

        conf_prop['global'] = {
          'kbase_endpoint': config['kbase.endpoint'],
          'workspace_url': config['workspace.srv.url'],
          'shock_url': config['shock.url'],
          'handle_url': config['handle.url'],
          'auth_service_url': config['auth-service-url'],
          'auth_service_url_allow_insecure': config['auth-service-url-allow-insecure'],
          'scratch': '/kb/module/work/tmp'
           }

        with open(job_dir + '/config.properties', 'w') as configfile:
            conf_prop.write(configfile)

        # Create input.json
        input = {
            "version": "1.1",
            "method": params['method'],
            "params": params['params'],
            "context": dict()
            }
        ijson = job_dir + '/input.json'
        with open(ijson, 'w') as f:
            f.write(json.dumps(input))

        # Create token file
        with open(job_dir + '/token', 'w') as f:
            f.write(self.token)


        return True

    def _sort_logs(self, sout, serr):
        lines = []
        if len(sout) > 0:
            for line in sout.decode("utf-8").split('\n'):
                if len(line) > 0:
                    lines.append({'line': line, 'is_error': 1})
        if len(serr) > 0:
            for line in serr.decode("utf-8").split('\n'):
                if len(line) > 0:
                    lines.append({'line': line, 'is_error': 0})
        return lines

    def _get_volume_mounts(self, module, method, cgroup):
        req = {
            'module_name': module,
            'function_name': method,
            'client_group': cgroup
        }
        if self.catadmin is None:
            return None
        return self.catadmin.list_volume_mounts({'module_name': module})

    def _get_job_dir(self, job_id, subjob=False):
        if subjob:
            return os.path.join(self.subjobdir, job_id)
        else:
            return self.job_dir


    # TODO: Thin this down a bit and move some of this to init
    def run(self, config, params, job_id, callback=None, subjob=False, logger=None, return_output=False):
        """
        Run the method.  This is used for subjobs too.
        This is a blocking call.  It will not return until the job/process exits.
        """
        # Mkdir workdir/tmp
        job_dir = self._get_job_dir(job_id, subjob=subjob)
        if not os.path.exists(job_dir):
            os.mkdir(job_dir)
        (module, method) = params['method'].split('.')
        version = params.get('service_ver')

        # Look up the module info
        req = {'module_name': module}
        if version is not None:
            req['version'] = version
        module_info = self.catalog.get_module_version(req)
        image = module_info['docker_img_name']
        list = self.docker.images.list()

        # Pull the image if we don't have it
        pulled = False
        for im in list:
            if image in im.tags:
                id = im.id
                pulled = True
        if not pulled:
            self.log.info("Pulling %s" % (image))
            id = self.docker.images.pull(image).id

        self.log.info("image id=%s job_id=%s" % (id, job_id))

        # Initialize workdir
        self._init_workdir(config, job_dir, params)

        # Run the container
        vols = {
            job_dir: {'bind': '/kb/module/work', 'mode': 'rw'}
        }
        # Check to see if that image exists, and if refdata exists
        # paths to tmp dirs, refdata, volume mounts/binds
        if 'data_version' in module_info:
            ref_data = os.path.join(self.refbase, module_info['data_folder'], module_info['data_version'])
            vols[ref_data] = {'bind': '/data', 'mode': 'ro'}
        # TODO: Use admin token to get volume mounts 
        extra_vols = self._get_volume_mounts(module, method, None)
        env = {
            'SDK_CALLBACK_URL': callback
        }
        # TODO: Add secure params
        # Set up labels used for job administration purposes
        labels = {
            "app_id": "%s/%s" % (module, method),
            "app_name": method,
            "condor_id": os.environ.get('CONDOR_ID'),
            "image_name": image,
            "image_version": image.split('.')[-1],
            "job_id": job_id,
            "method_name": "TODO",
            "njs_endpoint": "https://kbase.us/services/njs_wrapper",
            "parent_job_id": "",
            "user_name": config['user'],
            "wsid": str(params.get('wsid',''))
        }
        c = self.docker.containers.run(image, 'async',
                                   environment=env,
                                   detach=True,
                                   labels=labels,
                                   volumes=vols)
        # Start a thread to monitor output and handle finished containers
        # 
        last = 1
        while c.status in ['created', 'running']:
            c.reload()
            now = int(time())
            sout = c.logs(stdout=True, stderr=False, since=last, until=now, timestamps=True)
            serr = c.logs(stdout=False, stderr=True, since=last, until=now, timestamps=True)
            # TODO: Stream this to njs
            lines = self._sort_logs(sout, serr)
            if logger is not None:
                logger.log(lines)
            last=now
            sleep(1)
        c.remove()
        if return_output:
            output = self.get_output(job_id, subjob=subjob)
            return output


    def is_finished(self, job_id, subjob=True):
        of = os.path.join(self._get_job_dir(job_id, subjob=subjob), 'output.json')
        if os.path.exists(of):
            return True

    def get_output(self, job_id, subjob=True):
        # Attempt to read output file and see if it is well formed
        # Throw errors if not
        of = os.path.join(self._get_job_dir(job_id, subjob=subjob), 'output.json')
        if os.path.exists(of):
            with open(of) as json_file:
                output = json.load(json_file)
        else:
            print("No output")
            raise OSError('No output json')

        if 'error' in output:
            print("Error in job")
            raise JobError(**output['error'])

        return output

    def cleanup(self, config, job_id=''):
        return
        # sleep(1)
        # for c in self.docker.containers.list(all=True, filters={'label': 'job_id=%s' % (job_id)}):
        #     print(c.status)
        #     c.remove()
        # for d in self.dirs:
        #     for f in ['token', 'config.properties', 'input.json', 'output.json']:
        #         try:
        #             os.remove(d+'/'+f)
        #         except:
        #             continue
        #     try:
        #         os.removedirs(d)
        #     except:
        #         continue
        # self.dirs = []
