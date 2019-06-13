from .DockerRunner import DockerRunner
import os
import json
from time import time as _time
from time import sleep as _sleep
from configparser import ConfigParser
import sys

# Get the image version from the catalog
# Grab that image from dockerhub using that image version and tag
# TODO: Set up a log flushing thread
# TODO: Get secure params (e.g. username and password)
# Write out config file with all kbase endpoints / secure params
# TODO: Extract catalog stuff

class MethodRunner:
    """
    This class marshalls request for jobs and launches the containers.

    It handles preparing the run area, monitoring the container execution,
    and returning output via a queue.
    """

    def __init__(self, config, job_id, logger=None):
        """
        Inputs: config dictionary, Job ID, and optional logger
        """
        self.config = config
        self.job_id = job_id
        self.logger = logger
        self.token = config['token']
        self.workdir = config.get('workdir', '/mnt/awe/condor')
        self.basedir = os.path.join(self.workdir, 'job_%s' % (self.job_id))
        self.refbase = config.get('refdata_dir', '/tmp/ref')
        self.job_dir = os.path.join(self.basedir, 'workdir')
        runtime = config.get('runtime', 'docker')
        self.containers = []
        if runtime=='docker':
            self.runner = DockerRunner(logger=logger)
        else:
            raise OSError("Unknown runtime")


    def _init_workdir(self, config, job_dir, params):
        # Create all the directories
        if not os.path.exists(self.basedir):
            os.mkdir(self.basedir)
        if not os.path.exists(self.job_dir):
            os.mkdir(self.job_dir)
        self.subjobdir = os.path.join(self.basedir, 'subjobs')
        if not os.path.exists(self.subjobdir):
            os.mkdir(self.subjobdir)
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

    def _get_job_dir(self, job_id, subjob=False):
        if subjob:
            return os.path.join(self.subjobdir, job_id)
        else:
            return self.job_dir


    # TODO: Thin this down a bit and move some of this to init
    def run(self, config, module_info, params, job_id, fin_q=None, callback=None, subjob=False):
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

        image = module_info['docker_img_name']
        id = self.runner.get_image(image)

        if subjob:
            self.logger.log('Subjob method: {} JobID: {}'.format(params['method'], job_id))
        self.logger.log('Running docker container for image: {}'.format(image))


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
        extra_vols = module_info.get('volume_mounts', None) 
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

        # If there is a fin_q then run this async
        action = {
            'name': module,
            'ver': version,
            'code_url': module_info['git_url'],
            'commit': module_info['git_commit_hash']
        }
        # TODO thing about error handling here
        c = self.runner.run(job_id, image, env, vols, labels, subjob, [fin_q])
        self.containers.append(c)
        #args = [job_id, image, env, vols, labels, subjob, [fin_q]]
        return action

    def get_output(self, job_id, subjob=True):
        # Attempt to read output file and see if it is well formed
        # Throw errors if not
        of = os.path.join(self._get_job_dir(job_id, subjob=subjob), 'output.json')
        if os.path.exists(of):
            with open(of) as json_file:
                output = json.load(json_file)
        else:
            self.logger.error("No output")
            return None

        if 'error' in output:
            self.logger.error("Error in job")

        return output

    def cleanup_all(self):
        for c in self.containers:
            try:
                c.remove()
            except:
                continue
        return True
