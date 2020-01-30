#!/usr/bin/env python

import logging
import os
import sys
import time
from typing import Dict

from JobRunner.JobRunner import JobRunner

logging.basicConfig(level=logging.INFO, format=log_format)
_TOKEN_ENV = "KB_AUTH_TOKEN"
_ADMIN_TOKEN_ENV = "KB_ADMIN_AUTH_TOKEN"
_DEBUG = "DEBUG_MODE"


def _get_debug_mode():
    """
    Check to see if this job run is in debug mode
    :return:
    """
    if _DEBUG in os.environ:
        if os.environ[_DEBUG].lower() == 'true':
            return True
    return False


def _get_token():
    # Get the token from the environment or a file.
    # Set the KB_AUTH_TOKEN if not set.
    if _TOKEN_ENV in os.environ:
        token = os.environ[_TOKEN_ENV]
    else:
        try:
            with open('token') as f:
                token = f.read().rstrip()
            os.environ[_TOKEN_ENV] = token
        except:
            print("Failed to get token.")
            sys.exit(2)
    return token


def _get_admin_token():
    if _ADMIN_TOKEN_ENV not in os.environ:
        print("Missing admin token needed for volume mounts.")
        sys.exit(2)
    admin_token = os.environ.pop('KB_ADMIN_AUTH_TOKEN')
    if _ADMIN_TOKEN_ENV in os.environ:
        print("Failed to sanitize environment")
    return admin_token


def _terminate_job_in_ee2(jr: JobRunner, params: Dict):
    finished_job_in_ee2 = False
    attempts = 5

    while not finished_job_in_ee2:
        if attempts == 0:
            break
        try:
            jr.ee2.finish_job(params=params)
            finished_job_in_ee2 = True
        except Exception:
            try:
                jr.ee2.cancel_job(params=params)
                finished_job_in_ee2 = True
            except Exception:
                pass
        attempts -= 1
        if not finished_job_in_ee2:
            time.sleep(30)


def terminate_job(jr: JobRunner):
    """
    Unexpected Job Error, so attempt to finish the job, and if that fails, attempt to cancel the job
    """
    params = {'job_id': jr.job_id,
              'error_message': 'Unexpected Job Error',
              'error_code': 2,
              'terminated_code': 2
              }

    _terminate_job_in_ee2(jr=jr, params=params)

    # Attempt to clean up Docker and Special Runner Containers
    # Kill Callback Server
    try:
        jr.mr.cleanup_all(debug=_get_debug_mode())
    except Exception as e:
        logging.info(e)

    try:
        jr.cbs.kill()
    except:
        logging.info(e)

    jr.logger.error(
        f'An unhandled exception resulted in a premature exit of the app. Job id is {jr.job_id}')


def main():
    # Input job id and njs_service URL
    if len(sys.argv) == 3:
        job_id = sys.argv[1]
        ee2_url = sys.argv[2]
    else:
        print("Incorrect usage")
        sys.exit(1)

    config = dict()
    config['workdir'] = os.getcwd()
    if not os.path.exists(config['workdir']):
        os.makedirs(config['workdir'])
        logging.info(f"Creating work directory at {config['workdir']}")

    config['catalog-service-url'] = ee2_url.replace('ee2', 'catalog')
    auth_ext = 'auth/api/legacy/KBase/Sessions/Login'
    config['auth-service-url'] = ee2_url.replace('ee2', auth_ext)

    # WARNING: Condor job environment may not inherit from system ENV
    if 'USE_SHIFTER' in os.environ:
        config['runtime'] = 'shifter'

    if 'JR_MAX_TASKS' in os.environ:
        config['max_tasks'] = int(os.environ['JR_MAX_TASKS'])

    token = _get_token()
    at = _get_admin_token()
    debug = _get_debug_mode()

    try:
        logging.info("About to create job runner")
        jr = JobRunner(config, ee2_url, job_id, token, at, debug)
        if debug:
            jr.logger.log(line=f'Debug mode enabled. Containers will not be deleted after job run.')
        jr.run()
    except Exception as e:
        logging.error("An unhandled error was encountered")
        logging.error(e, exc_info=True)
        terminate_job(jr)
        sys.exit(2)


if __name__ == '__main__':
    main()
