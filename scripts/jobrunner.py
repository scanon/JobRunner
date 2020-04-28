#!/usr/bin/env python
# import sentry_sdk
import logging
import os
import sys
import time
from typing import Dict

from JobRunner.JobRunner import JobRunner

# from sentry_sdk.integrations.sanic import SanicIntegration
# from sentry_sdk import configure_scope

_TOKEN_ENV = "KB_AUTH_TOKEN"
_ADMIN_TOKEN_ENV = "KB_ADMIN_AUTH_TOKEN"
_DEBUG = "DEBUG_MODE"


def get_jr_logger():
    logger = logging.getLogger("jr")
    logger.propagate = False
    logger.setLevel(0)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    log_level = os.environ.get("LOGLEVEL", "INFO").upper()
    if log_level:
        ch.setLevel(log_level)
        logger.setLevel(log_level)

    formatter = logging.Formatter("%(created)f:%(levelname)s:%(name)s:%(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


jr_logger = get_jr_logger()


def _get_debug_mode():
    """
    Check to see if this job run is in debug mode
    :return:
    """
    if _DEBUG in os.environ:
        if os.environ[_DEBUG].lower() == "true":
            return True
    return False


def _get_token():
    # Get the token from the environment or a file.
    # Set the KB_AUTH_TOKEN if not set.
    if _TOKEN_ENV in os.environ:
        token = os.environ[_TOKEN_ENV]
    else:
        try:
            with open("token") as f:
                token = f.read().rstrip()
            os.environ[_TOKEN_ENV] = token
        except:
            jr_logger.error("Failed to get token.")
            sys.exit(2)
    return token


def _get_admin_token():
    if _ADMIN_TOKEN_ENV not in os.environ:
        jr_logger.error("Missing admin token needed for volume mounts.")
        sys.exit(2)
    admin_token = os.environ.pop("KB_ADMIN_AUTH_TOKEN")
    if _ADMIN_TOKEN_ENV in os.environ:
        jr_logger.error("Failed to sanitize environment")
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
    params = {
        "job_id": jr.job_id,
        "error_message": "Unexpected Job Error",
        "error_code": 2,
        "terminated_code": 2,
    }

    _terminate_job_in_ee2(jr=jr, params=params)

    # Attempt to clean up Docker and Special Runner Containers
    # Kill Callback Server
    try:
        jr.mr.cleanup_all(debug=_get_debug_mode())
    except Exception as e:
        jr_logger.info(e)

    try:
        jr.cbs.kill()
    except Exception as e2:
        jr_logger.info(e2)

    jr.logger.error(
        f"An unhandled exception resulted in a premature exit of the app. Job id is {jr.job_id}"
    )


def main():
    # Input job id and njs_service URL
    if len(sys.argv) == 3:
        job_id = sys.argv[1]
        ee2_url = sys.argv[2]
    else:
        jr_logger.error("Incorrect usage")
        sys.exit(1)

    # sentry_sdk.init(dsn=os.environ.get("SENTRY_URL"), integrations=[SanicIntegration()])

    config = dict()
    config["workdir"] = os.getcwd()
    if not os.path.exists(config["workdir"]):
        os.makedirs(config["workdir"])
        jr_logger.info(f"Creating work directory at {config['workdir']}")

    config["catalog-service-url"] = ee2_url.replace("ee2", "catalog")
    auth_ext = "auth/api/legacy/KBase/Sessions/Login"
    config["auth-service-url"] = ee2_url.replace("ee2", auth_ext)

    # WARNING: Condor job environment may not inherit from system ENV
    if "USE_SHIFTER" in os.environ:
        config["runtime"] = "shifter"

    if "JR_MAX_TASKS" in os.environ:
        config["max_tasks"] = int(os.environ["JR_MAX_TASKS"])

    token = _get_token()
    at = _get_admin_token()
    debug = _get_debug_mode()

    # with configure_scope() as scope:
    #     scope.user = {"username": os.environ.get('USER_ID')}
    jr = None
    try:
        jr_logger.info("About to create job runner")
        jr = JobRunner(config, ee2_url, job_id, token, at, debug)
    except Exception as e:
        jr_logger.error(
            f"An unhandled error was encountered setting up job runner {e}",
            exc_info=True,
        )
        if jr:
            terminate_job(jr)
        sys.exit(3)

    try:
        jr_logger.info("About to run job with job runner")
        if debug:
            jr.logger.log(
                line=f"Debug mode enabled. Containers will not be deleted after job run."
            )
        jr.run()
    except Exception as e:
        jr_logger.error(f"An unhandled error was encountered {e}", exc_info=True)
        jr.logger.error(line=f"{e}")
        terminate_job(jr)
        sys.exit(4)


if __name__ == "__main__":
    main()
