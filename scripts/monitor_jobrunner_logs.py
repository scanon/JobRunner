#!/usr/bin/env python

import io
import logging
import os
import signal
import sys
import time
from enum import Enum

from JobRunner.logger import Logger

logging.basicConfig(level=logging.INFO)
_TOKEN_ENV = "KB_AUTH_TOKEN"
_ADMIN_TOKEN_ENV = "KB_ADMIN_AUTH_TOKEN"


class TerminatedCode(Enum):
    """
    Reasons for why the job was cancelled
    """

    terminated_by_user = 0
    terminated_by_admin = 1
    terminated_by_automation = 2


class ErrorCode(Enum):
    """
    Reasons why the job was marked as error
    """

    unknown_error = 0
    job_crashed = 1
    job_terminated_by_automation = 2
    job_over_timelimit = 3
    job_missing_output = 4
    token_expired = 5


def _set_token():
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
            print("Failed to get token.")
            sys.exit(2)
    return token


job_runner_error_fp = "jobrunner.error"
job_runner_out_fp = "jobrunner.out"


def job_contains_unhandled_error():
    unhandled_error_pattern = r"ERROR:root:An unhandled error was encountered"
    with open(job_runner_error_fp, "r", encoding="utf-8") as f:
        if unhandled_error_pattern in f.read():
            return True
    return False


def main():
    _set_token()
    # Input job id and ee2 URL
    if len(sys.argv) == 4:
        job_id = sys.argv[1]
        ee2_url = sys.argv[2]
        job_runner_pid = sys.argv[3]
    else:
        print("Incorrect usage. Provide job_id,  ee2_url,  and PID of job runner")
        sys.exit(1)

    while True:
        time.sleep(5)
        if job_contains_unhandled_error():

            logger = Logger(ee2_url=ee2_url, job_id=job_id)


            killed_message = "The monitor jobrunner script killed your job. It's possible the " \
                             "node got overloaded with too many jobs! Memory={memory} CPU={cpu} "



            with io.open(job_runner_error_fp, "r", encoding="utf-8") as f:
                for line in f.readlines():
                    ts = None
                    try:
                        ts = time.strptime(line.split(" ")[0], "%H:%M:%S")
                    except ValueError:
                        print("Not a timestamp")
                    logger.error(line=line, ts=ts)

                try:

                    logger.ee2.finish_job(
                        {
                            "job_id": job_id,
                            "error_message": "Unexpected Job Error. Your job was killed by "
                                             + sys.argv[0],
                            "error_code": ErrorCode.job_terminated_by_automation.value,
                        }
                    )
                    logger.error(killed_message)
                except Exception as fjException:
                    logger.error(line=str(fjException), ts=ts)

                    try:
                        logger.ee2.cancel_job(
                            {
                                "job_id": job_id,
                                "terminated_code": TerminatedCode.terminated_by_automation.value,
                            }
                        )
                    except Exception as cjException:
                        logger.error(line=str(cjException), ts=ts)
                    logger.error(killed_message)
                try:
                    os.kill(int(job_runner_pid), signal.SIGKILL)
                    sys.exit(1337)
                except Exception as e:
                    print(e)


if __name__ == "__main__":
    main()
