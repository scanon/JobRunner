#!/usr/bin/env python

import logging
import os
import signal
import sys
import time
from enum import Enum

import psutil

from JobRunner.logger import Logger

logging.basicConfig(level=logging.INFO)
_TOKEN_ENV = "KB_AUTH_TOKEN"
_ADMIN_TOKEN_ENV = "KB_ADMIN_AUTH_TOKEN"

import subprocess


def tailf(filename):
    # returns last 15 lines from a file, starting from the beginning
    command = "tail -n 15 " + filename
    p = subprocess.Popen(command.split(), stdout=subprocess.PIPE, universal_newlines=True)
    lines = []
    for line in p.stdout:
        lines.append(line)
    return lines


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


job_runner_error_fp = "../_condor_stderr"
job_runner_out_fp = "../_condor_stdout"


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

            # gives a single float value
            cpu = psutil.cpu_percent()
            # gives an object with many fields
            # you can convert that object to a dictionary
            memory = dict(psutil.virtual_memory()._asdict())

            killed_message = "The monitor jobrunner script killed your job. It is possible the " \
                             f"node got overloaded with too many jobs! Memory={memory} CPU={cpu} "

            last_lines = tailf(job_runner_error_fp)
            ts = None
            for line in last_lines:
                try:
                    ts = time.strptime(line.split(" ")[0], "%H:%M:%S")
                except ValueError:
                    pass
                try:
                    logger.error(line=line, ts=ts)
                except Exception:
                    pass

            error_msg = f"Unexpected Job Error. Your job was killed by {sys.argv[0]} "

            error = {
                "code": ErrorCode.job_terminated_by_automation.value,
                "name": "Output not found",
                "message": error_msg,
                "error": error_msg,
            }

            kill_job_params = {
                "job_id": job_id,
                "error_message": error_msg,
                "error_code": ErrorCode.job_terminated_by_automation.value,
                "error": error
            }

            attempt_kill(logger=logger, killed_message=killed_message,
                         kill_job_params=kill_job_params, ts=ts)
            attempt_kill(logger=logger, killed_message=killed_message,
                         kill_job_params=kill_job_params, ts=ts)

            try:
                os.kill(int(job_runner_pid), signal.SIGKILL)
                sys.exit(1337)
            except Exception as e:
                print(e)


def attempt_kill(logger, killed_message, kill_job_params, ts):
    try:
        logger.error(killed_message)
        logger.ee2.finish_job(kill_job_params)
    except Exception as fjException:
        time.sleep(10)
        logger.error(line=str(fjException), ts=ts)
        logger.error(killed_message)


if __name__ == "__main__":
    main()
