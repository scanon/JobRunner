import os
import sys
import logging
from clients.execution_engine2Client import execution_engine2


# TODO: Add a buffer  we may need some flush thread too.


class Logger(object):
    def __init__(self, ee2_url, job_id, ee2=None):
        self.ee2_url = ee2_url
        self.ee2 = ee2
        if self.ee2 is None:
            self.ee2 = execution_engine2(self.ee2_url)

        self.job_id = job_id
        self.debug = os.environ.get("DEBUG_RUNNER", None)
        self.jr_logger = logging.getLogger("jr")
        self.jr_logger.info(f"Logger initialized for {job_id}")

    def log_lines(self, lines):
        if self.debug:  # pragma: no cover
            for line in lines:
                if line["is_error"]:
                    sys.stderr.write(line + "\n")
                else:
                    print(line["line"])
        self.ee2.add_job_logs({"job_id": self.job_id}, lines)

    def log(self, line, ts=None):
        if self.debug:  # pragma: no cover
            self.jr_logger.info(line)
        log_line = {"line": line, "is_error": 0}
        if ts:
            line["ts"] = ts

        self.ee2.add_job_logs({"job_id": self.job_id}, [log_line])

    def error(self, line, ts=None):
        log_line = {"line": line, "is_error": 1}
        if ts:
            line["ts"] = ts
        self.ee2.add_job_logs({"job_id": self.job_id}, [log_line])
        try:
            raise Exception(f"Exception: {line}")
        except Exception as e:
            if self.debug:  # pragma: no cover
                self.jr_logger.error(f"Failure: {line}")
