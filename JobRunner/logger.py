import logging
import os
import sys
from typing import List

from clients.execution_engine2Client import execution_engine2


# TODO: Add a buffer  we may need some flush thread too.


class Logger(object):
    def __init__(self, ee2_url: str, job_id: str, ee2: execution_engine2 = None):
        self.ee2_url = ee2_url
        self.ee2 = ee2
        if self.ee2 is None:
            self.ee2 = execution_engine2(self.ee2_url)

        self.job_id = job_id
        self.debug = os.environ.get("DEBUG_RUNNER", None)
        self.jr_logger = logging.getLogger("jr")
        self.jr_logger.info(f"Logger initialized for {job_id}")
        self.retry = False

    def _add_job_logs(self, lines: List):
        """
        Allow ee2 log retries, or fail and report it back
        :param lines:
        :return:
        """
        try:
            self.ee2.add_job_logs({"job_id": self.job_id}, lines)
        except Exception:
            if self.retry:
                self.ee2.add_job_logs({"job_id": self.job_id}, lines)

    def log_lines(self, lines: List):
        """
        Wrapper for logging multiple logs at once, at various log levels
        :param lines: The lines to log
        """
        if self.debug:  # pragma: no cover
            for line in lines:
                if line["is_error"]:
                    sys.stderr.write(line + "\n")
                else:
                    self.jr_logger.info(line["line"])
        self._add_job_logs(lines)

    def log(self, line: str, ts=None):
        """
        Wrapper for preparing a single error log line
        :param line: The lines to log, at a default log level
        :param ts: Timestamp
        """
        if self.debug:  # pragma: no cover
            self.jr_logger.info(line)
        log_line = {"line": line, "is_error": 0}
        if ts:
            line["ts"] = ts
        self._add_job_logs([log_line])

    def error(self, line: str, ts=None):
        """
        Wrapper for preparing a single normal log line
        :param line: The lines to log, at an ERROR log level
        :param ts: Timestamp
        """
        log_line = {"line": line, "is_error": 1}
        if ts:
            line["ts"] = ts
        self.jr_logger.error(line)
        self._add_job_logs([log_line])
