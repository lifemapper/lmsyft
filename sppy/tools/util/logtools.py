"""Standard logger for console and/or file logging."""
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

# Rough log of processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5


# .....................................................................................
class Logger:
    """Class containing a logger for consistent logging."""

    # .......................
    def __init__(
            self, log_name, log_path=None, log_console=True,
            log_level=logging.INFO):
        """Constructor.

        Args:
            log_name (str): A name for the logger.
            log_path (str): A path for write logging information to a file.
            log_console (bool): Should logs be written to the console.
            log_level (int): What level of logs should be retained.
        """
        self.log_directory = log_path
        self.log_name = log_name
        self.log_console = log_console
        self.log_level = log_level
        self.filename = None

        # Create file and/or console handlers
        handlers = []
        if self.log_directory is not None:
            self.filename = os.path.join(self.log_directory, f"{self.log_name}.log")
            os.makedirs(self.log_directory, exist_ok=True)
            handlers.append(
                RotatingFileHandler(
                    self.filename, mode="w", maxBytes=LOGFILE_MAX_BYTES, backupCount=10,
                    encoding="utf-8")
            )
        if self.log_console:
            handlers.append(logging.StreamHandler(stream=sys.stdout))

        # Get logger
        self.logger = logging.getLogger(self.log_name)
        self.logger.setLevel(logging.DEBUG)
        # Add handlers to logger
        formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
        for handler in handlers:
            handler.setLevel(self.log_level)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.propagate = False

    # ........................
    def log(self, msg, refname="", log_level=logging.INFO):
        """Log a message.

        Args:
            msg (str): A message to write to the logger.
            refname (str): Class or function name to use in logging message.
            log_level (int): A level to use when logging the message.
        """
        if self.logger is not None:
            self.logger.log(log_level, refname + ': ' + msg)


# ......................................................
def logit(logger, msg, refname="", log_level=logging.INFO):
    """Log or print a message.

    Args:
        logger: Logger object
        msg: Message to log
        refname: referring object, module, or function
        log_level: level of severity
    """
    if log_level is None:
        log_level = logging.INFO
    if logger is not None:
        logger.log(msg, refname=refname, log_level=log_level)
    else:
        print(f"{refname}:log_level {log_level}: {msg}")


"""
{'Date': 'Mon, 30 Nov 2020 17:12:14 GMT',
'Content-Type': 'application/json',
'Access-Control-Allow-Origin': '*',
'Access-Control-Allow-Methods': 'HEAD, GET, POST, DELETE, PUT, OPTIONS',
'Server': 'Jetty(9.3.z-SNAPSHOT)',
'Cache-Control': 'public, max-age=3601',
'X-Varnish': '614072909',
'Age': '0',
'Via': '1.1 varnish (Varnish/6.0)',
'Content-Length': '4986',
'Connection': 'keep-alive'}
"""
