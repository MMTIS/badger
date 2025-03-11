#  We will have logging based on the standard logging
# https://docs.python.org/3/howto/logging.html
# --log=INFO
# --logfile=filename
import logging
from typing import Any

from configuration import NOSOFTLOGGING, processing_data, log_dict
import os
import sys


# Basic ideas:
# - There might still remain print statements, that are just send to the screen
# - we still wrap them to make sure, we can deactivate it
# - log level can be set
# - There is a standard log (log format)
# - Some logs have special purposes (e.g. the script protocol, outputs with lists)
# - some things are corrected and only one problem is resolved.
# - the logging should assist the pipeline.


# Attention: log_print only writes to the screen and not to the log_file
def log_print(s: str) -> None:
    global NOSOFTLOGGING
    if not NOSOFTLOGGING:
        print(s)


def prepare_logger(log_level: int, log_file_name: str) -> logging.Logger:
    # create logger
    global log_dict
    global processing_data
    mylogger = logging.getLogger("script_runner")
    if mylogger.hasHandlers():
        # already initalised
        return mylogger
    if mylogger:
        mylogger.setLevel(log_level)
        log_dict: dict[str, list[Any]] = {}  # type: ignore
        # create console handler and set level to debug
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # add formatter to ch
        ch.setFormatter(formatter)
        mylogger.addHandler(ch)

        # add ch to logger
        if log_file_name is not None and len(log_file_name) > 5:
            # if the processing dir doesn't exist, then we create it
            directory = os.path.dirname(processing_data + "/" + log_file_name)
            os.makedirs(directory, exist_ok=True)
            fh = logging.FileHandler(processing_data + "/" + log_file_name, mode="a")
            fh.setFormatter(formatter)
            fh.setLevel(log_level)
            mylogger.addHandler(fh)
    else:
        print("ERROR: Logger can't be initalized.")
    return mylogger


# just log every occurance
def log_all(log_level: int, message: str) -> None:
    mylogger = logging.getLogger("script_runner")
    mylogger.log(log_level, message)
    log_flush()


# Only prints the message once and continues
def log_once(log_level: int, key: str, message: str) -> None:
    global log_dict
    a = log_dict.get(key)
    mylogger = logging.getLogger("script_runner")
    if a is None:
        log_dict[key] = [1, message]
        mylogger.log(log_level, key + ":" + message)
        log_flush()
    else:
        count = log_dict[key][0] + 1
        mess = log_dict[key][1]
        log_dict[key] = [count, mess]


# writes the numbers of occurances of each error type
def log_write_counts(log_level: int) -> None:
    global log_dict
    mylogger = logging.getLogger("script_runner")
    if len(log_dict) > 0:
        mylogger.log(logging.INFO, "Logging: Recapitulation of warnings")
        for key, arr in log_dict.items():
            mylogger.log(log_level, f"{key}: {arr[1]} (counted {arr[0]})")
        log_dict = {}
        log_flush()


# flushes the log to disk
def log_flush() -> None:
    mylogger = logging.getLogger("script_runner")
    if mylogger is not None:
        for handler in mylogger.handlers:
            handler.flush()
    else:
        print("ERROR: not flushing log.")
