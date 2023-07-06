from functools import wraps
import logging
from time import time
import sys
from .__version__ import version

version = version.replace(".", "")
DEBUG_ROUTE = sys.stderr
SYSTEM_USER = "empi_system"


def timeit(func):
    """
    :param func: Decorated function
    :return: wrapped function
    An opinion for exec-timing service components
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        print(
            f"{func.__name__} executed in {end - start:.4f} seconds",
            file=DEBUG_ROUTE
        )
        return result
    return wrapper


def make_logger(name: str):
    services_logger = setup_logger(__name__, F"{name}.log")

    return services_logger


def setup_logger(name, log_file, level=logging.DEBUG):
    """configure a services logger
    :param name: the repr of the location or application being logged
    :param level: the logging level (debug, ..., critical)
    :param log_file: the absolute path to the log, as a string
    :return logger: a formatted, leveled, handled, named, and located logging object
    """
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


mylogger = make_logger(f"empi_{version}")
