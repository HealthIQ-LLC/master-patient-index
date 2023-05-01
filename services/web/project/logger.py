import logging

from .__version__ import version
version = version.replace(".", "")


def make_logger(name:str):
    services_logger = setup_logger(__name__, F"{name}.log")

    return services_logger


def setup_logger(name, log_file, level=logging.DEBUG):
    """configure a services logger
    :param name: the repr of the location or application being logged
    :param level: the logging level (debug, ..., critical)
    :param log_file: the absolute path to the log, as a string
    """
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


mylogger = make_logger(f"EMPI_{version}")
