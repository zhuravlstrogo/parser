import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler



def setup_logging(path):
    log_formatter = logging.Formatter(
        "[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s"
    )

    time_rot_log_handler = TimedRotatingFileHandler(
        f"{path}logs/update.log", when="midnight", backupCount=100
    )
    time_rot_log_handler.setFormatter(log_formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)

    logger = logging.getLogger()

    logger.addHandler(time_rot_log_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)


def exception_handler(type, value, traceback):
    logging.error("Logging an uncaught exception", exc_info=(type, value, traceback))


# Install exception handler
sys.excepthook = exception_handler
