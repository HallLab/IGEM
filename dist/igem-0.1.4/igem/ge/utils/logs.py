import logging

from ge.models import Logs

"""
Status:
    e = error
    w = warning
    s = success
"""


def start_logger(process=None):
    # create a logger
    logger = logging.getLogger(process)

    # configure the logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


# Logs
def logger(log, status="e", message=None):
    log.info(message)
    if not message:
        message = 'not inform'
    Logs.objects.create(
        process=log.name, description=message, status=status
    )
    return True
