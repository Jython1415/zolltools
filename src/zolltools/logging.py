"""Convenience module for configuring logging for the zolltools package"""

import logging
from typing import Union, Iterable

LOGGERS: list[str] = [
    "zolltools.db.pqtools",
    "zolltools.db.sasconvert",
    "zolltools.nemsis.locationcodes",
    "zolltools.logging",
    "zolltools.strtools",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def add_handler(
    handler: logging.Handler,  # pylint: disable=unused-argument
    logger_names: Union[
        Iterable[str], str, None
    ] = None,  # pylint: disable=unused-argument
    clear=False,  # pylint: disable=unused-argument
) -> list[logging.Logger]:
    """
    Adds `handler` to a logger (or all loggers) in the zolltools package.

    :param handler: the handler to add to the logger(s).
    :param logger_names: leave as `None` to apply the handler to all zolltools
    loggers. Set to a logger name (e.g. "zolltools.nemsis.locationcodes") to
    select that particular logger. Set to a list of logger names to apply to all
    those loggers.
    :param clear: set to `True` to clear all other handlers from the logger(s)
    before adding `handler`.
    :returns: a list of the loggers the handler was applied to.
    :raises ValueError: if the logger specified by `logger` cannot be found.
    """

    if logger_names is None:
        logger_names = LOGGERS
    elif isinstance(logger_names, str) and logger_names in LOGGERS:
        logger_names = [logger_names]
    elif not set(logger_names).issubset(LOGGERS):
        invalid_logger_names = set(logger_names).difference(LOGGERS)
        logger.error("add_handler: %s is not a valid logger", str(invalid_logger_names))
        raise ValueError(
            f"{invalid_logger_names} are not a valid loggers:\n\t{LOGGERS}"
        )

    result = []
    for name in logger_names:
        result_logger = logging.getLogger(name)
        result_logger.addHandler(handler)
        result.append(result_logger)
    return result
