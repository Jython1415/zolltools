"""Convenience module for configuring logging for the zolltools package"""

import logging
from typing import Union

LOGGERS: list[str] = [
    "zolltools.db.pqtools",
    "zolltools.db.sasconvert",
    "zolltools.nemsis.locationcodes",
    "zolltools.logging",
    "zolltools.strtools",
]


def add_handler(
    handler: logging.Handler,  # pylint: disable=unused-argument
    logger_name: Union[None, str] = None,  # pylint: disable=unused-argument
    clear=False,  # pylint: disable=unused-argument
) -> list[logging.Logger]:
    """
    Adds `handler` to a logger (or all loggers) in the zolltools package.

    :param handler: the handler to add to the logger(s).
    :param logger: leave as `None` to apply the handler to all zolltools
    loggers. Set to a logger name (e.g. "zolltools.nemsis.locationcodes") to
    select that particular logger.
    :param clear: set to `True` to clear all other handlers from the logger(s)
    before adding `handler`.
    :returns: a list of the loggers the handler was applied to.
    :raises ValueError: if the logger specified by `logger` cannot be found.
    """

    if logger_name is None:
        logger_names = LOGGERS
    elif logger_name in LOGGERS:
        logger_names = [logger_name]
    else:
        raise ValueError(f"{logger_name} is not a valid logger:\n\t{LOGGERS}")

    result = []
    for name in logger_names:
        logger = logging.getLogger(name)
        logger.addHandler(handler)
        result.append(logger)
    return result
