"""Convenience module for configuring logging for the zolltools package"""

import logging
from typing import Union, Iterable

LOGGERS: list[str] = [
    "zolltools.db.pqtools",
    "zolltools.db.sasconvert",
    "zolltools.nemsis.locationcodes",
    "zolltools.logging",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _get_loggers(logger_names: Union[Iterable[str], str, None]) -> list[logging.Logger]:
    """
    Returns the loggers associated with the names provided.

    :param logger_names: `None` for all loggers in the package. Provide a list
    of names or a single name of a module for which the logger is required
    otherwise.
    :returns: a list of the loggers requested.
    :raises ValueError: if any of the names provided are not valid modules.
    """

    logger_names_is_invalid_str: bool = (
        isinstance(logger_names, str) and logger_names not in LOGGERS
    )
    logger_names_contains_invalid_str: bool = (
        not isinstance(logger_names, str)
        and isinstance(logger_names, Iterable)
        and not set(logger_names).issubset(LOGGERS)
    )
    if logger_names_is_invalid_str or logger_names_contains_invalid_str:
        if isinstance(logger_names, str):
            logger_names = [logger_names]
        invalid_logger_names: set[str] = set(logger_names).difference(LOGGERS)
        raise ValueError(f"{invalid_logger_names} are not valid loggers:\n\t{LOGGERS}")

    if logger_names is None:
        logger_names = LOGGERS
    elif isinstance(logger_names, str):
        logger_names = [logger_names]

    return [logging.getLogger(name) for name in logger_names]


def add_handler(
    handler: logging.Handler,
    logger_names: Union[Iterable[str], str, None] = None,
    clear=False,
) -> list[logging.Logger]:
    """
    Adds `handler` to loggers in the zolltools package.

    :param handler: the handler to add to the logger(s).
    :param logger_names: leave as `None` to apply the handler to all zolltools
    loggers. Set to a logger name (e.g. "zolltools.nemsis.locationcodes") to
    select that particular logger. Set to a list of logger names to apply to all
    those loggers.
    :param clear: set to `True` to clear all other handlers from the logger(s)
    before adding `handler`.
    :returns: a list of the loggers the handler was applied to.
    :raises ValueError: if a logger specified by `logger_names` does not exist
    in the package.
    """

    loggers = _get_loggers(logger_names)
    for module_logger in loggers:
        if clear:
            module_logger.handlers.clear()
        module_logger.addHandler(handler)
    return loggers


def set_level(
    level: Union[int, str],
    logger_names: Union[Iterable[str], str, None] = None,
) -> list[logging.Logger]:
    """
    Sets the logging level for loggers in the zolltools package.

    :param level: the level to assign to the loggers
    :param logger_names: `None` to set all loggers. Provide a list of names (or
    just a single name) to set the level of only those loggers.
    :returns: a list of the loggers the level was assigned to.
    :raises ValueError: if a logger specified by `logger_names` does not exist
    in the package.
    """

    loggers = _get_loggers(logger_names)
    for module_logger in loggers:
        module_logger.setLevel(level)
    return loggers
