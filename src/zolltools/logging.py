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
    handler: logging.Handler,
    logger_names: Union[Iterable[str], str, None] = None,
    clear=False,  # pylint: disable=unused-argument
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

    log_prefix = "add_handler"

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
        logger.error("%s: %s are not a valid loggers", log_prefix, invalid_logger_names)
        raise ValueError(
            f"{invalid_logger_names} are not a valid loggers:\n\t{LOGGERS}"
        )

    if logger_names is None:
        logger_names = LOGGERS
        logger.debug("%s: selected all loggers", log_prefix)
    elif isinstance(logger_names, str):
        logger_names = [logger_names]
        logger.debug("%s: selected single logger: %s", log_prefix, logger_names[0])

    result = []
    for name in logger_names:
        result_logger = logging.getLogger(name)
        result_logger.addHandler(handler)
        result.append(result_logger)
    return result


def set_level(
    level: Union[int, str],  # pylint: disable=unused-argument
    logger_names: Union[  # pylint: disable=unused-argument
        Iterable[str], str, None
    ] = None,
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

    return []
