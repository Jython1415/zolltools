"""Tests for locationcodes.py"""

import logging

import pytest
import hypothesis as hp
import hypothesis.strategies as st
from zolltools import logging as zoll_logging


def test_add_handler_all() -> None:
    """
    Tests the add_handler function when adding a handler to all loggers
    """

    expected_loggers = zoll_logging.LOGGERS

    handler = logging.NullHandler()
    result_loggers: list[logging.Logger] = zoll_logging.add_handler(handler)
    assert isinstance(result_loggers, list)
    for logger in result_loggers:
        assert isinstance(logger, logging.Logger)
        assert logger.name in expected_loggers
        assert handler in logger.handlers
    result_logger_names: set[str] = {logger.name for logger in result_loggers}
    assert len(set(expected_loggers).difference(result_logger_names)) == 0


@pytest.mark.parametrize(("logger_name"), zoll_logging.LOGGERS)
def test_add_handler_specific(logger_name) -> None:
    """
    Tests the add_handler function when adding a handler to a specific logger
    """

    handler = logging.NullHandler()
    result_logger: logging.Logger = zoll_logging.add_handler(
        handler, logger_names=logger_name
    )[0]
    assert isinstance(result_logger, logging.Logger)
    assert result_logger.name == logger_name
    assert handler in result_logger.handlers


@hp.given(logger_names=st.sets(st.sampled_from(zoll_logging.LOGGERS), min_size=1))
def test_add_handler_list(logger_names):
    """
    Tests the add_handler function when adding a handler to a list of loggers
    """

    handler = logging.NullHandler()
    result_loggers = zoll_logging.add_handler(handler, logger_names=logger_names)
    assert isinstance(result_loggers, list)
    for logger in result_loggers:
        assert isinstance(logger, logging.Logger)
        assert logger.name in logger_names
        assert handler in logger.handlers
    result_logger_names: set[str] = {logger.name for logger in result_loggers}
    assert len(set(logger_names).difference(result_logger_names)) == 0


@hp.given(logger_name=st.text())
def test_add_handler_exception(logger_name) -> None:
    """
    Tests the add_handler function when the logger requested does not exist
    """

    hp.assume(logger_name not in zoll_logging.LOGGERS)
    with pytest.raises(ValueError):
        zoll_logging.add_handler(logging.NullHandler(), logger_names=logger_name)


@hp.given(logger_name=st.sampled_from(zoll_logging.LOGGERS))
def test_add_handler_clear_option(logger_name) -> None:
    """
    Tests the clear parameter for the add_handler function
    """

    handler1 = logging.NullHandler()
    handler2 = logging.NullHandler()
    handler3 = logging.NullHandler()

    result_loggers = zoll_logging.add_handler(handler1, logger_names=logger_name)
    assert isinstance(result_loggers, list)
    assert len(result_loggers) == 1
    result_logger: logging.Logger = result_loggers[0]
    assert handler1 in result_logger.handlers
    result_loggers = zoll_logging.add_handler(handler2, logger_names=logger_name)
    assert handler1 in result_logger.handlers
    assert handler2 in result_logger.handlers
    result_loggers = zoll_logging.add_handler(
        handler3, logger_names=logger_name, clear=True
    )
    assert handler1 not in result_logger.handlers
    assert handler2 not in result_logger.handlers
    assert handler3 in result_logger.handlers


@hp.given(
    expected_level=st.one_of(
        st.sampled_from(
            [
                logging.DEBUG,
                logging.INFO,
                logging.WARNING,
                logging.ERROR,
                logging.CRITICAL,
            ]
        ),
        st.integers(),
    )
)
def test_set_level_all(expected_level) -> None:
    """
    Tests the set_level function when applied to all modules
    """

    expected_loggers = zoll_logging.LOGGERS
    result_loggers = zoll_logging.set_level(expected_level)
    assert isinstance(result_loggers, list)
    for logger in result_loggers:
        assert isinstance(logger, logging.Logger)
        assert logger.name in expected_loggers
        assert logger.level == expected_level
    result_logger_names: set[str] = {logger.name for logger in result_loggers}
    assert len(set(expected_loggers).difference(result_logger_names)) == 0


@hp.given(
    logger_name=st.sampled_from(zoll_logging.LOGGERS),
    expected_level=st.one_of(
        st.sampled_from(
            [
                logging.DEBUG,
                logging.INFO,
                logging.WARNING,
                logging.ERROR,
                logging.CRITICAL,
            ]
        ),
        st.integers(),
    ),
)
def test_set_level_specific(logger_name, expected_level) -> None:
    """
    Tests the set_level function when setting the level for a specific logger
    """

    result_loggers = zoll_logging.set_level(expected_level, logger_names=logger_name)
    assert isinstance(result_loggers, list)
    result_logger = result_loggers[0]
    assert isinstance(result_logger, logging.Logger)
    assert result_logger.name == logger_name
    assert result_logger.level == expected_level


@hp.given(
    logger_names=st.sets(st.sampled_from(zoll_logging.LOGGERS), min_size=1),
    expected_level=st.one_of(
        st.sampled_from(
            [
                logging.DEBUG,
                logging.INFO,
                logging.WARNING,
                logging.ERROR,
                logging.CRITICAL,
            ]
        ),
        st.integers(),
    ),
)
def test_set_level_list(logger_names, expected_level) -> None:
    """
    Tests the set_level function when setting the level for a list of loggers
    """

    result_loggers = zoll_logging.set_level(expected_level, logger_names=logger_names)
    assert isinstance(result_loggers, list)
    for logger in result_loggers:
        assert isinstance(logger, logging.Logger)
        assert logger.name in logger_names
        assert logger.level == expected_level
    result_logger_names: set[str] = {logger.name for logger in result_loggers}
    assert len(set(logger_names).difference(result_logger_names)) == 0


@hp.given(logger_name=st.text())
def test_add_level_exception(logger_name) -> None:
    """
    Tests the add_level function when the logger requested does not exist
    """

    hp.assume(logger_name not in zoll_logging.LOGGERS)
    with pytest.raises(ValueError):
        zoll_logging.set_level(logging.ERROR, logger_names=logger_name)
