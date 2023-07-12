"""Tests for locationcodes.py"""

import logging

import pytest
import hypothesis as hp
import hypothesis.strategies as st
from zolltools import logging as zoll_logging


def test_add_handler_all() -> None:
    """Tests the add_handler function when adding a handler to all loggers"""

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


@pytest.mark.parametrize(("logger_name"), [zoll_logging.LOGGERS])
def test_add_handler_specific(logger_name) -> None:
    """Tests the add_handler function when adding a handler to a specific logger"""

    handler = logging.NullHandler()
    result_logger: logging.Logger = zoll_logging.add_handler(
        handler, logger=logger_name
    )[0]
    assert isinstance(result_logger, logging.Logger)
    assert result_logger.name == logger_name
    assert handler in result_logger.handlers


@hp.given(logger_name=st.text())
def test_add_handler_exception(logger_name) -> None:
    """Tests the add_handler when the logger request does not exist"""

    hp.assume(logger_name not in zoll_logging.LOGGERS)
    with pytest.raises(ValueError):
        zoll_logging.add_handler(logging.NullHandler(), logger=logger_name)
