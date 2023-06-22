"""Module for working with location codes"""

import sys
import time
import json
import pickle
import logging
import argparse
from pathlib import Path
from importlib import resources

import zolltools
from zolltools import strtools

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

STORAGE_FOLDER_NAME = "location-codes-groupings"
STORAGE_FOLDER_EXTENSION = ".json"

_MAPPING = None


def get_mapping() -> dict:
    """
    Returns the location code mapping. The first read will read the mapping from
    storage.

    :returns: the location code mapping
    """

    # TODO CLN: refactor `locationcodes` to avoid a global mapping variable
    global _MAPPING  # pylint: disable=global-statement

    log_prefix = "get_mapping"

    start_time = time.perf_counter_ns()
    if _MAPPING is not None:
        return _MAPPING
    root = resources.files(zolltools)
    mapping_file = root.joinpath("nemsis", "data", "location-code-mapping.pkl")
    logger.debug("%s: identified mapping file: %s", log_prefix, repr(mapping_file))
    with mapping_file.open("rb") as file:
        mapping = pickle.load(file)
        _MAPPING = mapping
        end_time = time.perf_counter_ns()
        logger.info(
            "%s: mapping read from %s in %d ns",
            log_prefix,
            mapping_file,
            end_time - start_time,
        )
        return mapping


def to_description(code, default=None, mapping=None):
    """
    Returns the description for the provided code

    :param code: the code to get a description of
    :param default: the value to return if a description is not found. If set to
    `None`, a `KeyError` exception will be raised instead.
    :param mapping: the mapping to use. If set to `None`, the function will use
    the mapping provided in the module: `locationcodes.get_mapping()`
    :returns: a description of the location code, `code`
    :raises KeyError: if the code does not match the data dictionary for
    location code listing
    """

    log_prefix = "to_description"

    if mapping is None:
        logger.debug("%s: mapping accessed with get_mapping", log_prefix)
        mapping = get_mapping()

    if code == "7701001":
        return "Not Applicable"
    if code == "7701003":
        return "Not Recorded"

    try:
        description = mapping[code]
    except KeyError as error:
        if default is None:
            logger.error(
                "%s: code (%s) not found in mapping %s", log_prefix, code, repr(mapping)
            )
            raise KeyError(
                f"{code} is an invalid code." "Not in the location code mapping %s",
                str(mapping),
            ) from error
        return default

    return description


def _get_storage_dir() -> Path:
    """Return a Path pointing to the directory holding the groupings"""

    storage_dir_path = Path.cwd().joinpath(STORAGE_FOLDER_NAME)
    return storage_dir_path


def get_grouping(name: str) -> dict:
    """
    Returns a grouping of location codes.

    :param name: the name of the grouping to retrieve.
    :returns: a dict representing the grouping
    :raises FileNotFoundError: when there is no grouping corresponding to `name`
    """

    log_prefix = "get_grouping"

    storage_dir = _get_storage_dir()
    grouping_path = storage_dir.joinpath(f"{name}{STORAGE_FOLDER_EXTENSION}")
    logger.debug("%s: file path identified as %s", log_prefix, grouping_path)

    if not grouping_path.exists():
        logger.error(
            "%s: grouping file (%s) could not be found", log_prefix, grouping_path
        )
        raise FileNotFoundError(
            f'"{name}" cannot be found. {grouping_path} does not exist.'
        )

    try:
        with open(grouping_path, "rb") as grouping_file:
            logger.debug("%s: grouping file successfully opened", log_prefix)
            grouping = json.load(grouping_file)
            logger.debug("%s: grouping read from file", log_prefix)
            return grouping
    except OSError as error:
        logger.error(
            "%s: grouping file (%s) could not be accessed", log_prefix, grouping_path
        )
        raise OSError(f"{name} could not be accessed") from error


def _list():
    """Lists location code groupings"""

    storage_dir = _get_storage_dir()
    paths = storage_dir.glob(f"*{STORAGE_FOLDER_EXTENSION}")
    names = sorted(
        [strtools.removesuffix(path.name, STORAGE_FOLDER_EXTENSION) for path in paths]
    )
    logger.debug(
        "_list: %d groupings identified in %s: %s", len(names), storage_dir, str(names)
    )

    print("\n".join(names))


def _validate():
    """Validates a location code grouping"""

    print("This command is still WIP...")


def _init():
    """Initializes a folder for location code groupings"""

    storage_dir = _get_storage_dir()
    storage_dir.mkdir(exist_ok=True)

    print(f"Store location code groupings in {STORAGE_FOLDER_NAME}\n> {storage_dir}")


def _main():
    """Method that defines the logic of the module when executed."""

    parser = argparse.ArgumentParser(description="module description")
    parser.add_argument(
        "action",
        choices=["list", "validate", "init"],
        help="important help message",
    )

    args = parser.parse_args(sys.argv[1:])

    if args.action == "list":
        _list()
    elif args.action == "validate":
        _validate()
    elif args.action == "init":
        _init()
    else:
        raise ValueError(f"Bad argument: {args.action}")


if __name__ == "__main__":
    _main()
