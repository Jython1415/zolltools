"""Module for working with Y92 codes"""

import logging
import json
from pathlib import Path
import argparse
import sys
import pickle
from importlib import resources
import zolltools
from zolltools import strtools

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

STORAGE_FOLDER_NAME = "location-codes-groupings"
STORAGE_FOLDER_EXT = ".json"


def get_mapping() -> dict:
    """Returns the Y92 mapping"""

    logger.debug("get_mapping: function called")
    root = resources.files(zolltools)
    logger.debug("get_mapping: root traversable created: %s", repr(root))
    mapping_file = root.joinpath("nemsis", "data", "y92-mapping.pkl")
    logger.debug("get_mapping: identified mapping file: %s", repr(mapping_file))
    with mapping_file.open("rb") as file:
        logger.debug("get_mapping: mapping file opened")
        mapping = pickle.load(file)
        logger.info("get_mapping: mapping read from file")
        return mapping


def to_description(code, default=None, mapping=None):
    """
    Returns the description for the provided code

    :param code: the code to get a description of
    :param default: the value to return if a description is not found. If set to `None`,
    a KeyError exception will be raised instead.
    :param mapping: the mapping to use. If set to `None`, the function will use the mapping
    provided in the module: `locationcodes.get_mapping()`
    :returns: a description of the Y92 code, `code`
    :raises KeyError: if the code does not match the data dictionary for Y92 code listing
    """

    if mapping is None:
        logger.debug("to_description: mapping accessed with get_mapping")
        mapping = get_mapping()

    if code == "7701001":
        return "Not Applicable"
    elif code == "7701003":
        return "Not Recorded"

    try:
        description = mapping[code]
    except KeyError as error:
        if default is None:
            logger.error(
                "to_description: code (%s) not found in mapping %s", code, repr(mapping)
            )
            raise KeyError(
                f"{code} is an invalid code."
                "Not in the NEMSIS data dictionary or Y92 code listings."
            ) from error
        return default

    return description


def _get_storage_dir() -> Path:
    """Return a Path pointing to the directory holding the groupings"""

    storage_dir_path = Path.cwd().joinpath(STORAGE_FOLDER_NAME)
    logger.debug("_get_storage_dir: storage directory identified as %s", storage_dir_path)
    return storage_dir_path


def get_grouping(name: str) -> dict:
    """
    Returns a grouping of Y92 codes.

    :param name: the name of the grouping to retrieve.
    :returns: a dict representing the grouping
    :raises FileNotFoundError: when there is no grouping corresponding to `name`
    """

    storage_dir = _get_storage_dir()
    grouping_path = storage_dir.joinpath(f"{name}{STORAGE_FOLDER_EXT}")
    logger.debug("get_grouping: file path identified as %s", grouping_path)

    if not grouping_path.exists():
        logger.error("get_grouping: grouping file (%s) could not be found", grouping_path)
        raise FileNotFoundError(
            f'"{name}" cannot be found. {grouping_path} does not exist.'
        )

    try:
        with open(grouping_path, "rb") as grouping_file:
            logger.debug("get_grouping: grouping file successfully opened")
            grouping = json.load(grouping_file)
            logger.debug("get_grouping: grouping read from file")
            return grouping
    except OSError as error:
        logger.error("get_grouping: grouping file (%s) could not be accessed", grouping_path)
        raise OSError(f"{name} could not be accessed") from error


def _list():
    """Lists location code groupings"""

    storage_dir = _get_storage_dir()
    paths = storage_dir.glob(f"*{STORAGE_FOLDER_EXT}")
    names = sorted(
        [strtools.removesuffix(path.name, STORAGE_FOLDER_EXT) for path in paths]
    )
    logger.debug("_list: %d groupings identified: %s", len(names), repr(names))

    print("\n".join(names))


def _validate():
    """Validates a location code grouping"""


def _init():
    """Initializes a folder for location code groupings"""

    storage_dir = _get_storage_dir()
    storage_dir.mkdir(exist_ok=True)

    print(f"Store location code groupings in {STORAGE_FOLDER_NAME}\n> {storage_dir}")


def main():
    """Method that defines the logic of the module when executed."""
    parser = argparse.ArgumentParser(description="module description")
    parser.add_argument(
        "action",
        choices=["list", "validate", "init"],
        help="important help message",
    )

    # FIND FIRST ITEM IN sys.argv[] THAT STARTS WITH "locationcodes"
    args = parser.parse_args(sys.argv[1:])

    if args.action == "list":
        _list()
    elif args.action == "validate":
        _validate()
    elif args.action == "init":
        _init()
    else:
        print("this should not have happened...")


if __name__ == "__main__":
    main()
