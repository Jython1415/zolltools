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

    root = resources.files(zolltools)
    mapping_file = root.joinpath("nemsis", "data", "y92-mapping.pkl")
    with mapping_file.open("rb") as file:
        return pickle.load(file)


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
        mapping = get_mapping()

    if code == "7701001":
        return "Not Applicable"
    elif code == "7701003":
        return "Not Recorded"

    try:
        description = mapping[code]
    except KeyError as error:
        if default is None:
            raise KeyError(
                f"{code} is an invalid code."
                "Not in the NEMSIS data dictionary or Y92 code listings."
            ) from error
        return default

    return description


def _get_storage_dir() -> Path:
    """Return a Path pointing to the directory holding the groupings"""

    return Path.cwd().joinpath(STORAGE_FOLDER_NAME)


def get_grouping(name: str) -> dict:
    """
    Returns a grouping of Y92 codes.

    :param name: the name of the grouping to retrieve.
    :returns: a dict representing the grouping
    :raises FileNotFoundError: when there is no grouping corresponding to `name`
    """

    storage_dir = _get_storage_dir()
    grouping_path = storage_dir.joinpath(f"{name}{STORAGE_FOLDER_EXT}")

    if not grouping_path.exists():
        raise FileNotFoundError(
            f'"{name}" cannot be found. {grouping_path} does not exist.'
        )

    try:
        with open(grouping_path, "rb") as grouping_file:
            return json.load(grouping_file)
    except OSError as error:
        raise OSError(f"{name} could not be accessed") from error


def _list():
    """Lists location code groupings"""

    storage_dir = _get_storage_dir()
    paths = storage_dir.glob(f"*{STORAGE_FOLDER_EXT}")
    names = sorted(
        [strtools.removesuffix(path.name, STORAGE_FOLDER_EXT) for path in paths]
    )

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
