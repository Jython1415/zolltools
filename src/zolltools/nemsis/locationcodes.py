"""Module for working with Y92 codes"""

from pathlib import Path
import argparse
import sys
import pickle
from importlib import resources
import zolltools
from zolltools import strtools

_STORAGE_FOLDER_NAME = "location-codes-groupings"
_STORAGE_FOLDER_EXT = ".json"


def get_mapping() -> dict:
    """Returns the Y92 mapping"""

    root = resources.files(zolltools)
    mapping_file = root.joinpath("nemsis", "data", "y92-mapping.pkl")
    with mapping_file.open("rb") as file:
        return pickle.load(file)


def to_description(code, default=None):
    """Returns the description for the provided code
    Raises a KeyError exception if the code does not
    match the data dictionary for Y92 code listing
    """

    if code == "7701001":
        return "Not Applicable"
    elif code == "7701003":
        return "Not Recorded"

    try:
        description = get_mapping()[code]
    except KeyError as error:
        if default is None:
            raise KeyError(
                f"{code} is an invalid code."
                "Not in the NEMSIS data dictionary or Y92 code listings."
            ) from error
        return "not-found"

    return description


def get_grouping(name: str) -> dict:
    """
    Returns a grouping of Y92 codes.

    :param name: the name of the grouping to retrieve.
    :returns: a dict representing the grouping
    :raises FileNotFoundError: when there is no grouping corresponding to `name`
    """

    return {"hi": name}


def _list():
    """Lists location code groupings"""

    storage_dir = Path.cwd().joinpath(_STORAGE_FOLDER_NAME)
    paths = storage_dir.glob(f"*{_STORAGE_FOLDER_EXT}")
    names = sorted(
        [strtools.removesuffix(path.name, _STORAGE_FOLDER_EXT) for path in paths]
    )

    print("\n".join(names))


def _validate():
    """Validates a location code grouping"""


def _init():
    """Initializes a folder for location code groupings"""

    storage_dir = Path.cwd().joinpath(_STORAGE_FOLDER_NAME)
    storage_dir.mkdir(exist_ok=True)

    print(f"Store location code groupings in {_STORAGE_FOLDER_NAME}\n> {storage_dir}")


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
