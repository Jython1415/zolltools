"""Module for working with Y92 codes"""

import pickle
from importlib import resources
import zolltools


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
