"""Module for caching objects in notebooks"""

from __future__ import annotations

import uuid
import pickle
from pathlib import Path
from collections.abc import Callable
from typing import TypeVar, Optional, NamedTuple

T = TypeVar("T")
State = TypeVar("State")
StorageObject = NamedTuple("StorageObject", [("state", State), ("stored_object", T)])


def load(  # pylint: disable=too-many-arguments
    state: State,
    generate: Callable[[State], T],
    unique_id: int,
    folder: Path = Path.cwd(),
    force_update: bool = False,
    reload: Optional[Callable[[State, State], bool]] = None,
    hash_func: Optional[Callable[[object], int]] = None,
) -> tuple[T, Path]:
    """
    Stores/loads an object to/from storage. The object is what is returned by
    `generate(state)`. If the object has not been stored yet, or if the `state`
    parameter has changed between calls of the function, the object will be
    regenerated by calling the `generate` function. Only if the `state` is the
    same as the previous call will the object be returned without a repeated
    call of `generate`. `unique_id` must be the same between function calls to
    retrieve the same object. Overlapping ids will overwrite other cached
    objects.

    :param state: the state used to generate the object to be stored. Ideally it
    should contain only what is necessary to generate the object to store.
    :param generate: the function used to generate the object.
    :param unique_id: a unique identifier to link a cached object between
    function calls and sessions.
    :param folder: the folder to generate the cache file in.
    :param force_update: set to True to regenerate the object regardless of the
    return value of `reload`.
    :param reload: set to a callable to define a custom reload condition. The
    default comparison is `previous_state != state`. The states are passed into
    `reload` as `reload(previous_state, state)`. A return value of `True` would
    cause the object to be regenerated.
    :param hash_func: the hash function used to determine the file name. It is
    recommended that you only change this to manually avert a detected
    collision that is unavoidable otherwise. The default hash function is the
    built-in `hash`.
    :returns: the object and the path to the object.
    """

    if reload is None:
        reload = _default_state_comparison
    assert reload is not None

    if hash_func is None:
        hash_func = hash
    assert hash_func is not None

    file_path = folder.joinpath(uuid.UUID(int=hash_func(str(unique_id))).hex)
    if not file_path.exists() or force_update:
        return (_store(file_path, state, generate), file_path)

    with open(file_path, "rb") as file:
        stored_object_package: StorageObject = pickle.load(file)
        prev_state, stored_object = stored_object_package

    if reload(prev_state, state):
        stored_object = _store(file_path, state, generate)

    return (stored_object, file_path)


def _default_state_comparison(prev_state: State, state: State) -> bool:
    """
    Compares two states to determine whether they are different. Returns true if
    the states are different.

    :param prev_state: the previous state.
    :param state: the current state.
    :returns: True if the states are different. False otherwise.
    """

    return prev_state != state


def _store(file_path: Path, state: State, generate: Callable[[State], T]) -> T:
    """
    Stores the output of `generate(state)` at `file_path`, overwriting the
    existing file as necessary.

    :param file_path: the path to store the object at.
    :state: the state from which to generate the object.
    :generate: the function used to generate the object to store.
    :returns: the object that is stored.
    """

    with open(file_path, "wb") as file:
        stored_object = generate(state)
        pickle.dump(StorageObject(state, stored_object), file)
        return stored_object
