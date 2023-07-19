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
    reload: Optional[Callable[[State, State], bool]] = None,
    folder: Path = Path.cwd(),
    force_update: bool = False,
    hash_func: Optional[Callable[[object], int]] = None,
) -> tuple[T, Path]:
    """
    Placeholder docstring
    """

    if reload is None:
        reload = _default_state_comparison
    assert reload is not None

    if hash_func is None:
        hash_func = hash
    assert hash_func is not None

    file_path = folder.joinpath(uuid.UUID(int=hash_func(str(unique_id)).hex))
    if not file_path.exists() or force_update:
        return (_store(file_path, state, generate), file_path)

    with open(file_path, "rb") as file:
        stored_object_package: StorageObject = pickle.load(file)
        prev_state, stored_object = stored_object_package

    if reload(prev_state, state):
        stored_object = _store(file_path, state, generate)

    return (stored_object, file_path)


def _default_state_comparison(prev_state: State, state: State) -> bool:
    return prev_state == state


def _store(file_path: Path, state: State, generate: Callable[[State], T]) -> T:
    with open(file_path, "wb") as file:
        stored_object = generate(state)
        pickle.dump(StorageObject(state, stored_object), file)
        return stored_object
