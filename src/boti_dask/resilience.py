from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]

from ._internal import _is_dask_dataframe_like

try:
    from dask.distributed import get_client
except ImportError:
    get_client = None


try:
    from distributed.comm.core import CommClosedError
except ImportError:

    class CommClosedError(Exception):
        pass


try:
    from tornado.iostream import StreamClosedError
except ImportError:

    class StreamClosedError(Exception):
        pass


RECOVERABLE_DASK_ERRORS = (
    CommClosedError,
    StreamClosedError,
    TimeoutError,
    ConnectionError,
    OSError,
)


# Module-level fallback for call sites with no caller-supplied logger (e.g.
# static helpers). Debug level only — these are expected/best-effort paths.
#
# This logger's name (and get_client's module-global binding just above) are
# load-bearing for tests: they monkeypatch `resilience_module.get_client` and
# scope `caplog.at_level(..., logger="boti_dask.resilience")`, so every
# function below that touches either must stay defined in *this* module
# rather than move to resilience_ops.py/resilience_async.py.
_module_log = logging.getLogger(__name__)


def _has_dask_graph(obj: Any) -> bool:
    return hasattr(obj, "__dask_graph__")


def _is_dask_collection(obj: Any) -> bool:
    return _is_dask_dataframe_like(obj) or _has_dask_graph(obj)


def _contains_dask_collection(obj: Any) -> bool:
    if _is_dask_collection(obj):
        return True
    if isinstance(obj, dict):
        obj = obj.values()
    elif not isinstance(obj, (list, tuple)):
        return False
    return any(_contains_dask_collection(value) for value in obj)


def _call_get_client_safely() -> Any | None:
    if get_client is None:
        return None
    try:
        return get_client()
    except Exception:
        _module_log.debug("get_client() failed while resolving active Dask client", exc_info=True)
        return None


# default is unused here (an int can't fail to convert), but the signature
# must match the other _TO_INT_SAFE_HANDLERS callbacks for the dispatch
# table below to treat them uniformly.
# spaghetti-ignore[pass-through-method]
def _to_int_safe_int(value: Any, default: int) -> int:
    return int(value)


def _to_int_safe_float(value: Any, default: int) -> int:
    if np is not None and np.isnan(value):
        return default
    return int(value)


def _to_int_safe_first_element(
    value: Any, default: int, *, fallback_first: Callable[[Any], Any], kind: str
) -> int:
    """Extract the first element of a sequence-like *value*.

    Tries numpy's fast path first; falls back to ``fallback_first(value)``
    when numpy is unavailable or the fast path fails.
    """
    if np is not None:
        try:
            array = np.asarray(value)
            if array.size == 0:
                return default
            return _to_int_safe(array.ravel()[0], default=default)
        except Exception:
            _module_log.debug(
                "numpy fast path failed for %s value in _to_int_safe", kind, exc_info=True
            )
    return _to_int_safe(fallback_first(value), default=default)


def _to_int_safe_sequence(value: Any, default: int) -> int:
    if not value:
        return default
    return _to_int_safe_first_element(
        value, default, fallback_first=lambda v: v[0], kind="tuple/list"
    )


def _to_int_safe_pandas(value: Any, default: int) -> int:
    if len(value) == 0:
        return default
    return _to_int_safe_first_element(
        value, default, fallback_first=lambda v: v.iloc[0], kind="Series/Index"
    )


def _to_int_safe_via_item(value: Any, default: int, *, item_fn: Callable[[], Any]) -> int:
    try:
        return _to_int_safe(item_fn(), default=default)
    except Exception:
        _module_log.debug("value.item() failed in _to_int_safe", exc_info=True)
        return default


def _to_int_safe_fallback(value: Any, default: int) -> int:
    item_fn = getattr(value, "item", None)
    if callable(item_fn):
        return _to_int_safe_via_item(value, default, item_fn=item_fn)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# Ordered like the isinstance chain they replace: first matching type wins.
_TO_INT_SAFE_HANDLERS: list[tuple[type | tuple[type, ...], Callable[[Any, int], int]]] = [
    (int, _to_int_safe_int),
    (float, _to_int_safe_float),
    ((tuple, list), _to_int_safe_sequence),
    ((pd.Series, pd.Index), _to_int_safe_pandas),
]


def _to_int_safe(value: Any, default: int = 0) -> int:
    """Convert *value* to int, returning *default* on failure.

    Handles ``numpy`` scalar types gracefully when numpy is not installed.
    """
    if value is None:
        return default
    for types, handler in _TO_INT_SAFE_HANDLERS:
        if isinstance(value, types):
            return handler(value, default)
    return _to_int_safe_fallback(value, default)


def dask_is_probably_empty(obj: Any) -> bool:
    if _is_dask_collection(obj):
        return int(getattr(obj, "npartitions", 0) or 0) == 0
    try:
        return len(obj) == 0
    except Exception:
        _module_log.debug(
            "len() failed in dask_is_probably_empty, assuming non-empty", exc_info=True
        )
        return False


__all__ = [
    "RECOVERABLE_DASK_ERRORS",
    "dask_is_probably_empty",
]
