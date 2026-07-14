from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from typing import Any

import pandas as pd

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]

from ._internal import _is_dask_dataframe_like, _is_running_client, _log
from .diagnostics import inspect_graph
from .session import pool

try:
    from dask.distributed import get_client
    from dask.distributed import wait as distributed_wait
except ImportError:
    get_client = None

    def distributed_wait(*_args: Any, **_kwargs: Any) -> None:
        return None

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
_module_log = logging.getLogger(__name__)


def _has_dask_graph(obj: Any) -> bool:
    return hasattr(obj, "__dask_graph__")


def _is_dask_collection(obj: Any) -> bool:
    return _is_dask_dataframe_like(obj) or _has_dask_graph(obj)


def _contains_dask_collection(obj: Any) -> bool:
    if _is_dask_collection(obj):
        return True
    if isinstance(obj, dict):
        return any(_contains_dask_collection(value) for value in obj.values())
    if isinstance(obj, (list, tuple)):
        return any(_contains_dask_collection(value) for value in obj)
    return False


def _resolve_active_client(provided_client: Any | None = None) -> Any | None:
    if _is_running_client(provided_client):
        return provided_client
    if get_client is None:
        return None
    try:
        current = get_client()
    except Exception:
        _module_log.debug("get_client() failed while resolving active Dask client", exc_info=True)
        return None
    return current if _is_running_client(current) else None


def _translate_orphaned_graph_error(*, operation_name: str, obj: Any, exc: Exception) -> Exception:
    if _is_dask_collection(obj) and isinstance(exc, ValueError) and "Missing dependency" in str(exc):
        return RuntimeError(
            f"{operation_name} failed because the Dask collection is orphaned from its original "
            "client/worker state. Compute or preview inside the owning session, or rebuild the collection."
        )
    return exc


def _sync_local_compute(obj: Any) -> Any:
    if _is_dask_dataframe_like(obj):
        raise RuntimeError(
            "Local fallback forbidden for Dask DataFrame-like objects. "
            "Attach a distributed client or rebuild the collection."
        )
    if not hasattr(obj, "compute"):
        return obj
    compute = obj.compute
    try:
        return compute(scheduler="threads")
    except TypeError:
        return compute()


def _sync_local_persist(obj: Any) -> Any:
    if _is_dask_dataframe_like(obj):
        raise RuntimeError(
            "Local fallback forbidden for Dask DataFrame-like objects. "
            "Attach a distributed client or rebuild the collection."
        )
    if not hasattr(obj, "persist"):
        return obj
    persist = obj.persist
    try:
        return persist(scheduler="threads")
    except TypeError:
        return persist()


def _sync_local_wait(obj: Any) -> Any:
    if hasattr(obj, "compute"):
        _sync_local_compute(obj)
    return obj


def _resolve_sync_result(result: Any) -> Any:
    if _is_dask_collection(result):
        return result
    result_fn = getattr(result, "result", None)
    if callable(result_fn):
        return result_fn()
    if inspect.isawaitable(result):
        raise RuntimeError("Received asynchronous Dask client result in a synchronous API.")
    return result


def _sync_client_compute(obj: Any, client: Any, *, timeout: float | None = None) -> Any:
    return _resolve_sync_result(client.compute(obj))


def _sync_client_persist(obj: Any, client: Any, *, timeout: float | None = None) -> Any:
    return _resolve_sync_result(client.persist(obj))


def _sync_client_wait(obj: Any, client: Any, *, timeout: float | None = None) -> Any:
    wait_result = distributed_wait(obj, timeout=timeout)
    if inspect.isawaitable(wait_result):
        raise RuntimeError("Received asynchronous Dask wait result in a synchronous API.")
    return obj


def _sync_client_gather(obj: Any, client: Any, *, timeout: float | None = None) -> Any:
    if _contains_dask_collection(obj):
        computed = client.compute(obj)
        return client.gather(computed)
    return client.gather(obj)


def _retry_client(current_client: Any | None) -> Any | None:
    rebound = _resolve_active_client()
    if rebound is not None:
        return rebound
    return current_client if _is_running_client(current_client) else None


def _execute_sync(
    *,
    operation_name: str,
    obj: Any,
    dask_client: Any | None,
    logger: Any | None,
    remote_op: Callable[..., Any],
    local_op: Callable[[Any], Any],
    dry_run: bool = False,
    timeout: float | None = None,
) -> Any:
    metrics = inspect_graph(obj, logger=logger) if _is_dask_collection(obj) else None
    if dry_run:
        _log(logger, "info", f"{operation_name}: dry run requested; execution skipped.")
        return None

    active_client = _resolve_active_client(dask_client)
    try:
        if active_client is not None:
            return remote_op(obj, active_client, timeout=timeout)
        return local_op(obj)
    except RECOVERABLE_DASK_ERRORS as exc:
        _log(logger, "warning", f"{operation_name}: recoverable failure ({type(exc).__name__}); retrying once.")
        retry_client = _retry_client(active_client)
        if retry_client is not None:
            return remote_op(obj, retry_client, timeout=timeout)
        if metrics and metrics.get("is_dask") and _is_dask_dataframe_like(obj):
            raise RuntimeError(
                "Distributed client lost and cannot be retried safely for a Dask DataFrame-like "
                "collection. Rebuild the collection before retrying."
            ) from exc
        return local_op(obj)
    except Exception as exc:
        translated = _translate_orphaned_graph_error(
            operation_name=operation_name,
            obj=obj,
            exc=exc,
        )
        if translated is not exc:
            raise translated from exc
        raise


def safe_compute(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return _execute_sync(
        operation_name="safe_compute",
        obj=obj,
        dask_client=dask_client,
        logger=logger,
        remote_op=_sync_client_compute,
        local_op=_sync_local_compute,
        dry_run=dry_run,
    )


def safe_persist(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
) -> Any:
    result = _execute_sync(
        operation_name="safe_persist",
        obj=obj,
        dask_client=dask_client,
        logger=logger,
        remote_op=_sync_client_persist,
        local_op=_sync_local_persist,
    )
    pool.track_persisted_collection(result, _resolve_active_client(dask_client))
    return result


def safe_wait(
    obj: Any,
    *,
    dask_client: Any | None = None,
    timeout: float | None = None,
    logger: Any | None = None,
) -> Any:
    return _execute_sync(
        operation_name="safe_wait",
        obj=obj,
        dask_client=dask_client,
        logger=logger,
        remote_op=_sync_client_wait,
        local_op=_sync_local_wait,
        timeout=timeout,
    )


def safe_head(
    obj: Any,
    *,
    n: int = 5,
    npartitions: int = 1,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    if _is_dask_dataframe_like(obj):
        if _resolve_active_client(dask_client) is None and getattr(obj, "_boti_managed_persisted", False):
            raise RuntimeError(
                "safe_head failed because the Dask collection is orphaned from its original client/worker state."
            )
        return safe_compute(
            obj.head(n=n, npartitions=npartitions, compute=False),
            dask_client=dask_client,
            logger=logger,
            dry_run=dry_run,
        )
    head = getattr(obj, "head", None)
    if callable(head):
        return head(n)
    raise TypeError(f"safe_head requires an object with head(); got {type(obj)!r}")


def _local_gather(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {key: _local_gather(value) for key, value in obj.items()}
    if isinstance(obj, tuple):
        return tuple(_local_gather(value) for value in obj)
    if isinstance(obj, list):
        return [_local_gather(value) for value in obj]
    if hasattr(obj, "compute"):
        return _sync_local_compute(obj)
    result_fn = getattr(obj, "result", None)
    if callable(result_fn):
        return result_fn()
    return obj


def safe_gather(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return _execute_sync(
        operation_name="safe_gather",
        obj=obj,
        dask_client=dask_client,
        logger=logger,
        remote_op=_sync_client_gather,
        local_op=_local_gather,
        dry_run=dry_run,
    )


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
    return _to_int_safe_first_element(value, default, fallback_first=lambda v: v[0], kind="tuple/list")


def _to_int_safe_pandas(value: Any, default: int) -> int:
    if len(value) == 0:
        return default
    return _to_int_safe_first_element(value, default, fallback_first=lambda v: v.iloc[0], kind="Series/Index")


def _to_int_safe_fallback(value: Any, default: int) -> int:
    result = getattr(value, "item", None)
    if callable(result):
        try:
            return _to_int_safe(result(), default=default)
        except Exception:
            _module_log.debug("value.item() failed in _to_int_safe", exc_info=True)
            return default
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
        _module_log.debug("len() failed in dask_is_probably_empty, assuming non-empty", exc_info=True)
        return False


def dask_is_empty(
    obj: Any,
    *,
    sample: int = 4,
    dask_client: Any | None = None,
    logger: Any | None = None,
) -> bool:
    if dask_is_probably_empty(obj):
        return True
    if not _is_dask_dataframe_like(obj):
        try:
            return len(obj) == 0
        except Exception:
            _log(logger, "debug", "len() call failed for non-DataFrame object, returning False")
            return False

    partitions = int(getattr(obj, "npartitions", 0) or 0)
    probes = min(max(sample, 1), partitions)
    try:
        for index in range(probes):
            count_expr = obj.get_partition(index).map_partitions(len, meta=("n", "int64")).sum()
            if dask_client is not None:
                count = safe_compute(count_expr, dask_client=dask_client, logger=logger)
            else:
                count = count_expr.compute(scheduler="threads")
            if _to_int_safe(count) > 0:
                return False
        if probes == partitions:
            return True
        total_expr = obj.map_partitions(len, meta=("n", "int64")).sum()
        if dask_client is not None:
            total = safe_compute(total_expr, dask_client=dask_client, logger=logger)
        else:
            total = total_expr.compute(scheduler="threads")
        return _to_int_safe(total) == 0
    except Exception as exc:
        _log(logger, "warning", f"dask_is_empty probe failed: {exc}")
        return False


async def async_safe_compute(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return await asyncio.to_thread(
        safe_compute,
        obj,
        dask_client=dask_client,
        logger=logger,
        dry_run=dry_run,
    )


async def async_safe_persist(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
) -> Any:
    return await asyncio.to_thread(
        safe_persist,
        obj,
        dask_client=dask_client,
        logger=logger,
    )


async def async_safe_wait(
    obj: Any,
    *,
    dask_client: Any | None = None,
    timeout: float | None = None,
    logger: Any | None = None,
) -> Any:
    return await asyncio.to_thread(
        safe_wait,
        obj,
        dask_client=dask_client,
        timeout=timeout,
        logger=logger,
    )


async def async_safe_head(
    obj: Any,
    *,
    n: int = 5,
    npartitions: int = 1,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return await asyncio.to_thread(
        safe_head,
        obj,
        n=n,
        npartitions=npartitions,
        dask_client=dask_client,
        logger=logger,
        dry_run=dry_run,
    )


async def async_safe_gather(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return await asyncio.to_thread(
        safe_gather,
        obj,
        dask_client=dask_client,
        logger=logger,
        dry_run=dry_run,
    )


__all__ = [
    "RECOVERABLE_DASK_ERRORS",
    "async_safe_compute",
    "async_safe_gather",
    "async_safe_head",
    "async_safe_persist",
    "async_safe_wait",
    "dask_is_empty",
    "dask_is_probably_empty",
    "inspect_graph",
    "safe_compute",
    "safe_gather",
    "safe_head",
    "safe_persist",
    "safe_wait",
]
