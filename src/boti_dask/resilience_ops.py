"""Synchronous Dask-safe operation execution: safe_compute/persist/wait/head/gather.

Split out of resilience.py to keep files under the length threshold. Depends
on resilience.py (for RECOVERABLE_DASK_ERRORS, _is_dask_collection,
_contains_dask_collection, _call_get_client_safely) but resilience.py depends
on nothing here, so the import direction is one-way — see resilience.py's
module docstring-equivalent comment on _module_log for why.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, NamedTuple

from ._internal import _is_dask_dataframe_like, _is_running_client, _log
from .diagnostics import inspect_graph
from .resilience import (
    RECOVERABLE_DASK_ERRORS,
    _call_get_client_safely,
    _contains_dask_collection,
    _is_dask_collection,
)
from .session import pool

try:
    from dask.distributed import wait as distributed_wait
except ImportError:

    def distributed_wait(*_args: Any, **_kwargs: Any) -> None:
        return None


def _resolve_active_client(provided_client: Any | None = None) -> Any | None:
    if _is_running_client(provided_client):
        return provided_client
    current = _call_get_client_safely()
    return current if _is_running_client(current) else None


def _translate_orphaned_graph_error(*, operation_name: str, obj: Any, exc: Exception) -> Exception:
    if (
        _is_dask_collection(obj)
        and isinstance(exc, ValueError)
        and "Missing dependency" in str(exc)
    ):
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


class _SyncCallContext(NamedTuple):
    """Bundles _execute_sync's call-shape params so the recoverable-error
    handler below doesn't need its own 9-parameter signature."""

    operation_name: str
    obj: Any
    remote_op: Callable[..., Any]
    local_op: Callable[[Any], Any]
    timeout: float | None
    logger: Any | None


def _handle_recoverable_dask_error(
    ctx: _SyncCallContext,
    *,
    active_client: Any | None,
    metrics: dict[str, Any] | None,
    exc: Exception,
) -> Any:
    _log(
        ctx.logger,
        "warning",
        f"{ctx.operation_name}: recoverable failure ({type(exc).__name__}); retrying once.",
    )
    retry_client = _retry_client(active_client)
    if retry_client is not None:
        return ctx.remote_op(ctx.obj, retry_client, timeout=ctx.timeout)
    if metrics and metrics.get("is_dask") and _is_dask_dataframe_like(ctx.obj):
        raise RuntimeError(
            "Distributed client lost and cannot be retried safely for a Dask DataFrame-like "
            "collection. Rebuild the collection before retrying."
        ) from exc
    return ctx.local_op(ctx.obj)


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
    ctx = _SyncCallContext(operation_name, obj, remote_op, local_op, timeout, logger)
    try:
        return (
            remote_op(obj, active_client, timeout=timeout)
            if active_client is not None
            else local_op(obj)
        )
    except RECOVERABLE_DASK_ERRORS as exc:
        return _handle_recoverable_dask_error(
            ctx, active_client=active_client, metrics=metrics, exc=exc
        )
    except Exception as exc:
        translated = _translate_orphaned_graph_error(
            operation_name=operation_name,
            obj=obj,
            exc=exc,
        )
        if translated is not exc:
            raise translated from exc
        raise


# Not a copy-pasted twin: async_safe_compute() is already a thin
# asyncio.to_thread() wrapper delegating to this function, so the "shared
# helper for the non-blocking parts" the finding suggests is _execute_sync
# itself, already in place. The similarity score is boilerplate parameter
# passing, not duplicated logic.
# spaghetti-ignore[sync-async-duplication]
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


# See safe_compute's comment above: async_safe_persist() is already a
# thin asyncio.to_thread() wrapper around this, not a copy-pasted twin.
# spaghetti-ignore[sync-async-duplication]
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


# See safe_compute's comment above: async_safe_wait() is already a thin
# asyncio.to_thread() wrapper around this, not a copy-pasted twin.
# spaghetti-ignore[sync-async-duplication]
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
        # Reading a marker set by SessionPool.track_persisted_collection() on
        # a third-party dask collection, not a boti class we own.
        # spaghetti-ignore[encapsulation-violation]
        if _resolve_active_client(dask_client) is None and getattr(
            obj, "_boti_managed_persisted", False
        ):
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


# Ordered like the isinstance chain they replace: first matching type wins.
_LOCAL_GATHER_CONTAINER_HANDLERS: list[tuple[type, Callable[[Any], Any]]] = [
    (dict, lambda obj: {key: _local_gather(value) for key, value in obj.items()}),
    (tuple, lambda obj: tuple(_local_gather(value) for value in obj)),
    (list, lambda obj: [_local_gather(value) for value in obj]),
]


def _local_gather(obj: Any) -> Any:
    for container_type, handler in _LOCAL_GATHER_CONTAINER_HANDLERS:
        if isinstance(obj, container_type):
            return handler(obj)
    if hasattr(obj, "compute"):
        return _sync_local_compute(obj)
    result_fn = getattr(obj, "result", None)
    return result_fn() if callable(result_fn) else obj


# See safe_compute's comment above: async_safe_gather() is already a
# thin asyncio.to_thread() wrapper around this, not a copy-pasted twin.
# spaghetti-ignore[sync-async-duplication]
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


__all__ = [
    "safe_compute",
    "safe_gather",
    "safe_head",
    "safe_persist",
    "safe_wait",
]
