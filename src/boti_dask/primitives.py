"""Cluster-wide coordination primitives with process-local fallbacks.

``cluster_semaphore``, ``cluster_lock`` and ``cluster_queue`` return the
``distributed`` primitive of the same name when a running client is available
(passed in, or discovered with ``distributed.get_client()`` -- which also works
inside a task on a worker), and a ``threading`` / ``queue`` equivalent of the
same name otherwise. Either way the object is shared by name: two callers
asking for ``cluster_lock("etl")`` get the same lock, cluster-wide or
process-wide. Callers write one code path and never import ``distributed``.

A client passed in explicitly must be running; a closed one is an error, not a
quiet fall back to the process. ``namespace`` (for example
``DaskSession.namespace``) is prefixed into the name so two sessions on one
scheduler never share a primitive by accident.

Cluster primitives obtained through a client are tracked on the session pool
and closed when that client is closed.
"""

from __future__ import annotations

import logging
import queue
import re
import threading
from typing import Any

from ._internal import _is_running_client
from .session_pool import pool

_NAMESPACE = "boti-dask"
_module_log = logging.getLogger(__name__)
# distributed refuses to re-create a named semaphore with a different limit:
# "Inconsistent max leases: 8, expected: 4". The first creator's limit wins.
_INCONSISTENT_LEASES = re.compile(r"Inconsistent max leases: \d+, expected: (\d+)")

_local_registry: dict[tuple[str, str], Any] = {}
_local_registry_lock = threading.Lock()


def _resolve_client(client: Any | None) -> Any | None:
    """A running client: the one given, else the ambient one, else None.

    A client that was passed in but is not running raises: the caller asked for
    cluster-wide coordination and a process-local primitive would not give it.
    """
    if client is not None:
        if not _is_running_client(client):
            raise RuntimeError(
                "The client passed to a cluster primitive is not running "
                f"(status={getattr(client, 'status', None)!r}); refusing to fall back "
                "to a process-local primitive."
            )
        return client
    try:
        from distributed import get_client
    except ImportError:
        return None
    try:
        ambient = get_client()
    except ValueError:
        return None
    return ambient if _is_running_client(ambient) else None


def _local(
    kind: str, name: str, factory: Any, *, limit: int | None = None, strict: bool = True
) -> Any:
    """The process-local primitive registered as (*kind*, *name*), created on first use.

    *limit* is remembered with the primitive; asking again with another limit
    is a mismatch (see :func:`_limit_mismatch`).
    """
    with _local_registry_lock:
        key = (kind, name)
        found = _local_registry.get(key)
        if found is None:
            found = (factory(), limit)
            _local_registry[key] = found
        elif limit is not None and found[1] != limit:
            _limit_mismatch(name, requested=limit, existing=found[1], strict=strict)
        return found[0]


def _limit_mismatch(name: str, *, requested: int, existing: int, strict: bool) -> None:
    """A semaphore already exists under *name* with another limit: raise, or warn and adopt."""
    message = (
        f"cluster_semaphore({name!r}): limit {requested} requested but {existing} is "
        "already set; the first creator's limit wins"
    )
    if strict:
        raise ValueError(f"{message}. Pass strict=False to adopt the existing limit.")
    _module_log.warning(message)


def _qualified(name: str, namespace: str | None = None) -> str:
    for label, value in (("names", name), ("namespaces", namespace)):
        if value is None:
            continue
        if not value or any(ch.isspace() for ch in value):
            raise ValueError(f"Primitive {label} must be non-empty and contain no whitespace.")
    return f"{_NAMESPACE}:{name}" if namespace is None else f"{_NAMESPACE}:{namespace}:{name}"


def cluster_semaphore(
    name: str,
    max_leases: int = 1,
    *,
    client: Any | None = None,
    namespace: str | None = None,
    strict: bool = True,
) -> Any:
    """A semaphore shared by *name* across the cluster, or across this process.

    Returns ``distributed.Semaphore`` (acquire/release/context manager,
    ``acquire(timeout=...)``) or ``threading.BoundedSemaphore``. A semaphore
    that already exists under *name* with a different limit is a
    ``ValueError``; with ``strict=False`` the existing limit is adopted and a
    warning logged.
    """
    if max_leases < 1:
        raise ValueError("max_leases must be at least 1.")
    qualified = _qualified(name, namespace)
    live = _resolve_client(client)
    if live is None:
        return _local(
            "semaphore",
            qualified,
            lambda: threading.BoundedSemaphore(max_leases),
            limit=max_leases,
            strict=strict,
        )
    primitive = _cluster_semaphore(name, qualified, max_leases, strict=strict)
    pool.track_primitive(live, primitive)
    return primitive


def _cluster_semaphore(name: str, qualified: str, max_leases: int, *, strict: bool) -> Any:
    from distributed import Semaphore

    try:
        primitive = Semaphore(max_leases=max_leases, name=qualified)
        # Registration is otherwise deferred to the first acquire; do it now so a
        # limit conflict surfaces here, where it can be resolved, not mid-task.
        primitive.register()
    except ValueError as exc:
        match = _INCONSISTENT_LEASES.search(str(exc))
        if match is None:
            raise
        existing = int(match.group(1))
        _limit_mismatch(name, requested=max_leases, existing=existing, strict=strict)
        primitive = Semaphore(max_leases=existing, name=qualified)
        primitive.register()
    return primitive


def cluster_lock(name: str, *, client: Any | None = None, namespace: str | None = None) -> Any:
    """A lock shared by *name* across the cluster, or across this process."""
    qualified = _qualified(name, namespace)
    live = _resolve_client(client)
    if live is None:
        return _local("lock", qualified, threading.Lock)
    from distributed import Lock

    primitive = Lock(name=qualified)
    pool.track_primitive(live, primitive)
    return primitive


def cluster_queue(
    name: str, *, maxsize: int = 0, client: Any | None = None, namespace: str | None = None
) -> Any:
    """A FIFO queue shared by *name* across the cluster, or across this process.

    Both forms expose ``put(item, timeout=...)`` and ``get(timeout=...)``.
    """
    qualified = _qualified(name, namespace)
    live = _resolve_client(client)
    if live is None:
        return _local("queue", qualified, lambda: queue.Queue(maxsize=maxsize))
    from distributed import Queue

    primitive = Queue(name=qualified, client=live, maxsize=maxsize)
    pool.track_primitive(live, primitive)
    return primitive


def reset_local_primitives() -> None:
    """Forget every process-local primitive (test isolation)."""
    with _local_registry_lock:
        _local_registry.clear()


__all__ = ["cluster_lock", "cluster_queue", "cluster_semaphore", "reset_local_primitives"]
