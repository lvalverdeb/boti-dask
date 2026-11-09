"""Cluster-wide coordination primitives with process-local fallbacks.

``cluster_semaphore``, ``cluster_lock`` and ``cluster_queue`` return the
``distributed`` primitive of the same name when a running client is available
(passed in, or discovered with ``distributed.get_client()`` -- which also works
inside a task on a worker), and a ``threading`` / ``queue`` equivalent of the
same name otherwise. Either way the object is shared by name: two callers
asking for ``cluster_lock("etl")`` get the same lock, cluster-wide or
process-wide. Callers write one code path and never import ``distributed``.

Cluster primitives obtained through a client are tracked on the session pool
and closed when that client is closed.
"""

from __future__ import annotations

import queue
import threading
from typing import Any

from ._internal import _is_running_client
from .session_pool import pool

_NAMESPACE = "boti-dask"

_local_registry: dict[tuple[str, str], Any] = {}
_local_registry_lock = threading.Lock()


def _resolve_client(client: Any | None) -> Any | None:
    """A running client: the one given, else the ambient one, else None."""
    if client is not None:
        return client if _is_running_client(client) else None
    try:
        from distributed import get_client
    except ImportError:
        return None
    try:
        ambient = get_client()
    except ValueError:
        return None
    return ambient if _is_running_client(ambient) else None


def _local(kind: str, name: str, factory: Any) -> Any:
    with _local_registry_lock:
        key = (kind, name)
        primitive = _local_registry.get(key)
        if primitive is None:
            primitive = factory()
            _local_registry[key] = primitive
        return primitive


def _qualified(name: str) -> str:
    if not name or any(ch.isspace() for ch in name):
        raise ValueError("Primitive names must be non-empty and contain no whitespace.")
    return f"{_NAMESPACE}:{name}"


def cluster_semaphore(name: str, max_leases: int = 1, *, client: Any | None = None) -> Any:
    """A semaphore shared by *name* across the cluster, or across this process.

    Returns ``distributed.Semaphore`` (acquire/release/context manager,
    ``acquire(timeout=...)``) or ``threading.BoundedSemaphore``.
    """
    if max_leases < 1:
        raise ValueError("max_leases must be at least 1.")
    qualified = _qualified(name)
    live = _resolve_client(client)
    if live is None:
        return _local("semaphore", qualified, lambda: threading.BoundedSemaphore(max_leases))
    from distributed import Semaphore

    primitive = Semaphore(max_leases=max_leases, name=qualified)
    pool.track_primitive(live, primitive)
    return primitive


def cluster_lock(name: str, *, client: Any | None = None) -> Any:
    """A lock shared by *name* across the cluster, or across this process."""
    qualified = _qualified(name)
    live = _resolve_client(client)
    if live is None:
        return _local("lock", qualified, threading.Lock)
    from distributed import Lock

    primitive = Lock(name=qualified)
    pool.track_primitive(live, primitive)
    return primitive


def cluster_queue(name: str, *, maxsize: int = 0, client: Any | None = None) -> Any:
    """A FIFO queue shared by *name* across the cluster, or across this process.

    Both forms expose ``put(item, timeout=...)`` and ``get(timeout=...)``.
    """
    qualified = _qualified(name)
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
