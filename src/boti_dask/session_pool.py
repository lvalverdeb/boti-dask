from __future__ import annotations

import logging
import threading
import warnings
import weakref
from typing import Any

from ._internal import _is_dask_dataframe_like, _is_running_client, _log

# Module-level fallback for SessionPool methods with no caller-supplied logger.
# Debug level only — these are expected/best-effort paths.
_module_log = logging.getLogger(__name__)


class SessionPool:
    """Manages Dask client, cluster, and persisted-collection registries.

    A module-level singleton replaces the previous bare module-level dicts.
    """

    def __init__(self) -> None:
        self._managed_clients: dict[int, weakref.ReferenceType[Any]] = {}
        self._persisted_collections: dict[int, list[weakref.ReferenceType[Any]]] = {}
        self._shared_sessions: dict[str, dict[str, Any]] = {}
        self._shared_creation_locks: dict[str, threading.Lock] = {}
        self._lock = threading.RLock()

    # -- managed client helpers ------------------------------------------------

    def register_managed_client(self, client: Any) -> None:
        client_id = id(client)

        def _prune(ref: weakref.ReferenceType[Any]) -> None:
            # GC callback: drop registry entries for the collected client so
            # long-lived processes churning many clients do not accumulate
            # dead ids. The identity check guards against id() reuse by a
            # newer client registered under the same address.
            with self._lock:
                if self._managed_clients.get(client_id) is ref:
                    self._managed_clients.pop(client_id, None)
                    self._persisted_collections.pop(client_id, None)

        with self._lock:
            self._managed_clients[client_id] = weakref.ref(client, _prune)

    def unregister_managed_client(self, client: Any) -> None:
        client_id = id(client)
        with self._lock:
            self._managed_clients.pop(client_id, None)
            self._persisted_collections.pop(client_id, None)

    def is_client_registered(self, client: Any) -> bool:
        with self._lock:
            return id(client) in self._managed_clients

    # -- persisted collection tracking -----------------------------------------

    def track_persisted_collection(self, obj: Any, client: Any | None) -> None:
        if client is None or not _is_dask_dataframe_like(obj):
            return
        client_id = id(client)
        with self._lock:
            if client_id not in self._managed_clients:
                return
        try:
            # Marker attribute on a third-party dask collection, not a boti
            # class we own — underscore-prefixed deliberately so it can never
            # collide with a real dask/pandas attribute.
            # spaghetti-ignore[encapsulation-violation]
            setattr(obj, "_boti_managed_persisted", True)
        except Exception:
            _module_log.debug("Failed to mark persisted collection", exc_info=True)
        try:
            ref = weakref.ref(obj)
        except TypeError:
            return
        with self._lock:
            self._persisted_collections.setdefault(client_id, []).append(ref)

    def live_persisted_collection_count(self, client: Any) -> int:
        client_id = id(client)
        with self._lock:
            refs = self._persisted_collections.get(client_id, [])
            live = [ref for ref in refs if ref() is not None]
            if live:
                self._persisted_collections[client_id] = live
            else:
                self._persisted_collections.pop(client_id, None)
        return len(live)

    # -- shared session helpers ------------------------------------------------

    def shared_creation_lock(self, key: str) -> threading.Lock:
        """Per-key lock held by DaskSession.open() across acquire-or-create.

        Without it, two concurrent first opens of the same key both miss
        ``try_acquire_shared_session`` and both create a cluster; the second
        ``register_shared_session`` then overwrites the first entry, leaking
        the first cluster and corrupting the ref count.
        """
        with self._lock:
            lock = self._shared_creation_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._shared_creation_locks[key] = lock
            return lock

    def register_shared_session(self, key: str, *, client: Any, cluster: Any | None) -> None:
        self.register_managed_client(client)
        with self._lock:
            self._shared_sessions[key] = {
                "client": client,
                "cluster": cluster,
                "ref_count": 1,
            }

    def get_shared_session(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            entry = self._shared_sessions.get(key)
            return dict(entry) if entry is not None else None

    def try_acquire_shared_session(self, key: str) -> dict[str, Any] | None:
        """Atomically check for a shared session and increment ref_count.

        Returns a dict with ``client`` / ``cluster`` keys if acquired,
        or ``None`` if the session does not exist or the client is gone.
        """
        with self._lock:
            entry = self._shared_sessions.get(key)
            if entry is None:
                return None
            client = entry.get("client")
            if not _is_running_client(client):
                self._shared_sessions.pop(key, None)
                return None
            entry["ref_count"] = int(entry.get("ref_count", 0)) + 1
            return {"client": client, "cluster": entry.get("cluster")}

    def release_shared_session(self, key: str, *, logger: Any | None = None) -> None:
        client: Any | None = None
        cluster: Any | None = None
        with self._lock:
            entry = self._shared_sessions.get(key)
            if entry is None:
                return
            entry["ref_count"] = int(entry.get("ref_count", 0)) - 1
            if entry["ref_count"] > 0:
                _log(
                    logger,
                    "debug",
                    f"Released shared Dask session key={key!r}; ref_count={entry['ref_count']}",
                )
                return
            client = entry.get("client")
            cluster = entry.get("cluster")
            self._shared_sessions.pop(key, None)

        if client is not None:
            live = self.live_persisted_collection_count(client)
            if live:
                msg = (
                    "Closing shared Dask session with "
                    f"{live} live persisted collection(s). "
                    "Those collections will become unusable after the session closes; "
                    "compute or preview inside the shared session, or keep another shared holder open."
                )
                warnings.warn(msg, RuntimeWarning, stacklevel=3)
                _log(logger, "warning", msg)
            try:
                client.close()
            finally:
                self.unregister_managed_client(client)
        if cluster is not None:
            cluster.close()

    # -- testing helpers -------------------------------------------------------

    def debug_set_shared_ref_count(self, key: str, count: int) -> None:
        with self._lock:
            if key in self._shared_sessions:
                self._shared_sessions[key]["ref_count"] = count

    def debug_shared_session_keys(self) -> list[str]:
        with self._lock:
            return list(self._shared_sessions)


pool = SessionPool()
