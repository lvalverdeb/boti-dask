from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import warnings
import weakref
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import dask
import dask.dataframe as dd

try:
    from dask.distributed import Client, LocalCluster, get_client
except ImportError:
    Client = None
    LocalCluster = None
    get_client = None

from boti.core.managed_resource import ManagedResource
from boti.core.models import ResourceConfig

_RECOMMENDED_DASK_CONFIG = {
    "distributed.comm.timeouts.connect": "20s",
    "distributed.comm.timeouts.tcp": "120s",
    "distributed.worker.memory.target": 0.6,
    "distributed.worker.memory.spill": 0.7,
    "distributed.worker.memory.pause": 0.8,
    "distributed.scheduler.allowed-failures": 3,
    "distributed.deploy.lost-worker-timeout": "60s",
    "distributed.admin.large-graph-warning-threshold": "50MB",
    "dataframe.shuffle.method": "tasks",
}

_ENV_PREFIX_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*_?$")


# ---------------------------------------------------------------------------
# SessionPool — replaces module-level registries
# ---------------------------------------------------------------------------

class SessionPool:
    """Manages Dask client, cluster, and persisted-collection registries.

    A module-level singleton replaces the previous bare module-level dicts.
    """

    def __init__(self) -> None:
        self._managed_clients: dict[int, weakref.ReferenceType[Any]] = {}
        self._persisted_collections: dict[int, list[weakref.ReferenceType[Any]]] = {}
        self._shared_sessions: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    # -- managed client helpers ------------------------------------------------

    def register_managed_client(self, client: Any) -> None:
        with self._lock:
            self._managed_clients[id(client)] = weakref.ref(client)

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
            setattr(obj, "_boti_managed_persisted", True)
        except Exception:
            pass
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
                _log(logger, "debug", f"Released shared Dask session key={key!r}; ref_count={entry['ref_count']}")
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


# ---------------------------------------------------------------------------
# Module-level helpers (shared state handled via pool)
# ---------------------------------------------------------------------------

def _is_dask_dataframe_like(obj: Any) -> bool:
    return isinstance(obj, (dd.DataFrame, dd.Series)) or hasattr(obj, "_meta")


def _log(logger: Any | None, level: str, message: str) -> None:
    if logger is None:
        return
    log_fn = getattr(logger, level, None)
    if callable(log_fn):
        log_fn(message)


def _stable_mapping_repr(value: Mapping[str, Any]) -> str:
    return repr(sorted((str(key), repr(item)) for key, item in value.items()))


def _is_running_client(client: Any | None) -> bool:
    if client is None:
        return False
    return getattr(client, "status", "running") in {"running", "started"}


def _verify_client_connection(client: Any) -> None:
    try:
        client.scheduler_info()
    except Exception as exc:
        raise RuntimeError(
            "Failed to verify Dask client connectivity. "
            "Check the scheduler address and cluster health before retrying."
        ) from exc


def _prepare_cluster_kwargs(cluster_factory: Any, cluster_kwargs: Mapping[str, Any]) -> dict[str, Any]:
    resolved = dict(cluster_kwargs)
    if cluster_factory is LocalCluster and "dashboard_address" not in resolved:
        resolved["dashboard_address"] = ":0"
    return resolved


# Backward-compat aliases — delegate to the module-level pool singleton.

def _register_managed_client(client: Any) -> None:
    pool.register_managed_client(client)


def _unregister_managed_client(client: Any) -> None:
    pool.unregister_managed_client(client)


def _track_persisted_collection(obj: Any, client: Any | None) -> None:
    pool.track_persisted_collection(obj, client)


def _live_persisted_collection_count(client: Any) -> int:
    return pool.live_persisted_collection_count(client)


def _register_shared_session(key: str, *, client: Any, cluster: Any | None) -> None:
    pool.register_shared_session(key, client=client, cluster=cluster)


def _release_shared_session(key: str, *, logger: Any | None = None) -> None:
    pool.release_shared_session(key, logger=logger)


# ---------------------------------------------------------------------------
# Public helpers (unchanged API)
# ---------------------------------------------------------------------------

def describe_client(client: Any) -> dict[str, Any]:
    try:
        info = client.scheduler_info()
    except Exception:
        info = {}
    workers = info.get("workers", {}) if isinstance(info, dict) else {}
    scheduler = getattr(getattr(client, "cluster", None), "scheduler_address", None)
    if scheduler is None:
        scheduler = getattr(getattr(client, "scheduler", None), "address", None)
    return {
        "scheduler": scheduler,
        "dashboard": getattr(client, "dashboard_link", None),
        "workers": len(workers),
        "threads": sum(worker.get("nthreads", 0) for worker in workers.values()),
    }


def current_client_summary() -> dict[str, Any] | None:
    if get_client is None:
        return None
    try:
        return describe_client(get_client())
    except Exception:
        return None


def recommended_dask_config(*, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    config = dict(_RECOMMENDED_DASK_CONFIG)
    if overrides:
        config.update(dict(overrides))
    return config


def apply_recommended_dask_config(**overrides: Any) -> Any:
    return dask.config.set(recommended_dask_config(overrides=overrides))


def _validate_env_prefix(prefix: str) -> str:
    normalized = prefix.strip()
    if not normalized or not _ENV_PREFIX_PATTERN.fullmatch(normalized):
        raise ValueError(
            "Environment prefixes must match [A-Za-z_][A-Za-z0-9_]* and may end with a single underscore."
        )
    return normalized


def _parse_env_bool(raw: str, *, field_name: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"Invalid boolean value for {field_name!r}: {raw!r}. Use one of true/false, yes/no, 1/0."
    )


def _parse_env_json_mapping(raw: str, *, field_name: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON value for {field_name!r}: {raw!r}. Provide a JSON object."
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid value for {field_name!r}: expected a JSON object.")
    return dict(parsed)


def _load_dotenv_values(env_file: str | Path | None) -> dict[str, str]:
    if env_file is None:
        return {}
    path = Path(env_file)
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"\"", "'"}:
            value = value[1:-1]
        values[key] = value
    return values


# ---------------------------------------------------------------------------
# DaskSessionSettings (unchanged)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DaskSessionSettings:
    scheduler_address: str | None = None
    shared: bool = False
    shared_key: str | None = None
    verify_connectivity: bool = False
    cluster_kwargs: dict[str, Any] = field(default_factory=dict)
    client_kwargs: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env_prefix(
        cls,
        prefix: str,
        *,
        env_file: str | Path | None = None,
    ) -> DaskSessionSettings:
        normalized_prefix = _validate_env_prefix(prefix)
        merged = _load_dotenv_values(env_file)
        merged.update({k: v for k, v in os.environ.items() if isinstance(v, str)})

        scheduler_address = merged.get(f"{normalized_prefix}SCHEDULER_ADDRESS")
        shared_raw = merged.get(f"{normalized_prefix}SHARED")
        shared_key = merged.get(f"{normalized_prefix}SHARED_KEY")
        verify_raw = merged.get(f"{normalized_prefix}VERIFY_CONNECTIVITY")
        cluster_kwargs_raw = merged.get(f"{normalized_prefix}CLUSTER_KWARGS")
        client_kwargs_raw = merged.get(f"{normalized_prefix}CLIENT_KWARGS")

        return cls(
            scheduler_address=scheduler_address or None,
            shared=False if shared_raw is None else _parse_env_bool(shared_raw, field_name="shared"),
            shared_key=shared_key or None,
            verify_connectivity=False
            if verify_raw is None
            else _parse_env_bool(verify_raw, field_name="verify_connectivity"),
            cluster_kwargs={}
            if cluster_kwargs_raw is None
            else _parse_env_json_mapping(cluster_kwargs_raw, field_name="cluster_kwargs"),
            client_kwargs={}
            if client_kwargs_raw is None
            else _parse_env_json_mapping(client_kwargs_raw, field_name="client_kwargs"),
        )

    def to_session_kwargs(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "shared": self.shared,
            "verify_connectivity": self.verify_connectivity,
            "cluster_kwargs": dict(self.cluster_kwargs),
            "client_kwargs": dict(self.client_kwargs),
        }
        if self.scheduler_address:
            payload["scheduler_address"] = self.scheduler_address
        if self.shared_key:
            payload["shared_key"] = self.shared_key
        return payload


# ---------------------------------------------------------------------------
# DaskSession — ManagedResource subclass
# ---------------------------------------------------------------------------

class DaskSession(ManagedResource):
    """A Dask distributed session backed by a :class:`ManagedResource` lifecycle.

    Accepts an existing client, connects to a scheduler address, or creates a
    local cluster.  Supports shared (multi-holder) and managed (single-holder)
    session patterns.
    """

    def __init__(
        self,
        client: Any | None = None,
        scheduler_address: str | None = None,
        cluster_factory: Callable[..., Any] | None = None,
        cluster_kwargs: Mapping[str, Any] | None = None,
        client_kwargs: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        shared: bool = False,
        shared_key: str | None = None,
        verify_connectivity: bool = False,
        config: ResourceConfig | None = None,
        verbose: bool = False,
        debug: bool = False,
    ) -> None:
        if config is None:
            config = ResourceConfig(
                logger=logger,
                verbose=verbose,
                debug=debug,
                skip_logger=logger is None,
            )
        elif logger is not None:
            config.logger = logger
            config.skip_logger = False

        super().__init__(config=config)

        self.client = client
        self.scheduler_address = scheduler_address
        self.cluster_factory = cluster_factory
        self.cluster_kwargs = dict(cluster_kwargs or {})
        self.client_kwargs = dict(client_kwargs or {})
        self.shared = shared
        self.shared_key = shared_key
        self.verify_connectivity = verify_connectivity

        # Runtime state
        self._cluster: Any | None = None
        self._owns_client: bool = False
        self._owns_cluster: bool = False
        self._shared_session_key: str | None = None

    # -- context manager (overrides ManagedResource @final to return client) --
    # The @final decorator on ManagedResource.__enter__ is a type-checker hint
    # only — not enforced at runtime.  We override to preserve the existing
    # ``with session as client`` pattern.

    def __enter__(self) -> Any:  # type: ignore[override]
        self._assert_open()
        return self.open()

    async def __aenter__(self) -> Any:  # type: ignore[override]
        self._assert_open()
        return await asyncio.to_thread(self.open)

    # -- internal helpers ------------------------------------------------------

    def _shared_key(self) -> str:
        if self.shared_key is not None:
            return self.shared_key
        scheduler = self.scheduler_address
        if scheduler is not None:
            return f"scheduler:{scheduler}|client={_stable_mapping_repr(self.client_kwargs)}"
        factory = self.cluster_factory or LocalCluster
        factory_name = (
            f"{getattr(factory, '__module__', type(factory).__module__)}."
            f"{getattr(factory, '__qualname__', getattr(factory, '__name__', type(factory).__name__))}"
        )
        return (
            f"cluster:{factory_name}|cluster={_stable_mapping_repr(self.cluster_kwargs)}|"
            f"client={_stable_mapping_repr(self.client_kwargs)}"
        )

    def _verify_client_if_requested(self, client: Any) -> None:
        if not self.verify_connectivity:
            return
        _verify_client_connection(client)
        if self.logger is not None:
            self.logger.info(f"Verified Dask client connectivity {describe_client(client)}")

    # -- open / cleanup --------------------------------------------------------

    def open(self) -> Any:
        if self.client is not None:
            self._verify_client_if_requested(self.client)
            if self.logger is not None:
                self.logger.debug(f"Using external Dask client {describe_client(self.client)}")
            return self.client

        if Client is None:
            raise RuntimeError("dask.distributed is required for DaskSession.")

        if self.shared:
            session_key = self._shared_key()
            acquired = pool.try_acquire_shared_session(session_key)
            if acquired is not None:
                client = acquired["client"]
                cluster = acquired["cluster"]
                self.client = client
                self._cluster = cluster
                self._shared_session_key = session_key
                try:
                    self._verify_client_if_requested(client)
                except Exception:
                    pool.release_shared_session(session_key, logger=self.logger)
                    self.client = None
                    self._cluster = None
                    self._shared_session_key = None
                    raise
                if self.logger is not None:
                    self.logger.info(f"Reusing shared Dask session {describe_client(client)} key={session_key!r}")
                return client

        if self.scheduler_address is not None:
            client = Client(self.scheduler_address, **dict(self.client_kwargs))
            if self.shared:
                pool.register_shared_session(self._shared_key(), client=client, cluster=None)
                self.client = client
                self._shared_session_key = self._shared_key()
            else:
                self.client = client
                self._owns_client = True
                pool.register_managed_client(client)
            try:
                self._verify_client_if_requested(client)
            except Exception:
                self.close()
                raise
            if self.logger is not None:
                if self.shared:
                    self.logger.info(f"Connected shared Dask client to {describe_client(client)}")
                else:
                    self.logger.info(f"Connected Dask client to {describe_client(client)}")
            return client

        cluster_factory = self.cluster_factory or LocalCluster
        if cluster_factory is None:
            raise RuntimeError("LocalCluster is unavailable. Install dask[distributed].")

        resolved_cluster_kwargs = _prepare_cluster_kwargs(cluster_factory, self.cluster_kwargs)
        cluster = cluster_factory(**resolved_cluster_kwargs)
        try:
            client = Client(cluster, **dict(self.client_kwargs))
        except Exception:
            cluster.close()
            raise

        if self.shared:
            pool.register_shared_session(self._shared_key(), client=client, cluster=cluster)
            self.client = client
            self._cluster = cluster
            self._shared_session_key = self._shared_key()
        else:
            self._cluster = cluster
            self._owns_cluster = True
            self.client = client
            self._owns_client = True
            pool.register_managed_client(client)

        try:
            self._verify_client_if_requested(client)
        except Exception:
            self.close()
            raise

        if self.logger is not None:
            if self.shared:
                self.logger.info(f"Started shared Dask session {describe_client(client)}")
            else:
                self.logger.info(f"Started managed Dask session {describe_client(client)}")
        return client

    def _cleanup(self) -> None:
        """Release Dask resources. Called by :meth:`ManagedResource.close`."""

        # Shared session: decrement ref count; only close when it hits zero.
        if self._shared_session_key is not None and self.client is not None:
            try:
                pool.release_shared_session(self._shared_session_key, logger=self.logger)
            finally:
                self.client = None
                self._cluster = None
                self._shared_session_key = None
                self._owns_client = False
                self._owns_cluster = False
            return

        # Managed session: close client + cluster.
        try:
            if self._owns_client and self.client is not None:
                live = pool.live_persisted_collection_count(self.client)
                if live:
                    msg = (
                        "Closing managed Dask session with "
                        f"{live} live persisted collection(s). "
                        "Those collections will become unusable after the session closes; "
                        "compute or preview them inside the session, or keep the client open. "
                        "Later head()/compute() calls may fail with Missing dependency."
                    )
                    warnings.warn(msg, RuntimeWarning, stacklevel=2)
                    if self.logger is not None:
                        self.logger.warning(msg)
                self.client.close()
        finally:
            if self.client is not None:
                pool.unregister_managed_client(self.client)
            self.client = None
            self._owns_client = False
            if self._owns_cluster and self._cluster is not None:
                self._cluster.close()
            self._cluster = None
            self._owns_cluster = False

    async def _acleanup(self) -> None:
        await asyncio.to_thread(self._cleanup)

    # -- factory helpers -------------------------------------------------------

    @classmethod
    def from_env_prefix(
        cls,
        prefix: str,
        *,
        env_file: str | Path | None = None,
        **overrides: Any,
    ) -> DaskSession:
        settings = DaskSessionSettings.from_env_prefix(prefix, env_file=env_file)
        payload = settings.to_session_kwargs()
        payload.update(overrides)
        return cls(**payload)


def dask_session_from_env_prefix(
    prefix: str,
    *,
    env_file: str | Path | None = None,
    **overrides: Any,
) -> DaskSession:
    return DaskSession.from_env_prefix(prefix, env_file=env_file, **overrides)


def dask_session(**kwargs: Any) -> DaskSession:
    return DaskSession(**kwargs)


__all__ = [
    "DaskSession",
    "DaskSessionSettings",
    "SessionPool",
    "apply_recommended_dask_config",
    "current_client_summary",
    "dask_session",
    "dask_session_from_env_prefix",
    "describe_client",
    "pool",
    "recommended_dask_config",
]
