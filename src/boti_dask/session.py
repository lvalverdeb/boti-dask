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
except ImportError:  # pragma: no cover
    Client = None  # type: ignore[assignment]
    LocalCluster = None  # type: ignore[assignment]
    get_client = None  # type: ignore[assignment]


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

_MANAGED_CLIENT_REGISTRY: dict[int, weakref.ReferenceType[Any]] = {}
_PERSISTED_COLLECTION_REGISTRY: dict[int, list[weakref.ReferenceType[Any]]] = {}
_SHARED_SESSION_REGISTRY: dict[str, dict[str, Any]] = {}
_REGISTRY_LOCK = threading.RLock()

_ENV_PREFIX_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*_?$")


# Internal helpers are intentionally module-level so resilience helpers can share state.
def _register_managed_client(client: Any) -> None:
    with _REGISTRY_LOCK:
        _MANAGED_CLIENT_REGISTRY[id(client)] = weakref.ref(client)


def _unregister_managed_client(client: Any) -> None:
    client_id = id(client)
    with _REGISTRY_LOCK:
        _MANAGED_CLIENT_REGISTRY.pop(client_id, None)
        _PERSISTED_COLLECTION_REGISTRY.pop(client_id, None)


def _is_dask_dataframe_like(obj: Any) -> bool:
    return isinstance(obj, (dd.DataFrame, dd.Series)) or hasattr(obj, "_meta")


def _track_persisted_collection(obj: Any, client: Any | None) -> None:
    if client is None or not _is_dask_dataframe_like(obj):
        return
    client_id = id(client)
    with _REGISTRY_LOCK:
        if client_id not in _MANAGED_CLIENT_REGISTRY:
            return
    try:
        setattr(obj, "_boti_managed_persisted", True)
    except Exception:
        pass
    try:
        ref = weakref.ref(obj)
    except TypeError:
        return
    with _REGISTRY_LOCK:
        _PERSISTED_COLLECTION_REGISTRY.setdefault(client_id, []).append(ref)


def _live_persisted_collection_count(client: Any) -> int:
    client_id = id(client)
    with _REGISTRY_LOCK:
        refs = _PERSISTED_COLLECTION_REGISTRY.get(client_id, [])
        live_refs = [ref for ref in refs if ref() is not None]
        if live_refs:
            _PERSISTED_COLLECTION_REGISTRY[client_id] = live_refs
        else:
            _PERSISTED_COLLECTION_REGISTRY.pop(client_id, None)
    return len(live_refs)


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


def _register_shared_session(key: str, *, client: Any, cluster: Any | None) -> None:
    _register_managed_client(client)
    with _REGISTRY_LOCK:
        _SHARED_SESSION_REGISTRY[key] = {
            "client": client,
            "cluster": cluster,
            "ref_count": 1,
        }


def _release_shared_session(key: str, *, logger: Any | None = None) -> None:
    client: Any | None = None
    cluster: Any | None = None
    with _REGISTRY_LOCK:
        entry = _SHARED_SESSION_REGISTRY.get(key)
        if entry is None:
            return
        entry["ref_count"] = int(entry.get("ref_count", 0)) - 1
        if entry["ref_count"] > 0:
            _log(logger, "debug", f"Released shared Dask session key={key!r}; ref_count={entry['ref_count']}")
            return
        client = entry.get("client")
        cluster = entry.get("cluster")
        _SHARED_SESSION_REGISTRY.pop(key, None)

    if client is not None:
        live_collections = _live_persisted_collection_count(client)
        if live_collections:
            message = (
                "Closing shared Dask session with "
                f"{live_collections} live persisted collection(s). "
                "Those collections will become unusable after the session closes; "
                "compute or preview inside the shared session, or keep another shared holder open."
            )
            warnings.warn(message, RuntimeWarning, stacklevel=3)
            _log(logger, "warning", message)
        try:
            client.close()
        finally:
            _unregister_managed_client(client)
    if cluster is not None:
        cluster.close()


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


@dataclass(slots=True)
class DaskSession:
    client: Any | None = None
    scheduler_address: str | None = None
    cluster_factory: Callable[..., Any] | None = None
    cluster_kwargs: Mapping[str, Any] = field(default_factory=dict)
    client_kwargs: Mapping[str, Any] = field(default_factory=dict)
    logger: Any | None = None
    shared: bool = False
    shared_key: str | None = None
    verify_connectivity: bool = False

    _cluster: Any | None = field(init=False, default=None)
    _owns_client: bool = field(init=False, default=False)
    _owns_cluster: bool = field(init=False, default=False)
    _shared_session_key: str | None = field(init=False, default=None)

    def __enter__(self) -> Any:
        return self.open()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    async def __aenter__(self) -> Any:
        return await asyncio.to_thread(self.open)

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    def _log(self, level: str, message: str) -> None:
        _log(self.logger, level, message)

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
        self._log("info", f"Verified Dask client connectivity {describe_client(client)}")

    def open(self) -> Any:
        if self.client is not None:
            self._verify_client_if_requested(self.client)
            self._log("debug", f"Using external Dask client {describe_client(self.client)}")
            return self.client

        if Client is None:
            raise RuntimeError("dask.distributed is required for DaskSession.")

        if self.shared:
            session_key = self._shared_key()
            with _REGISTRY_LOCK:
                shared_entry = _SHARED_SESSION_REGISTRY.get(session_key)
                if shared_entry is not None:
                    client = shared_entry.get("client")
                    if _is_running_client(client):
                        shared_entry["ref_count"] = int(shared_entry.get("ref_count", 0)) + 1
                        cluster = shared_entry.get("cluster")
                    else:
                        _SHARED_SESSION_REGISTRY.pop(session_key, None)
                        client = None
                        cluster = None
                else:
                    client = None
                    cluster = None

            if client is not None:
                self.client = client
                self._cluster = cluster
                self._shared_session_key = session_key
                self._verify_client_if_requested(client)
                self._log("info", f"Reusing shared Dask session {describe_client(client)} key={session_key!r}")
                return client

        if self.scheduler_address is not None:
            client = Client(self.scheduler_address, **dict(self.client_kwargs))
            self._verify_client_if_requested(client)
            if self.shared:
                _register_shared_session(self._shared_key(), client=client, cluster=None)
                self.client = client
                self._shared_session_key = self._shared_key()
                self._log("info", f"Connected shared Dask client to {describe_client(client)}")
                return client
            self.client = client
            self._owns_client = True
            _register_managed_client(client)
            self._log("info", f"Connected Dask client to {describe_client(client)}")
            return client

        cluster_factory = self.cluster_factory or LocalCluster
        if cluster_factory is None:
            raise RuntimeError("LocalCluster is unavailable. Install dask[distributed].")

        cluster = cluster_factory(**dict(self.cluster_kwargs))
        try:
            client = Client(cluster, **dict(self.client_kwargs))
            self._verify_client_if_requested(client)
        except Exception:
            cluster.close()
            raise

        if self.shared:
            _register_shared_session(self._shared_key(), client=client, cluster=cluster)
            self.client = client
            self._cluster = cluster
            self._shared_session_key = self._shared_key()
            self._log("info", f"Started shared Dask session {describe_client(client)}")
            return client

        self._cluster = cluster
        self._owns_cluster = True
        self.client = client
        self._owns_client = True
        _register_managed_client(client)
        self._log("info", f"Started managed Dask session {describe_client(client)}")
        return client

    def close(self) -> None:
        if self._shared_session_key is not None and self.client is not None:
            try:
                _release_shared_session(self._shared_session_key, logger=self.logger)
            finally:
                self.client = None
                self._cluster = None
                self._shared_session_key = None
                self._owns_client = False
                self._owns_cluster = False
            return

        try:
            if self._owns_client and self.client is not None:
                live_collections = _live_persisted_collection_count(self.client)
                if live_collections:
                    message = (
                        "Closing managed Dask session with "
                        f"{live_collections} live persisted collection(s). "
                        "Those collections will become unusable after the session closes; "
                        "compute or preview them inside the session, or keep the client open. "
                        "Later head()/compute() calls may fail with Missing dependency."
                    )
                    warnings.warn(message, RuntimeWarning, stacklevel=2)
                    self._log("warning", message)
                self.client.close()
        finally:
            if self.client is not None:
                _unregister_managed_client(self.client)
            self.client = None
            self._owns_client = False
            if self._owns_cluster and self._cluster is not None:
                self._cluster.close()
            self._cluster = None
            self._owns_cluster = False

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

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
    "apply_recommended_dask_config",
    "current_client_summary",
    "dask_session",
    "dask_session_from_env_prefix",
    "describe_client",
    "recommended_dask_config",
]

