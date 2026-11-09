from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

try:
    from dask.distributed import Client, LocalCluster, get_client
except ImportError:
    Client = None
    LocalCluster = None
    get_client = None

from boti.core.managed_resource import ManagedResource
from boti.core.models import ResourceConfig

from .session_helpers import (
    ConnectivityCheck,
    DaskSessionUnavailable,
    _derived_shared_key,
    _open_client,
    _prepare_cluster_kwargs,
    _resolve_connectivity,
    _verify_client_connection,
    apply_recommended_dask_config,
    recommended_dask_config,
)
from .session_pool import SessionPool, pool
from .session_rebind import SessionRebindMixin, _close_owned_connection
from .session_settings import DaskSessionSettings
from .watchdog import WatchdogConfig

# Module-level fallback for free functions with no caller-supplied logger
# (debug level only). Kept here because tests scope caplog to the
# "boti_dask.session" logger and monkeypatch get_client via this module.
_module_log = logging.getLogger(__name__)
_NAMESPACE_DIGEST_CHARS = 16


def describe_client(client: Any) -> dict[str, Any]:
    try:
        info = client.scheduler_info()
    except Exception:
        _module_log.debug("client.scheduler_info() failed in describe_client", exc_info=True)
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
        _module_log.debug(
            "describe_client(get_client()) failed in current_client_summary", exc_info=True
        )
        return None


# ---------------------------------------------------------------------------
# DaskSession — ManagedResource subclass
# ---------------------------------------------------------------------------


class DaskSession(SessionRebindMixin, ManagedResource):
    """A Dask distributed session backed by a :class:`ManagedResource` lifecycle.

    Accepts an existing client, connects to a scheduler address, or creates a
    cluster. Supports shared (multi-holder) and managed (single-holder) sessions.

    ``verify_connectivity`` (default on) pings the scheduler, waits for a worker
    and runs one task within :class:`ConnectivityCheck.timeout`; failure raises
    :class:`DaskSessionUnavailable`. ``telemetry`` becomes the process provider
    on open (:mod:`boti_dask.telemetry`). ``upload`` ships code to the workers
    on open and after a rebind (:mod:`boti_dask.upload`). ``watchdog`` keeps
    probing after open and calls :meth:`rebind` after repeated failures
    (:mod:`boti_dask.watchdog`); it applies to clients the session creates.
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
        verify_connectivity: bool | ConnectivityCheck = True,
        watchdog: WatchdogConfig | None = None,
        telemetry: Any | None = None,
        upload: Sequence[str | Path] | None = None,
        config: ResourceConfig | None = None,
        verbose: bool = False,
        debug: bool = False,
    ) -> None:
        super().__init__(config=_resource_config(config, logger, verbose=verbose, debug=debug))

        self.client = client
        self.scheduler_address = scheduler_address
        self.cluster_factory = cluster_factory
        self.cluster_kwargs = dict(cluster_kwargs or {})
        self.client_kwargs = dict(client_kwargs or {})
        self.shared = shared
        self.shared_key = shared_key
        self.connectivity: ConnectivityCheck | None = _resolve_connectivity(verify_connectivity)
        self.watchdog_config = watchdog
        self.telemetry = telemetry
        self.upload = list(upload or [])

        # Runtime state
        self._watchdog = None
        self._cluster: Any | None = None
        self._owns_client: bool = False
        self._owns_cluster: bool = False
        self._shared_session_key: str | None = None
        self._rebinding: bool = False

    # -- context manager: returns the client, not the session. __enter__ is
    # overridable for session-style resources; __exit__ stays final.

    def __enter__(self) -> Any:
        self._assert_open()
        return self.open()

    async def __aenter__(self) -> Any:
        self._assert_open()
        return await asyncio.to_thread(self.open)

    # -- internal helpers ------------------------------------------------------

    def _shared_key(self) -> str:
        if self.shared_key is not None:
            return self.shared_key
        return _derived_shared_key(
            self.scheduler_address,
            self.cluster_factory or LocalCluster,
            self.cluster_kwargs,
            self.client_kwargs,
        )

    @property
    def namespace(self) -> str:
        """Prefix for this session's cluster primitives (:mod:`boti_dask.primitives`).

        The explicit ``shared_key`` when given; otherwise a digest of the derived
        pool key for a shared session, or of this object's identity for a
        managed one, so two sessions never share a primitive by accident.
        """
        if self.shared_key is not None:
            return self.shared_key
        seed = self._shared_key() if self.shared else f"managed:{id(self):x}"
        return hashlib.sha256(seed.encode()).hexdigest()[:_NAMESPACE_DIGEST_CHARS]

    def _verify_client_if_requested(self, client: Any) -> None:
        if self.connectivity is None:
            return
        _verify_client_connection(client, self.connectivity)
        if self.logger is not None:
            self.logger.info("Verified Dask client connectivity %s", describe_client(client))

    # -- open / cleanup --------------------------------------------------------

    def open(self) -> Any:
        self._assert_open()
        if self.client is not None:
            self._verify_client_if_requested(self.client)
            if self.logger is not None:
                self.logger.debug("Using external Dask client %s", describe_client(self.client))
            return self.client

        if Client is None:
            raise RuntimeError("dask.distributed is required for DaskSession.")

        if self.shared:
            # Serialise acquire-or-create per key: without this lock two
            # concurrent first opens both miss try_acquire and both create a
            # cluster, and the second register overwrites the first pool
            # entry — leaking a cluster and corrupting the ref count.
            with pool.shared_creation_lock(self._shared_key()):
                client = self._open_connection()
        else:
            client = self._open_connection()
        self._install_telemetry()
        self._sync_uploads()
        self._start_watchdog()
        return client

    def _open_connection(self) -> Any:
        if self.shared:
            reused = self._reuse_shared_session()
            if reused is not None:
                return reused

        if self.scheduler_address is not None:
            return self._connect_to_scheduler()

        return self._create_local_cluster_session()

    def _verify_or_abort(self, client: Any) -> None:
        """Run connectivity verification if requested; on failure, close() and re-raise.

        During a rebind the session stays open: the caller discards the
        rejected connection and keeps the previous client.
        """
        try:
            self._verify_client_if_requested(client)
        except Exception:
            if not self._rebinding:
                self.close()
            raise

    def _reuse_shared_session(self) -> Any | None:
        """Reuse the existing shared session for this key, if one is live; else None."""
        session_key = self._shared_key()
        acquired = pool.try_acquire_shared_session(session_key, holder=self)
        if acquired is None:
            return None
        client = acquired["client"]
        self.client = client
        self._cluster = acquired["cluster"]
        self._shared_session_key = session_key
        self._verify_or_abort(client)
        if self.logger is not None:
            self.logger.info(
                "Reusing shared Dask session %s key=%r", describe_client(client), session_key
            )
        return client

    def _connect_to_scheduler(self) -> Any:
        """Connect directly to an existing scheduler address."""
        client = _open_client(Client, self.scheduler_address, self.client_kwargs, self.connectivity)
        if self.shared:
            pool.register_shared_session(
                self._shared_key(), client=client, cluster=None, holder=self
            )
            self.client = client
            self._shared_session_key = self._shared_key()
        else:
            self.client = client
            self._owns_client = True
            pool.register_managed_client(client)

        self._verify_or_abort(client)
        if self.logger is not None:
            verb = (
                "Connected shared Dask client to %s"
                if self.shared
                else "Connected Dask client to %s"
            )
            self.logger.info(verb, describe_client(client))
        return client

    def _create_local_cluster_session(self) -> Any:
        """Create a new cluster (via cluster_factory, defaulting to LocalCluster) and connect to it."""
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
            pool.register_shared_session(
                self._shared_key(), client=client, cluster=cluster, holder=self
            )
            self.client = client
            self._cluster = cluster
            self._shared_session_key = self._shared_key()
        else:
            self._cluster = cluster
            self._owns_cluster = True
            self.client = client
            self._owns_client = True
            pool.register_managed_client(client)

        self._verify_or_abort(client)
        if self.logger is not None:
            verb = (
                "Started shared Dask session %s"
                if self.shared
                else "Started managed Dask session %s"
            )
            self.logger.info(verb, describe_client(client))
        return client

    def _cleanup(self) -> None:
        """Release Dask resources. Called by :meth:`ManagedResource.close`."""
        self._stop_watchdog()

        # Shared session: decrement ref count; only close when it hits zero.
        if self._shared_session_key is not None and self.client is not None:
            try:
                pool.release_shared_session(
                    self._shared_session_key, logger=self.logger, holder=self
                )
            finally:
                self.client = None
                self._cluster = None
                self._shared_session_key = None
                self._owns_client = False
                self._owns_cluster = False
            return

        self._release_managed_connection()

    def _release_managed_connection(self) -> None:
        """Close an owned client and cluster, warning about live persisted collections."""
        client = self.client if self._owns_client else None
        cluster = self._cluster if self._owns_cluster else None
        self.client = None
        self._owns_client = False
        self._cluster = None
        self._owns_cluster = False
        _close_owned_connection(client, cluster, logger=self.logger)

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


def _resource_config(
    config: ResourceConfig | None, logger: Any | None, *, verbose: bool, debug: bool
) -> ResourceConfig:
    if config is None:
        return ResourceConfig(
            logger=logger, verbose=verbose, debug=debug, skip_logger=logger is None
        )
    if logger is not None:
        # Copy instead of mutating: the caller may share the config object.
        return config.model_copy(update={"logger": logger, "skip_logger": False})
    return config


# Functional-style factory aliases for DaskSession/DaskSession.from_env_prefix
# — the "underlying object" the rule suggests exposing is DaskSession itself,
# already public and re-exported in __all__ right alongside these.
# spaghetti-ignore[pass-through-method]
def dask_session_from_env_prefix(
    prefix: str,
    *,
    env_file: str | Path | None = None,
    **overrides: Any,
) -> DaskSession:
    return DaskSession.from_env_prefix(prefix, env_file=env_file, **overrides)


# spaghetti-ignore[pass-through-method]
def dask_session(**kwargs: Any) -> DaskSession:
    return DaskSession(**kwargs)


__all__ = [
    "ConnectivityCheck",
    "DaskSession",
    "DaskSessionSettings",
    "DaskSessionUnavailable",
    "SessionPool",
    "WatchdogConfig",
    "apply_recommended_dask_config",
    "current_client_summary",
    "dask_session",
    "dask_session_from_env_prefix",
    "describe_client",
    "pool",
    "recommended_dask_config",
]
