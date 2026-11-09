"""Rebind and watchdog lifecycle for :class:`~boti_dask.session.DaskSession`.

Kept beside the session as a mixin so the session class stays focused on
open/close; everything here operates on the session's own connection
methods (``_open_connection``, ``_release_managed_connection``).
"""

from __future__ import annotations

import logging
import warnings
from typing import Any, NamedTuple

from .session_pool import pool
from .telemetry import configure_telemetry, get_telemetry
from .watchdog import Watchdog, WatchdogConfig

_module_log = logging.getLogger(__name__)


def _close_quietly(client: Any | None, cluster: Any | None) -> None:
    """Best-effort close of a client and cluster being replaced; failures are logged only."""
    if client is not None:
        try:
            pool.close_client(client)
        except Exception:
            _module_log.debug("close_client failed during rebind", exc_info=True)
        finally:
            pool.unregister_managed_client(client)
    if cluster is not None:
        try:
            cluster.close()
        except Exception:
            _module_log.debug("cluster.close failed during rebind", exc_info=True)


def _close_owned_connection(client: Any | None, cluster: Any | None, *, logger: Any | None) -> None:
    """Close a managed client and cluster, warning about live persisted collections."""
    try:
        if client is not None:
            _warn_live_persisted(client, logger)
            pool.close_client(client)
    finally:
        if client is not None:
            pool.unregister_managed_client(client)
        if cluster is not None:
            cluster.close()


def _warn_live_persisted(client: Any, logger: Any | None) -> None:
    live = pool.live_persisted_collection_count(client)
    if not live:
        return
    msg = (
        f"Closing managed Dask session with {live} live persisted collection(s). "
        "Those collections will become unusable after the session closes; "
        "compute or preview them inside the session, or keep the client open. "
        "Later head()/compute() calls may fail with Missing dependency."
    )
    warnings.warn(msg, RuntimeWarning, stacklevel=3)
    if logger is not None:
        logger.warning(msg)


class _Binding(NamedTuple):
    """A session's connection state, taken aside while a replacement is opened."""

    client: Any | None
    cluster: Any | None
    shared_key: str | None
    owns_client: bool
    owns_cluster: bool


def _client_gauges(client: Any) -> dict[str, float]:
    """Workers and executing tasks from one ``scheduler_info()`` call; zeros when it fails."""
    try:
        workers = client.scheduler_info().get("workers", {})
    except Exception:
        _module_log.debug("scheduler_info() failed while recording gauges", exc_info=True)
        workers = {}
    executing = sum(
        int((worker.get("metrics") or {}).get("executing", 0) or 0) for worker in workers.values()
    )
    return {"workers": float(len(workers)), "task_count": float(executing)}


class SessionRebindMixin:
    """Adds :meth:`rebind`, the optional :class:`Watchdog` and telemetry gauges to a session."""

    # Provided by DaskSession.
    namespace: str
    client: Any | None
    logger: Any | None
    shared_key: str | None
    telemetry: Any | None
    upload: list[Any]
    watchdog_config: WatchdogConfig | None
    _watchdog: Watchdog | None
    _cluster: Any | None
    _shared_session_key: str | None
    _owns_client: bool
    _owns_cluster: bool
    _rebinding: bool

    # Also relies on DaskSession._open_connection / _release_managed_connection
    # and ManagedResource._assert_open; no stubs here, a stub would shadow
    # ManagedResource's in the MRO.

    def _start_watchdog(self) -> None:
        if self.watchdog_config is None or self._watchdog is not None:
            return
        self._watchdog = Watchdog(self, self.watchdog_config, logger=self.logger)
        self._watchdog.start()

    # -- uploads ---------------------------------------------------------------

    def _sync_uploads(self) -> None:
        if self.upload and self.client is not None:
            from .upload import sync_uploads

            sync_uploads(self.client, self.upload, logger=self.logger)

    # -- telemetry -------------------------------------------------------------

    def _install_telemetry(self) -> None:
        """Make this session's provider the process provider, then publish the gauges."""
        if self.telemetry is not None:
            configure_telemetry(self.telemetry)
        self.record_gauges()

    def record_gauges(self) -> dict[str, float]:
        """Publish ``workers``, ``persisted_collections`` and ``task_count`` for the client."""
        client = self.client
        if client is None:
            return {}
        values = _client_gauges(client)
        values["persisted_collections"] = float(pool.live_persisted_collection_count(client))
        telemetry = get_telemetry()
        for name, value in values.items():
            telemetry.gauge(name, value, shared_key=self.shared_key)
        return values

    def _stop_watchdog(self) -> None:
        if self._watchdog is not None:
            self._watchdog.stop()
            self._watchdog = None

    @property
    def watchdog(self) -> Watchdog | None:
        """The running :class:`Watchdog`, or ``None`` before open / without a config."""
        return self._watchdog

    # -- rebind ----------------------------------------------------------------

    def rebind(self, *, stale: Any | None = None) -> Any:
        """Replace the current client with a fresh connection to the same target.

        The replacement is opened first and the old client closed only once it
        is up, so a failed reconnect raises and leaves the session on its
        previous client. A managed session closes its client (and cluster); a
        shared session swaps the pool entry under the creation lock and points
        every holder of the key at the new client, keeping the holder count.
        With ``stale``, nothing happens if the session's client is already a
        different object (another holder rebound it first). Persisted
        collections on the old client are lost, as on close. An externally
        supplied client cannot be rebound.
        """
        self._assert_open()
        if self._shared_session_key is None and not self._owns_client:
            raise RuntimeError("Cannot rebind a DaskSession around an externally supplied client.")
        if stale is not None and self.client is not stale:
            return self.client
        self._rebinding = True
        try:
            if self._shared_session_key is None:
                client = self._rebind_managed()
            else:
                client = self._rebind_shared(self._shared_session_key)
        finally:
            self._rebinding = False
        pool.record_rebind()
        get_telemetry().add("rebinds", shared_key=self.shared_key)
        self.record_gauges()
        self._sync_uploads()
        if self.logger is not None:
            # The namespace is the explicit key or a digest of the derived one,
            # which embeds repr(client_kwargs); None for a managed session.
            self.logger.warning(
                "Rebound Dask session key=%r to %s",
                None if self._shared_session_key is None else self.namespace,
                getattr(client, "dashboard_link", None) or type(client).__name__,
            )
        return client

    def _rebind_managed(self) -> Any:
        previous, client = self._reconnect()
        cluster = previous.cluster if previous.owns_cluster else None
        _close_owned_connection(previous.client, cluster, logger=self.logger)
        return client

    def _rebind_shared(self, key: str) -> Any:
        with pool.shared_creation_lock(key):
            entry = pool.detach_shared_session(key)
            try:
                previous, client = self._reconnect()
            except Exception:
                pool.restore_shared_session(key, entry)
                raise
            holders = pool.carry_over_shared_holders(key, entry)
        for holder in holders:
            # Every holder is a DaskSession like this one; the method is
            # private to the class, not to the instance.
            # spaghetti-ignore[encapsulation-violation]
            holder._adopt_shared_client(client, self._cluster, key)
        _close_quietly(previous.client, previous.cluster)
        return client

    def _adopt_shared_client(self, client: Any, cluster: Any | None, key: str) -> None:
        """Point this holder at the client another holder of *key* just opened."""
        self.client = client
        self._cluster = cluster
        self._shared_session_key = key

    def _reconnect(self) -> tuple[_Binding, Any]:
        """Open a replacement connection; on failure restore the previous binding and re-raise."""
        previous = self._take_binding()
        try:
            client = self._open_connection()
        except Exception:
            self._discard_failed_attempt()
            self._put_binding(previous)
            raise
        return previous, client

    def _take_binding(self) -> _Binding:
        binding = _Binding(
            self.client,
            self._cluster,
            self._shared_session_key,
            self._owns_client,
            self._owns_cluster,
        )
        self._put_binding(_Binding(None, None, None, False, False))
        return binding

    def _put_binding(self, binding: _Binding) -> None:
        self.client = binding.client
        self._cluster = binding.cluster
        self._shared_session_key = binding.shared_key
        self._owns_client = binding.owns_client
        self._owns_cluster = binding.owns_cluster

    def _discard_failed_attempt(self) -> None:
        """Close whatever a reconnect registered before it failed verification."""
        if self._shared_session_key is not None:
            pool.detach_shared_session(self._shared_session_key)
        _close_quietly(self.client, self._cluster)
