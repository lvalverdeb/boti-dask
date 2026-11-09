"""Rebind and watchdog lifecycle for :class:`~boti_dask.session.DaskSession`.

Kept beside the session as a mixin so the session class stays focused on
open/close; everything here operates on the session's own connection
methods (``_open_connection``, ``_release_managed_connection``).
"""

from __future__ import annotations

import logging
from typing import Any

from .session_pool import pool
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


class SessionRebindMixin:
    """Adds :meth:`rebind` and the optional :class:`Watchdog` to a session."""

    # Provided by DaskSession.
    client: Any | None
    logger: Any | None
    watchdog_config: WatchdogConfig | None
    _watchdog: Watchdog | None
    _cluster: Any | None
    _shared_session_key: str | None
    _owns_client: bool

    # Also relies on DaskSession._open_connection / _release_managed_connection
    # and ManagedResource._assert_open; no stubs here, a stub would shadow
    # ManagedResource's in the MRO.

    def _start_watchdog(self) -> None:
        if self.watchdog_config is None or self._watchdog is not None:
            return
        self._watchdog = Watchdog(self, self.watchdog_config, logger=self.logger)
        self._watchdog.start()

    def _stop_watchdog(self) -> None:
        if self._watchdog is not None:
            self._watchdog.stop()
            self._watchdog = None

    @property
    def watchdog(self) -> Watchdog | None:
        """The running :class:`Watchdog`, or ``None`` before open / without a config."""
        return self._watchdog

    # -- rebind ----------------------------------------------------------------

    def rebind(self) -> Any:
        """Replace the current client with a fresh connection to the same target.

        A managed session closes its client (and cluster) and opens again; a
        shared session swaps the pool entry under the creation lock so every
        holder's next ``pool`` lookup and every ``safe_*`` retry sees the new
        client, keeping the holder count. Persisted collections on the old
        client are lost, as on close. An externally supplied client cannot be
        rebound.
        """
        self._assert_open()
        if self._shared_session_key is None and not self._owns_client:
            raise RuntimeError("Cannot rebind a DaskSession around an externally supplied client.")
        if self._shared_session_key is None:
            self._release_managed_connection()
            client = self._open_connection()
        else:
            key = self._shared_session_key
            with pool.shared_creation_lock(key):
                client = self._rebind_shared(key)
        pool.record_rebind()
        if self.logger is not None:
            self.logger.warning(
                "Rebound Dask session key=%r to %s",
                self._shared_session_key,
                getattr(client, "dashboard_link", None) or type(client).__name__,
            )
        return client

    def _rebind_shared(self, key: str) -> Any:
        entry = pool.detach_shared_session(key)
        self.client = None
        self._cluster = None
        self._shared_session_key = None
        if entry is not None:
            _close_quietly(entry.get("client"), entry.get("cluster"))
        client = self._open_connection()
        if entry is not None:
            pool.set_shared_ref_count(key, int(entry.get("ref_count", 1)))
        return client
