"""Background health checks that rebind a :class:`DaskSession` after repeated failures.

A :class:`Watchdog` runs the same probe :class:`DaskSession` uses on open (a
scheduler ping, a wait for ``min_workers`` and one round-trip task) every
``interval`` seconds on a daemon thread. After ``failures_before_rebind``
consecutive failures it asks the session to :meth:`~DaskSession.rebind`, which
replaces the dead client with a fresh connection to the same target. Each
transition is reported through ``on_event`` and the session logger, so an
application can alert, back off or shut down on its own terms.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ._internal import _log
from .session_helpers import ConnectivityCheck, _verify_client_connection

__all__ = ["Watchdog", "WatchdogConfig"]

DEFAULT_WATCHDOG_INTERVAL = 30.0
DEFAULT_FAILURES_BEFORE_REBIND = 3

_module_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WatchdogConfig:
    """How often to probe, how many misses to tolerate, and whom to tell."""

    interval: float = DEFAULT_WATCHDOG_INTERVAL
    failures_before_rebind: int = DEFAULT_FAILURES_BEFORE_REBIND
    check: ConnectivityCheck | None = None
    on_event: Callable[[dict[str, Any]], None] | None = None

    def __post_init__(self) -> None:
        if self.interval <= 0:
            raise ValueError("WatchdogConfig.interval must be positive.")
        if self.failures_before_rebind < 1:
            raise ValueError("WatchdogConfig.failures_before_rebind must be at least 1.")


class Watchdog:
    """Probe a session's client on a timer and rebind it after repeated failures."""

    def __init__(self, session: Any, config: WatchdogConfig, *, logger: Any | None = None) -> None:
        self.session = session
        self.config = config
        self.logger = logger
        self.failures = 0
        self.rebinds = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="boti-dask-watchdog", daemon=True)
        self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        self._thread = None

    # -- probing --------------------------------------------------------------

    def _check(self) -> ConnectivityCheck:
        return (
            self.config.check or getattr(self.session, "connectivity", None) or ConnectivityCheck()
        )

    def _run(self) -> None:
        while not self._stop.wait(self.config.interval):
            if self.session.client is None:
                self._emit("stopped", reason="session closed")
                return
            self.tick()

    def tick(self) -> None:
        """One probe; public so tests and schedulers can drive it without the thread."""
        try:
            _verify_client_connection(self.session.client, self._check())
        except Exception as exc:
            self._on_failure(exc)
            return
        if self.failures:
            self._emit("recovered", failures=self.failures)
        self.failures = 0

    def _on_failure(self, exc: Exception) -> None:
        self.failures += 1
        self._emit("probe_failed", failures=self.failures, error=f"{type(exc).__name__}: {exc}")
        if self.failures < self.config.failures_before_rebind:
            return
        self._emit("rebinding", failures=self.failures)
        try:
            self.session.rebind()
        except Exception as rebind_exc:
            self._emit("rebind_failed", error=f"{type(rebind_exc).__name__}: {rebind_exc}")
            return
        self.failures = 0
        self.rebinds += 1
        self._emit("rebound", rebinds=self.rebinds)

    def _emit(self, event: str, **fields: Any) -> None:
        payload = {
            "event": event,
            "shared_key": getattr(self.session, "shared_key", None),
            **fields,
        }
        level = "warning" if event in {"probe_failed", "rebinding", "rebind_failed"} else "info"
        _log(self.logger, level, f"Dask watchdog {event}: {fields}")
        if self.config.on_event is None:
            return
        try:
            self.config.on_event(payload)
        except Exception:
            _module_log.debug("watchdog on_event callback failed", exc_info=True)
