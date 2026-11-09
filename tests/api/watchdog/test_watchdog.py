"""The watchdog probes on a timer and rebinds the session after repeated failures."""

from __future__ import annotations

import threading
import time

import pytest

from boti_dask import ConnectivityCheck, DaskSession, DaskSessionSettings, Watchdog, WatchdogConfig
from boti_dask import session as session_module
from boti_dask.session_pool import pool


class StubFuture:
    def result(self, timeout=None):
        return 1


class StubClient:
    """A client whose health is a switch, so probes can be made to fail on demand."""

    healthy = True
    instances: list[StubClient] = []

    def __init__(self, address, **_kwargs) -> None:
        self.address = address
        self.status = "running"
        self.close_count = 0
        StubClient.instances.append(self)

    def _assert_healthy(self) -> None:
        if not StubClient.healthy:
            raise OSError("scheduler gone")

    def scheduler_info(self):
        self._assert_healthy()
        return {"workers": {"w": {"nthreads": 1}}}

    def wait_for_workers(self, n_workers, timeout=None):
        self._assert_healthy()

    def submit(self, fn, pure=False):
        self._assert_healthy()
        return StubFuture()

    def close(self) -> None:
        self.close_count += 1
        self.status = "closed"


@pytest.fixture
def stub_client(monkeypatch):
    StubClient.healthy = True
    StubClient.instances = []
    monkeypatch.setattr(session_module, "Client", StubClient)
    return StubClient


def _wait_for(predicate, timeout=5.0) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met in time")


def test_config_validation():
    with pytest.raises(ValueError, match="interval"):
        WatchdogConfig(interval=0)
    with pytest.raises(ValueError, match="failures_before_rebind"):
        WatchdogConfig(failures_before_rebind=0)


def test_tick_counts_failures_and_rebinds_at_threshold(stub_client):
    events: list[dict] = []
    session = DaskSession(
        scheduler_address="tcp://stub:1",
        verify_connectivity=False,
        watchdog=WatchdogConfig(
            interval=60,
            failures_before_rebind=2,
            check=ConnectivityCheck(timeout=0.2),
            on_event=events.append,
        ),
    )
    first = session.open()
    watchdog = session.watchdog
    assert watchdog is not None and watchdog.running
    try:
        rebinds_before = pool.rebind_count()
        watchdog.tick()
        assert watchdog.failures == 0 and events == []

        stub_client.healthy = False
        watchdog.tick()
        assert watchdog.failures == 1
        assert session.client is first
        stub_client.healthy = True  # the replacement must come up healthy
        stub_client.healthy = False
        watchdog.tick()  # second failure -> rebind
        assert [e["event"] for e in events] == [
            "probe_failed",
            "probe_failed",
            "rebinding",
            "rebound",
        ]
        assert session.client is not first
        assert first.close_count == 1
        assert watchdog.failures == 0 and watchdog.rebinds == 1
        assert pool.rebind_count() == rebinds_before + 1
        assert pool.is_client_registered(session.client)
        assert not pool.is_client_registered(first)
    finally:
        session.close()
    assert not watchdog.running


def test_thread_probes_on_interval_and_recovers(stub_client):
    events: list[dict] = []
    session = DaskSession(
        scheduler_address="tcp://stub:1",
        verify_connectivity=False,
        watchdog=WatchdogConfig(
            interval=0.02,
            failures_before_rebind=10,
            check=ConnectivityCheck(timeout=0.2),
            on_event=events.append,
        ),
    )
    session.open()
    try:
        stub_client.healthy = False
        _wait_for(lambda: session.watchdog.failures >= 2)
        stub_client.healthy = True
        _wait_for(lambda: any(e["event"] == "recovered" for e in events))
        assert session.watchdog.failures == 0
    finally:
        session.close()
    assert len(stub_client.instances) == 1  # never reached the rebind threshold


def test_shared_session_rebind_keeps_holders_and_swaps_pool_entry(stub_client):
    key = "watchdog-shared"
    holders = [
        DaskSession(
            scheduler_address="tcp://stub:1", shared=True, shared_key=key, verify_connectivity=False
        )
        for _ in range(2)
    ]
    clients = [h.open() for h in holders]
    assert clients[0] is clients[1]
    try:
        new_client = holders[0].rebind()
        assert new_client is not clients[0]
        assert clients[0].close_count == 1
        entry = pool.get_shared_session(key)
        assert entry["client"] is new_client
        assert entry["ref_count"] == 2
        assert holders[0].client is new_client
        # A later holder of the same key reuses the rebound client.
        third = DaskSession(
            scheduler_address="tcp://stub:1", shared=True, shared_key=key, verify_connectivity=False
        )
        assert third.open() is new_client
        third.close()
    finally:
        for h in holders:
            h.close()
    assert key not in pool.debug_shared_session_keys()
    assert new_client.close_count == 1


def test_external_client_cannot_be_rebound():
    session = DaskSession(client=object(), verify_connectivity=False)
    session.open()
    with pytest.raises(RuntimeError, match="externally supplied"):
        session.rebind()
    session.close()


def test_rebind_failure_is_reported_and_retried_next_tick(stub_client, monkeypatch):
    events: list[dict] = []
    session = DaskSession(
        scheduler_address="tcp://stub:1",
        verify_connectivity=False,
        watchdog=WatchdogConfig(
            interval=60,
            failures_before_rebind=1,
            check=ConnectivityCheck(timeout=0.2),
            on_event=events.append,
        ),
    )
    session.open()
    try:
        monkeypatch.setattr(session, "rebind", lambda: (_ for _ in ()).throw(OSError("still down")))
        stub_client.healthy = False
        session.watchdog.tick()
        assert [e["event"] for e in events] == ["probe_failed", "rebinding", "rebind_failed"]
        assert session.watchdog.failures == 1  # kept, so the next tick tries again
    finally:
        session.close()


def test_on_event_errors_never_break_the_watchdog(stub_client):
    def bad_callback(_event):
        raise RuntimeError("boom")

    session = DaskSession(
        scheduler_address="tcp://stub:1",
        verify_connectivity=False,
        watchdog=WatchdogConfig(
            interval=60,
            failures_before_rebind=5,
            on_event=bad_callback,
            check=ConnectivityCheck(timeout=0.2),
        ),
    )
    session.open()
    try:
        stub_client.healthy = False
        session.watchdog.tick()
        assert session.watchdog.failures == 1
    finally:
        session.close()


def test_settings_build_watchdog_config(monkeypatch):
    monkeypatch.setenv("DASK_SESSION_WATCHDOG_INTERVAL", "15")
    monkeypatch.setenv("DASK_SESSION_WATCHDOG_FAILURES", "2")
    settings = DaskSessionSettings.from_env_prefix("DASK_SESSION_")
    config = settings.to_session_kwargs()["watchdog"]
    assert config == WatchdogConfig(interval=15.0, failures_before_rebind=2)
    assert "watchdog" not in DaskSessionSettings().to_session_kwargs()


def test_watchdog_stops_itself_when_the_session_is_closed(stub_client):
    stopped = threading.Event()
    session = DaskSession(
        scheduler_address="tcp://stub:1",
        verify_connectivity=False,
        watchdog=WatchdogConfig(
            interval=0.02, on_event=lambda e: stopped.set() if e["event"] == "stopped" else None
        ),
    )
    session.open()
    watchdog: Watchdog = session.watchdog
    session.client = None  # simulate the client vanishing under the thread
    assert stopped.wait(2)
    _wait_for(lambda: not watchdog.running)
    session.close()
