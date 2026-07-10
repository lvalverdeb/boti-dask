from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import dask
import pytest

from boti_dask import session as session_module
from boti_dask.session import (
    DaskSession,
    DaskSessionSettings,
    apply_recommended_dask_config,
    current_client_summary,
    dask_session,
    dask_session_from_env_prefix,
    describe_client,
    recommended_dask_config,
)


class _CloseSpy:
    def __init__(self) -> None:
        self.close_count = 0
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self.close_count += 1


def test_recommended_dask_config_allows_overrides():
    profile = recommended_dask_config(overrides={"dataframe.shuffle.method": "p2p"})
    assert profile["dataframe.shuffle.method"] == "p2p"
    assert "distributed.comm.timeouts.connect" in profile


def test_apply_recommended_dask_config_context_manager():
    with apply_recommended_dask_config(**{"dataframe.shuffle.method": "tasks"}):
        assert dask.config.get("dataframe.shuffle.method") == "tasks"


def test_dask_session_connects_to_scheduler_address():
    distributed = pytest.importorskip("dask.distributed")
    Client = distributed.Client
    LocalCluster = distributed.LocalCluster

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster, Client(cluster):
        with dask_session(scheduler_address=cluster.scheduler_address, verify_connectivity=True) as client:
            summary = describe_client(client)

    assert summary["workers"] == 1


def test_dask_session_reuses_shared_client_by_key():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        with dask_session(
            scheduler_address=cluster.scheduler_address,
            shared=True,
            shared_key="boti-dask-shared",
        ) as client_a:
            with dask_session(
                scheduler_address=cluster.scheduler_address,
                shared=True,
                shared_key="boti-dask-shared",
            ) as client_b:
                assert client_a is client_b


def test_release_shared_session_is_safe_under_concurrent_release_calls():
    key = "threaded-release"
    client = _CloseSpy()
    cluster = _CloseSpy()

    session_module.pool.register_shared_session(key, client=client, cluster=cluster)
    session_module.pool.debug_set_shared_ref_count(key, 8)

    def release_once() -> None:
        session_module.pool.release_shared_session(key)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(release_once) for _ in range(8)]
        for future in futures:
            future.result()

    assert key not in session_module.pool.debug_shared_session_keys()

    assert client.close_count == 1
    assert cluster.close_count == 1


def test_concurrent_shared_first_open_creates_single_cluster(monkeypatch):
    """Two threads opening the same shared key concurrently must not both
    create a cluster: the second waits on the creation lock and reuses."""
    created: list[object] = []
    release_factory = threading.Event()

    class StubCluster:
        def __init__(self) -> None:
            created.append(self)
            release_factory.wait(timeout=5)  # hold first opener inside create
            self.close_count = 0

        def close(self) -> None:
            self.close_count += 1

    class StubClient:
        def __init__(self, cluster, **_kwargs) -> None:
            self.cluster = cluster
            self.status = "running"
            self.close_count = 0

        def close(self) -> None:
            self.close_count += 1
            self.status = "closed"

    monkeypatch.setattr(session_module, "Client", StubClient)

    key = "concurrent-first-open"
    sessions = [
        DaskSession(shared=True, shared_key=key, cluster_factory=StubCluster)
        for _ in range(2)
    ]
    clients: list[object] = []
    threads = [
        threading.Thread(target=lambda s=s: clients.append(s.open()))
        for s in sessions
    ]
    try:
        for t in threads:
            t.start()
        release_factory.set()
        for t in threads:
            t.join(timeout=5)
        assert not any(t.is_alive() for t in threads)

        assert len(created) == 1, "both openers created a cluster — race not serialised"
        assert clients[0] is clients[1]

        entry = session_module.pool.get_shared_session(key)
        assert entry is not None
        assert entry["ref_count"] == 2
    finally:
        for s in sessions:
            s.close()

    assert key not in session_module.pool.debug_shared_session_keys()
    assert clients[0].close_count == 1
    assert created[0].close_count == 1


def test_open_after_close_raises():
    """open() on a closed session must raise instead of leaking a fresh cluster."""
    session = DaskSession(client=object())
    session.close()
    with pytest.raises(RuntimeError, match="closed"):
        session.open()


def test_pool_prunes_registries_when_client_is_garbage_collected():
    """Registry entries for GC'd clients are removed so the pool cannot grow
    unboundedly in long-lived processes."""
    import gc

    class DummyClient:
        pass

    client = DummyClient()
    client_id = id(client)
    session_module.pool.register_managed_client(client)
    assert session_module.pool.is_client_registered(client)

    del client
    gc.collect()

    with session_module.pool._lock:
        assert client_id not in session_module.pool._managed_clients
        assert client_id not in session_module.pool._persisted_collections


def test_dask_session_settings_from_env_prefix(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "DASK_SESSION_SCHEDULER_ADDRESS=tcp://scheduler:8786",
                "DASK_SESSION_SHARED=true",
                "DASK_SESSION_SHARED_KEY=main-cluster",
                "DASK_SESSION_VERIFY_CONNECTIVITY=1",
                'DASK_SESSION_CLUSTER_KWARGS={"n_workers":2,"threads_per_worker":1}',
                'DASK_SESSION_CLIENT_KWARGS={"set_as_default":false}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    settings = DaskSessionSettings.from_env_prefix("DASK_SESSION_", env_file=env_file)
    assert settings.scheduler_address == "tcp://scheduler:8786"
    assert settings.shared is True
    assert settings.shared_key == "main-cluster"
    assert settings.verify_connectivity is True
    assert settings.cluster_kwargs == {"n_workers": 2, "threads_per_worker": 1}
    assert settings.client_kwargs == {"set_as_default": False}


def test_dask_session_settings_reject_invalid_json(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("DASK_SESSION_CLUSTER_KWARGS=not-json\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid JSON value"):
        DaskSessionSettings.from_env_prefix("DASK_SESSION_", env_file=env_file)


def test_dask_session_from_env_prefix_applies_overrides(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("DASK_SESSION_SHARED=true\n", encoding="utf-8")

    session = dask_session_from_env_prefix("DASK_SESSION_", env_file=env_file, shared=False)
    assert isinstance(session, DaskSession)
    assert session.shared is False


def test_describe_client_logs_debug_when_scheduler_info_fails(caplog):
    """Regression: describe_client used to swallow client.scheduler_info()
    failures via `except Exception: info = {}`, giving no trace of why
    workers/threads came back empty. It now logs at debug level."""

    class BrokenClient:
        def scheduler_info(self):
            raise RuntimeError("boom")

    with caplog.at_level(logging.DEBUG, logger="boti_dask.session"):
        summary = describe_client(BrokenClient())

    assert summary["workers"] == 0
    assert any("scheduler_info() failed" in record.message for record in caplog.records)


def test_current_client_summary_logs_debug_when_get_client_fails(monkeypatch, caplog):
    """Regression: current_client_summary used to swallow
    describe_client(get_client()) failures with a bare
    `except Exception: return None`. It now logs at debug level."""

    def failing_get_client():
        raise RuntimeError("no active client")

    monkeypatch.setattr(session_module, "get_client", failing_get_client)

    with caplog.at_level(logging.DEBUG, logger="boti_dask.session"):
        result = current_client_summary()

    assert result is None
    assert any("current_client_summary" in record.message for record in caplog.records)


def test_prepare_cluster_kwargs_defaults_dashboard_for_localcluster():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    resolved = session_module._prepare_cluster_kwargs(LocalCluster, {"n_workers": 1})

    assert resolved["dashboard_address"] == ":0"


def test_prepare_cluster_kwargs_preserves_explicit_dashboard_address():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    explicit = session_module._prepare_cluster_kwargs(
        LocalCluster,
        {"n_workers": 1, "dashboard_address": ":8789"},
    )
    explicit_none = session_module._prepare_cluster_kwargs(
        LocalCluster,
        {"n_workers": 1, "dashboard_address": None},
    )

    assert explicit["dashboard_address"] == ":8789"
    assert explicit_none["dashboard_address"] is None
