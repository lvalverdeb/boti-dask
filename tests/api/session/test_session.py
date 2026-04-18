from __future__ import annotations

import dask
import pytest
import threading
from concurrent.futures import ThreadPoolExecutor

from boti_dask import session as session_module
from boti_dask.session import (
    DaskSession,
    DaskSessionSettings,
    apply_recommended_dask_config,
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
        dashboard_address=None,
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
        dashboard_address=None,
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

    session_module._register_shared_session(key, client=client, cluster=cluster)
    with session_module._REGISTRY_LOCK:
        assert key in session_module._SHARED_SESSION_REGISTRY
        session_module._SHARED_SESSION_REGISTRY[key]["ref_count"] = 8

    def release_once() -> None:
        session_module._release_shared_session(key)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(release_once) for _ in range(8)]
        for future in futures:
            future.result()

    with session_module._REGISTRY_LOCK:
        assert key not in session_module._SHARED_SESSION_REGISTRY

    assert client.close_count == 1
    assert cluster.close_count == 1


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


