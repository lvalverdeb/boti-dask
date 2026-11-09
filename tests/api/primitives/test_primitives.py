"""Cluster primitives share by name on a cluster and fall back to the process."""

from __future__ import annotations

import queue
import threading
import time

import pytest

from boti_dask import cluster_lock, cluster_queue, cluster_semaphore
from boti_dask.primitives import reset_local_primitives
from boti_dask.session import dask_session, pool

_state = {"active": 0, "max_seen": 0, "runs": 0}
_state_lock = threading.Lock()


@pytest.fixture(autouse=True)
def _fresh_local_registry():
    reset_local_primitives()
    yield
    reset_local_primitives()


def _guarded_work(limit: int) -> int:
    with cluster_semaphore("test:fetch", max_leases=limit):
        with _state_lock:
            _state["active"] += 1
            _state["max_seen"] = max(_state["max_seen"], _state["active"])
        time.sleep(0.15)
        with _state_lock:
            _state["active"] -= 1
            _state["runs"] += 1
    return 1


def test_semaphore_bounds_concurrency_across_workers():
    pytest.importorskip("dask.distributed")
    _state.update(active=0, max_seen=0, runs=0)
    with dask_session(
        cluster_kwargs={
            "n_workers": 3,
            "threads_per_worker": 2,
            "processes": False,
            "dashboard_address": ":0",
        }
    ) as client:
        futures = [client.submit(_guarded_work, 2, pure=False) for _ in range(6)]
        assert sum(client.gather(futures)) == 6
    assert _state["runs"] == 6
    assert _state["max_seen"] <= 2
    assert _state["max_seen"] >= 2


def test_lock_and_queue_are_shared_by_name_on_the_cluster():
    pytest.importorskip("dask.distributed")
    with dask_session(
        cluster_kwargs={
            "n_workers": 1,
            "threads_per_worker": 2,
            "processes": False,
            "dashboard_address": ":0",
        }
    ) as client:
        q = cluster_queue("test:items", client=client)

        def producer(n: int) -> int:
            with cluster_lock("test:producer"):
                for i in range(n):
                    cluster_queue("test:items").put(i)
            return n

        assert client.submit(producer, 3, pure=False).result(timeout=20) == 3
        assert sorted(q.get(timeout=5) for _ in range(3)) == [0, 1, 2]


def test_semaphore_limit_mismatch_raises_unless_strict_is_off_on_the_cluster(caplog):
    pytest.importorskip("dask.distributed")
    with dask_session(
        cluster_kwargs={
            "n_workers": 1,
            "threads_per_worker": 1,
            "processes": False,
            "dashboard_address": ":0",
        }
    ) as client:
        first = cluster_semaphore("test:limit", max_leases=4, client=client)
        with pytest.raises(ValueError, match="limit 8 requested but 4 is already set"):
            cluster_semaphore("test:limit", max_leases=8, client=client)
        with caplog.at_level("WARNING", logger="boti_dask.primitives"):
            second = cluster_semaphore("test:limit", max_leases=8, client=client, strict=False)
        assert first.max_leases == 4
        assert second.max_leases == 4
    assert any("first creator's limit wins" in r.getMessage() for r in caplog.records)


def test_primitives_are_closed_with_their_client():
    closed: list[str] = []

    class StubPrimitive:
        def close(self) -> None:
            closed.append("primitive")

    class StubClient:
        status = "running"

        def close(self) -> None:
            closed.append("client")
            self.status = "closed"

    client = StubClient()
    pool.register_managed_client(client)
    pool.track_primitive(client, StubPrimitive())
    assert pool.tracked_primitive_count(client) == 1
    pool.close_client(client)
    pool.unregister_managed_client(client)
    assert closed == ["primitive", "client"]
    assert pool.tracked_primitive_count(client) == 0


def test_without_a_client_the_process_local_equivalents_are_shared_by_name():
    sem = cluster_semaphore("local:fetch", max_leases=2)
    assert isinstance(sem, threading.BoundedSemaphore)
    assert cluster_semaphore("local:fetch", max_leases=2) is sem
    assert isinstance(cluster_lock("local:lock"), type(threading.Lock()))
    assert cluster_lock("local:lock") is cluster_lock("local:lock")
    q = cluster_queue("local:q", maxsize=1)
    assert isinstance(q, queue.Queue)
    q.put("x")
    assert cluster_queue("local:q").get(timeout=1) == "x"
    with sem:
        with sem:
            assert sem.acquire(timeout=0.05) is False


def test_bad_names_and_limits_are_rejected():
    with pytest.raises(ValueError, match="names"):
        cluster_lock("has space")
    with pytest.raises(ValueError, match="max_leases"):
        cluster_semaphore("x", max_leases=0)


def test_semaphore_limit_mismatch_raises_locally_unless_strict_is_off(caplog):
    first = cluster_semaphore("local:limit", max_leases=2)
    with pytest.raises(ValueError, match="limit 3 requested but 2 is already set"):
        cluster_semaphore("local:limit", max_leases=3)
    with caplog.at_level("WARNING", logger="boti_dask.primitives"):
        assert cluster_semaphore("local:limit", max_leases=3, strict=False) is first
    assert any("first creator's limit wins" in r.getMessage() for r in caplog.records)
    assert cluster_semaphore("local:limit", max_leases=2) is first


class _ClosedClient:
    status = "closed"


def test_a_client_that_is_not_running_is_refused_rather_than_made_local():
    for make in (
        lambda: cluster_lock("dead:lock", client=_ClosedClient()),
        lambda: cluster_semaphore("dead:sem", client=_ClosedClient()),
        lambda: cluster_queue("dead:q", client=_ClosedClient()),
    ):
        with pytest.raises(RuntimeError, match="not running"):
            make()
    assert cluster_lock("dead:lock") is not None  # no client given: local is intended


def test_namespace_keeps_sessions_apart():
    from boti_dask.primitives import _qualified

    assert _qualified("x") == "boti-dask:x"
    assert _qualified("x", "etl") == "boti-dask:etl:x"
    assert cluster_lock("x", namespace="a") is not cluster_lock("x", namespace="b")
    assert cluster_lock("x", namespace="a") is cluster_lock("x", namespace="a")
    assert cluster_queue("x", namespace="a") is not cluster_queue("x")
    with pytest.raises(ValueError, match="namespaces"):
        cluster_lock("x", namespace="has space")

    shared = dask_session(shared=True, shared_key="my-key")
    assert shared.namespace == "my-key"
    managed_a, managed_b = dask_session(), dask_session()
    assert managed_a.namespace != managed_b.namespace
    assert " " not in managed_a.namespace
    assert cluster_lock("x", namespace=managed_a.namespace) is not cluster_lock(
        "x", namespace=managed_b.namespace
    )
