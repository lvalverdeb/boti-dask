"""Connectivity verification: fail fast with a named error, never hang."""

from __future__ import annotations

import time

import pytest

from boti_dask import ConnectivityCheck, DaskSession, DaskSessionSettings, DaskSessionUnavailable
from boti_dask.session import dask_session


def test_dead_scheduler_fails_within_the_deadline():
    started = time.perf_counter()
    with pytest.raises(DaskSessionUnavailable, match="could not connect"):
        with dask_session(
            scheduler_address="tcp://127.0.0.1:1",
            verify_connectivity=ConnectivityCheck(timeout=2),
        ):
            pass
    assert time.perf_counter() - started < 10


def test_cluster_without_workers_fails_within_the_deadline():
    distributed = pytest.importorskip("dask.distributed")
    started = time.perf_counter()
    with distributed.LocalCluster(n_workers=0, processes=False, dashboard_address=":0") as cluster:
        with pytest.raises(DaskSessionUnavailable, match="waiting for 1 worker"):
            with dask_session(
                scheduler_address=cluster.scheduler_address,
                verify_connectivity=ConnectivityCheck(timeout=1.5),
            ):
                pass
    assert time.perf_counter() - started < 10


class _RecordingClient:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.status = "running"

    def scheduler_info(self) -> dict:
        self.calls.append("scheduler_info")
        return {"workers": {"w1": {"nthreads": 1}}}

    def wait_for_workers(self, n_workers: int, timeout: float | None = None) -> None:
        self.calls.append(f"wait_for_workers:{n_workers}")

    def submit(self, fn, *args, **kwargs):
        self.calls.append("submit")
        return self

    def result(self, timeout: float | None = None) -> int:
        return 1


def test_default_check_pings_waits_and_round_trips_on_an_external_client():
    client = _RecordingClient()
    session = DaskSession(client=client)
    try:
        assert session.open() is client
    finally:
        session.close()
    assert client.calls == ["scheduler_info", "wait_for_workers:1", "submit"]


def test_verify_off_hands_out_a_client_that_cannot_be_probed():
    bare = object()
    session = DaskSession(client=bare, verify_connectivity=False)
    try:
        assert session.open() is bare
        assert session.connectivity is None
    finally:
        session.close()


def test_connectivity_check_rejects_bad_values():
    with pytest.raises(ValueError, match="timeout"):
        ConnectivityCheck(timeout=0)
    with pytest.raises(ValueError, match="min_workers"):
        ConnectivityCheck(min_workers=0)


def test_settings_carry_the_check_into_session_kwargs(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("BD_CONNECTIVITY_TIMEOUT=3.5\nBD_MIN_WORKERS=2\n", encoding="utf-8")
    settings = DaskSessionSettings.from_env_prefix("BD_", env_file=env_file)
    assert settings.to_session_kwargs()["verify_connectivity"] == ConnectivityCheck(
        timeout=3.5, min_workers=2
    )

    env_file.write_text("BD_VERIFY_CONNECTIVITY=false\n", encoding="utf-8")
    off = DaskSessionSettings.from_env_prefix("BD_", env_file=env_file)
    assert off.to_session_kwargs()["verify_connectivity"] is False

    env_file.write_text("BD_MIN_WORKERS=0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="min_workers"):
        DaskSessionSettings.from_env_prefix("BD_", env_file=env_file)
