"""Named cluster backends resolve lazily and report the extra that installs them."""

from __future__ import annotations

import sys
import types

import pytest

from boti_dask import DaskSession, DaskSessionSettings, clusters
from boti_dask import session as session_module


class FakeCluster:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class StubClient:
    def __init__(self, cluster, **_kwargs) -> None:
        self.cluster = cluster
        self.status = "running"

    def close(self) -> None:
        self.status = "closed"


@pytest.fixture
def fake_slurm(monkeypatch):
    module = types.ModuleType("dask_jobqueue")
    module.SLURMCluster = FakeCluster
    monkeypatch.setitem(sys.modules, "dask_jobqueue", module)
    return module


def test_cluster_kinds_are_the_four_backends():
    assert clusters.CLUSTER_KINDS == ("local", "kubernetes", "slurm", "coiled")


def test_local_kind_is_distributed_localcluster():
    from dask.distributed import LocalCluster

    assert clusters.cluster_factory("local") is LocalCluster


def test_factory_resolves_backend_lazily_and_builds_cluster(fake_slurm):
    assert clusters.cluster_factory(" SLURM ") is FakeCluster
    cluster = clusters.make_cluster("slurm", queue="debug", cores=4)
    assert isinstance(cluster, FakeCluster)
    assert cluster.kwargs == {"queue": "debug", "cores": 4}


def test_unknown_kind_lists_the_known_ones():
    with pytest.raises(ValueError, match="local, kubernetes, slurm, coiled"):
        clusters.cluster_factory("yarn")


@pytest.mark.parametrize(
    ("kind", "package", "extra"),
    [
        ("kubernetes", "dask_kubernetes", "k8s"),
        ("slurm", "dask_jobqueue", "slurm"),
        ("coiled", "coiled", "coiled"),
    ],
)
def test_missing_backend_names_the_extra(monkeypatch, kind, package, extra):
    monkeypatch.setitem(sys.modules, package, None)  # makes the import fail
    monkeypatch.delitem(sys.modules, f"{package}.operator", raising=False)
    with pytest.raises(ModuleNotFoundError, match=rf"boti-dask\[{extra}\]") as info:
        clusters.cluster_factory(kind)
    assert package in str(info.value)


def test_settings_read_cluster_kind_from_env(monkeypatch, fake_slurm):
    monkeypatch.setenv("DASK_SESSION_CLUSTER_KIND", "slurm")
    settings = DaskSessionSettings.from_env_prefix("DASK_SESSION_")
    assert settings.cluster_kind == "slurm"
    assert settings.to_session_kwargs()["cluster_factory"] is FakeCluster


def test_settings_reject_unknown_cluster_kind():
    with pytest.raises(ValueError, match="Unknown cluster kind"):
        DaskSessionSettings(cluster_kind="yarn")


def test_scheduler_address_wins_over_cluster_kind():
    settings = DaskSessionSettings(scheduler_address="tcp://s:8786", cluster_kind="slurm")
    assert "cluster_factory" not in settings.to_session_kwargs()


def test_session_builds_the_named_cluster(monkeypatch, fake_slurm):
    monkeypatch.setattr(session_module, "Client", StubClient)
    session = DaskSession(
        cluster_factory=clusters.cluster_factory("slurm"),
        cluster_kwargs={"cores": 2},
        verify_connectivity=False,
    )
    client = session.open()
    try:
        assert isinstance(client.cluster, FakeCluster)
        assert client.cluster.kwargs == {"cores": 2}
    finally:
        session.close()
    assert client.cluster.close_count == 1
