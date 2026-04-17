from __future__ import annotations

import dask
import pytest

from boti_dask.session import (
    apply_recommended_dask_config,
    dask_session,
    describe_client,
    recommended_dask_config,
)


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

