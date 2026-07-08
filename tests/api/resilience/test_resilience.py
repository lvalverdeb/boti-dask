from __future__ import annotations

import gc

import dask
import dask.dataframe as dd
import pandas as pd
import pytest

from boti_dask.resilience import (
    dask_is_empty,
    dask_is_probably_empty,
    inspect_graph,
    safe_compute,
    safe_gather,
    safe_head,
    safe_persist,
    safe_wait,
)


def test_inspect_graph_for_non_dask_object():
    metrics = inspect_graph({"x": 1})
    assert metrics["is_dask"] is False


def test_inspect_graph_for_dask_dataframe():
    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)
    metrics = inspect_graph(frame)
    assert metrics["is_dask"] is True
    assert metrics["npartitions"] == 2


def test_safe_compute_local_delayed():
    delayed_value = dask.delayed(lambda: 21 * 2)()
    assert safe_compute(delayed_value) == 42


def test_safe_gather_local_delayed_sequence():
    values = [dask.delayed(lambda value=value: value * 2)() for value in [1, 2, 3]]
    assert safe_gather(values) == [2, 4, 6]


def test_safe_head_for_dask_dataframe_local():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster
    from boti_dask.session import dask_session

    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3, 4]}), npartitions=2)
    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        with dask_session(scheduler_address=cluster.scheduler_address) as client:
            head = safe_head(frame, n=2, dask_client=client)
    assert list(head["id"]) == [1, 2]


def test_safe_persist_and_wait_with_client():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        from boti_dask.session import dask_session

        with dask_session(scheduler_address=cluster.scheduler_address) as client:
            persisted = safe_persist(frame, dask_client=client)
            safe_wait(persisted, dask_client=client, timeout=10)
            total = safe_compute(persisted["id"].sum(), dask_client=client)
            # Release the persisted collection before the managed session closes
            # so the test does not emit the live-persisted warning.
            del persisted
            gc.collect()

    assert int(total) == 6


def test_dask_empty_heuristics():
    empty_ddf = dd.from_pandas(pd.DataFrame({"id": []}), npartitions=1)
    non_empty_ddf = dd.from_pandas(pd.DataFrame({"id": [1]}), npartitions=1)

    assert dask_is_probably_empty(empty_ddf) is False
    assert dask_is_empty(empty_ddf) is True
    assert dask_is_empty(non_empty_ddf) is False

