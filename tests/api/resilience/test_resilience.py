from __future__ import annotations

import gc
import logging

import dask
import dask.dataframe as dd
import pandas as pd
import pytest

from boti_dask import resilience as resilience_module
from boti_dask import resilience_ops as resilience_ops_module
from boti_dask.diagnostics import inspect_graph
from boti_dask.resilience import dask_is_probably_empty
from boti_dask.resilience_async import dask_is_empty
from boti_dask.resilience_ops import (
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


def test_resolve_active_client_logs_debug_when_get_client_fails(monkeypatch, caplog):
    """Regression: _resolve_active_client used to swallow get_client()
    failures with a bare `except Exception: return None`. It now logs at
    debug level."""

    def failing_get_client():
        raise RuntimeError("no active client")

    monkeypatch.setattr(resilience_module, "get_client", failing_get_client)

    with caplog.at_level(logging.DEBUG, logger="boti_dask.resilience"):
        result = resilience_ops_module._resolve_active_client()

    assert result is None
    assert any(
        "get_client() failed while resolving active Dask client" in record.message
        for record in caplog.records
    )


def test_to_int_safe_logs_debug_when_numpy_fast_path_fails_for_list(monkeypatch, caplog):
    """Regression: the numpy fast path in _to_int_safe used to swallow
    np.asarray() failures for tuple/list input with a bare `except
    Exception: pass`, silently falling back to the slow path. It now logs
    at debug level while still falling back correctly."""

    def broken_asarray(*_args, **_kwargs):
        raise RuntimeError("numpy broke")

    monkeypatch.setattr(resilience_module.np, "asarray", broken_asarray)

    with caplog.at_level(logging.DEBUG, logger="boti_dask.resilience"):
        result = resilience_module._to_int_safe((5,))

    assert result == 5
    assert any(
        "numpy fast path failed for tuple/list value" in record.message
        for record in caplog.records
    )


def test_to_int_safe_logs_debug_when_numpy_fast_path_fails_for_series(monkeypatch, caplog):
    """Same regression as above, for the pd.Series/Index branch."""

    def broken_asarray(*_args, **_kwargs):
        raise RuntimeError("numpy broke")

    monkeypatch.setattr(resilience_module.np, "asarray", broken_asarray)

    with caplog.at_level(logging.DEBUG, logger="boti_dask.resilience"):
        result = resilience_module._to_int_safe(pd.Series([7]))

    assert result == 7
    assert any(
        "numpy fast path failed for Series/Index value" in record.message
        for record in caplog.records
    )


def test_to_int_safe_logs_debug_when_item_call_fails(caplog):
    """Regression: the `.item()` fallback in _to_int_safe used to swallow
    failures with a bare `except Exception: return default`. It now logs
    at debug level."""

    class BrokenItem:
        def item(self):
            raise RuntimeError("item() broke")

    with caplog.at_level(logging.DEBUG, logger="boti_dask.resilience"):
        result = resilience_module._to_int_safe(BrokenItem(), default=-1)

    assert result == -1
    assert any("value.item() failed" in record.message for record in caplog.records)


def test_dask_is_probably_empty_logs_debug_when_len_fails(caplog):
    """Regression: dask_is_probably_empty used to swallow len() failures
    with a bare `except Exception: return False`. It now logs at debug
    level."""

    class BadLen:
        def __len__(self):
            raise RuntimeError("len() broke")

    with caplog.at_level(logging.DEBUG, logger="boti_dask.resilience"):
        result = dask_is_probably_empty(BadLen())

    assert result is False
    assert any(
        "len() failed in dask_is_probably_empty" in record.message
        for record in caplog.records
    )

