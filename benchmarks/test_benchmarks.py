"""pytest-benchmark suite: wrapper overhead, shared-session churn, frame description.

Run with ``make bench`` (compares against benchmarks/baseline) or
``uv run pytest benchmarks --benchmark-only``. Not collected by the normal
test run: pyproject's ``testpaths`` is ``tests``.
"""

from __future__ import annotations

import dask
import numpy as np
import pandas as pd
import pytest

from boti_dask import DaskSession, describe_frame, safe_compute

_LOCAL = {"n_workers": 1, "threads_per_worker": 1, "processes": False, "dashboard_address": ":0"}
_ROWS = 1_000_000
_SHARED_KEY = "boti-dask-benchmark"


@pytest.fixture(scope="module")
def client():
    with DaskSession(cluster_kwargs=_LOCAL) as live:
        yield live


@pytest.fixture(scope="module")
def delayed_sum():
    return dask.delayed(sum)([1, 2, 3])


@pytest.fixture(scope="module")
def million_row_frame() -> pd.DataFrame:
    rng = np.random.default_rng(0)
    return pd.DataFrame(
        {"id": np.arange(_ROWS), "value": rng.random(_ROWS), "label": rng.choice(["a", "b"], _ROWS)}
    )


@pytest.mark.benchmark(group="compute")
def test_bare_client_compute(benchmark, client, delayed_sum):
    assert benchmark(lambda: client.compute(delayed_sum).result()) == 6


@pytest.mark.benchmark(group="compute")
def test_safe_compute(benchmark, client, delayed_sum):
    assert benchmark(lambda: safe_compute(delayed_sum, dask_client=client)) == 6


@pytest.mark.benchmark(group="session")
def test_shared_session_acquire_release(benchmark, client):
    anchor = DaskSession(shared=True, shared_key=_SHARED_KEY, cluster_kwargs=_LOCAL)
    anchor.open()

    def round_trip() -> None:
        holder = DaskSession(shared=True, shared_key=_SHARED_KEY, verify_connectivity=False)
        holder.open()
        holder.close()

    try:
        benchmark(round_trip)
    finally:
        anchor.close()


@pytest.mark.benchmark(group="diagnostics")
def test_describe_frame_1m_rows(benchmark, million_row_frame):
    result = benchmark(describe_frame, million_row_frame)
    assert result == {"engine": "pandas", "rows": _ROWS, "columns": 3}
