# boti-dask

Dask sessions, resilient execution and cluster utilities for the Boti ecosystem.
`import boti_dask` takes about two milliseconds and imports no Dask until a Dask-backed name is used.

Documentation: <https://lvalverdeb.github.io/boti-dask/> (built from `docs/` with mkdocs-material;
`uv run mkdocs serve` renders it locally).

## Install

```bash
uv add boti-dask                 # core: boti, dask[dataframe,distributed], pandas
uv add 'boti-dask[diagnostics]'  # numpy, polars, pyarrow for describe_frame on those engines
uv add 'boti-dask[k8s]'          # dask-kubernetes  -> cluster_kind="kubernetes"
uv add 'boti-dask[slurm]'        # dask-jobqueue    -> cluster_kind="slurm"
uv add 'boti-dask[coiled]'       # coiled           -> cluster_kind="coiled"
uv add 'boti-dask[otel]'         # OpenTelemetryProvider
```

## A verified session

```python
import dask
from boti_dask import dask_session, safe_compute

with dask_session(cluster_kwargs={"n_workers": 2, "processes": False}) as client:
    print(safe_compute(dask.delayed(lambda: 6 * 7)(), dask_client=client))
```

Before the client is handed out the scheduler is pinged, a worker is awaited and one task runs,
within 10 seconds by default; a dead or empty cluster raises `DaskSessionUnavailable` instead of
hanging. Tune it with `verify_connectivity=ConnectivityCheck(timeout=..., min_workers=...)`.

## A shared session with resilient execution

```python
import dask.dataframe as dd, pandas as pd
from boti_dask import dask_session, safe_persist, safe_compute

kwargs = {"n_workers": 2, "threads_per_worker": 1, "processes": False}
with dask_session(cluster_kwargs=kwargs, shared=True, shared_key="etl") as outer:
    ddf = dd.from_pandas(pd.DataFrame({"v": range(10_000), "g": ["X", "Y"] * 5_000}), npartitions=4)
    persisted = safe_persist(ddf, dask_client=outer)
    with dask_session(shared=True, shared_key="etl") as inner:   # same client, ref-counted
        print(safe_compute(persisted.groupby("g")["v"].sum(), dask_client=inner))
```

`safe_compute`, `safe_persist`, `safe_wait`, `safe_head` and `safe_gather` (and their
`async_safe_*` twins) retry once on a recoverable Dask error, re-resolve the client, and turn an
orphaned graph into a readable `RuntimeError`.

## Everything from one environment prefix

```python
from boti_dask import dask_session_from_env_prefix

with dask_session_from_env_prefix("ETL_DASK_", env_file=".env") as client:
    ...
```

```bash
ETL_DASK_CLUSTER_KIND=slurm                          # or kubernetes, coiled, local
ETL_DASK_CLUSTER_KWARGS={"queue": "batch", "cores": 8}
ETL_DASK_WATCHDOG_INTERVAL=30                        # probe every 30 s, rebind after 3 misses
ETL_DASK_TELEMETRY=otel                              # spans around safe_*, counters and gauges
ETL_DASK_UPLOAD=./src/etl_jobs,rich>=13              # shipped to workers once per scheduler
```

## Guides and reference

- Session, resilience, diagnostics and primitives: `docs/guides/`.
- Clusters, watchdog, telemetry and upload (new in 1.2, experimental): `docs/guides/`.
- API reference from the docstrings: `docs/reference/`.
- Stability tiers (`boti_dask.STABLE` / `boti_dask.EXPERIMENTAL`) and deprecation: `docs/stability.md`.
- Runnable scripts: `examples/` (`uv run python examples/smoke_all_examples.py`).
- Changes: `CHANGELOG.md`.

## Compatibility

Every push, and weekly, the compatibility-matrix workflow runs the test suite against the floor,
locked and latest releases of Dask and boti. The floors are the oldest versions the suite passes
on; the locked versions are what `uv.lock` resolves; latest is whatever PyPI serves that day.

| Dependency | Floor | Locked | Latest |
| --- | --- | --- | --- |
| `dask[dataframe,distributed]` | 2026.3.0 | see `uv.lock` | weekly |
| `boti` | 1.0.2 | 1.2.0 | weekly |

## Development

```bash
uv sync --group dev --group docs
make verify          # ruff + pytest, what CI runs
make bench           # pytest-benchmark against benchmarks/baseline/local
uv run mkdocs serve  # docs at http://127.0.0.1:8000
```
