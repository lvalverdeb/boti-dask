# boti-dask

Dask sessions, resilient execution and cluster utilities for the Boti ecosystem.

`import boti_dask` costs about two milliseconds and imports no Dask; the first use of a
Dask-backed name (`DaskSession`, `safe_compute`, …) imports `distributed`.

## Install

```bash
uv add boti-dask                 # core: boti, dask[dataframe,distributed], pandas
uv add 'boti-dask[diagnostics]'  # numpy, polars, pyarrow for describe_frame on those engines
uv add 'boti-dask[k8s]'          # dask-kubernetes       -> cluster_kind="kubernetes"
uv add 'boti-dask[slurm]'        # dask-jobqueue         -> cluster_kind="slurm"
uv add 'boti-dask[coiled]'       # coiled                -> cluster_kind="coiled"
uv add 'boti-dask[otel]'         # OpenTelemetryProvider
```

## Three things it does

**A verified session.** `dask_session` opens a client against a scheduler address, a cluster
backend or a local cluster, checks that the scheduler answers, that workers are present and
that a task runs, and raises `DaskSessionUnavailable` within a deadline instead of hanging.

```python
from boti_dask import dask_session, safe_compute
import dask

with dask_session(cluster_kwargs={"n_workers": 2, "processes": False}) as client:
    print(safe_compute(dask.delayed(lambda: 6 * 7)(), dask_client=client))
```

**Resilient execution.** `safe_compute`, `safe_persist`, `safe_wait`, `safe_head` and
`safe_gather` (and their `async_safe_*` twins) retry once on a recoverable Dask error, re-resolve
the client, and turn an orphaned graph into a readable `RuntimeError`.

**Configuration from the environment.** One prefix selects the scheduler or cluster kind, the
connectivity check, the watchdog, telemetry and code uploads:

```python
from boti_dask import dask_session_from_env_prefix

with dask_session_from_env_prefix("ETL_DASK_", env_file=".env") as client:
    ...
```

```bash
ETL_DASK_CLUSTER_KIND=slurm
ETL_DASK_CLUSTER_KWARGS={"queue": "batch", "cores": 8}
ETL_DASK_WATCHDOG_INTERVAL=30
ETL_DASK_TELEMETRY=otel
ETL_DASK_UPLOAD=./src/etl_jobs,rich>=13
```

## Where next

- [Session](guides/session.md): lifecycle, shared sessions, connectivity verification.
- [Resilience](guides/resilience.md): the `safe_*` wrappers and what they recover from.
- [Clusters](guides/clusters.md), [Watchdog](guides/watchdog.md), [Telemetry](guides/telemetry.md),
  [Upload](guides/upload.md): the 1.2 operational features.
- [Primitives](guides/primitives.md): cluster-wide semaphores, locks and queues.
- [API reference](reference/index.md): every module from its docstrings.
- [Stability](stability.md): what is stable, what is experimental, how deprecation works.
- [Changelog](changelog.md).
