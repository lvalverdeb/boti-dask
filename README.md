# boti-dask

Dask runtime/session/resilience utilities for the Boti ecosystem.

## API sections

- Session: `docs/api/session.md`
- Resilience: `docs/api/resilience.md`
- Diagnostics: `docs/api/diagnostics.md`
- Migration from `boti_data.distributed`: `docs/migration/from-boti-data-distributed.md`

## Phase-1 scope

This initial bootstrap provides:

- `DaskSession` and `dask_session(...)` helpers
- `DaskSessionSettings` and `dask_session_from_env_prefix(...)` helpers
- shared-session lifecycle support
- recommended Dask config profile helpers
- resilient execution wrappers:
  - `safe_compute`, `safe_persist`, `safe_wait`, `safe_head`, `safe_gather`
  - async counterparts
- diagnostics helpers:
  - `inspect_graph`, `describe_frame`, `diagnostics_logger`
- Dask emptiness and unique-value helpers:
  - `dask_is_probably_empty`, `dask_is_empty`, `UniqueValuesExtractor`

## Quick start

```python
from boti_dask import dask_session, inspect_graph, safe_compute
import dask

with dask_session(cluster_kwargs={"n_workers": 1, "threads_per_worker": 1, "processes": False, "dashboard_address": ":0"}) as client:
    value = dask.delayed(lambda: 6 * 7)()
    print(safe_compute(value, dask_client=client))
    print(inspect_graph(value))
```

If `dashboard_address` is omitted for local managed sessions, `boti-dask` defaults it to `":0"`.

Load session defaults from prefixed environment variables:

```python
from boti_dask import DaskSessionSettings, dask_session_from_env_prefix

settings = DaskSessionSettings.from_env_prefix("BOTI_DASK_", env_file=".env")
session = dask_session_from_env_prefix("BOTI_DASK_", env_file=".env", verify_connectivity=True)
```

## Distributed computing scenarios

### Multi-worker cluster with grouped aggregation

Launch a 4-worker cluster, distribute a DataFrame across 8 partitions, and
run a grouped sum that executes in parallel across all workers:

```python
from boti_dask import dask_session, describe_client, safe_persist, safe_compute
import dask.dataframe as dd, pandas as pd

with dask_session(
    cluster_kwargs={"n_workers": 4, "threads_per_worker": 2, "processes": False, "dashboard_address": ":0"},
) as client:
    print(describe_client(client))  # "workers": 4, "threads": 8

    ddf = dd.from_pandas(pd.DataFrame({"v": range(10_000), "g": ["X","Y","Z"] * 3334}), npartitions=8)
    persisted = safe_persist(ddf, dask_client=client)
    grouped = safe_compute(persisted.groupby("g")["v"].sum(), dask_client=client)
    print(grouped)  # {"X": 16665000, "Y": 16665000, "Z": 16665000}
```

### Shared session with nested contexts

Multiple ``with`` blocks share the same Dask client via reference counting.
The client stays alive until the last holder exits:

```python
from boti_dask import dask_session

kwargs = {"n_workers": 2, "threads_per_worker": 1, "processes": False, "dashboard_address": ":0"}
with dask_session(cluster_kwargs=kwargs, shared=True, shared_key="my-cluster") as outer:
    with dask_session(shared=True, shared_key="my-cluster") as inner:
        print(outer is inner)  # True
    # outer still alive here
```

### Async distributed pipeline

Persist, wait, head, compute, and gather — all through async-safe wrappers:

```python
import asyncio
from boti_dask import dask_session, async_safe_persist, async_safe_head, async_safe_gather
import dask.dataframe as dd, pandas as pd, dask

async def pipeline():
    ddf = dd.from_pandas(pd.DataFrame({"id": range(100)}), npartitions=4)
    with dask_session(cluster_kwargs={"n_workers": 2, "processes": False, "dashboard_address": ":0"}) as client:
        p = await async_safe_persist(ddf, dask_client=client)
        preview = await async_safe_head(p, n=3, dask_client=client)
        gathered = await async_safe_gather([dask.delayed(lambda: 42)()], dask_client=client)
        return preview["id"].tolist(), gathered

asyncio.run(pipeline())
```

### Orphaned graph detection

When a Dask collection outlives the session that created it, operations
fail with a clear error instead of a cryptic Dask traceback:

```python
with dask_session(cluster_kwargs={...}) as client:
    persisted = safe_persist(ddf, dask_client=client)
# session closed — persisted is now orphaned
safe_head(persisted)  # RuntimeError: orphaned from its original client/worker state
```

Pass an external Dask `Client` to keep the client alive across session boundaries:

```python
from dask.distributed import LocalCluster, Client
cluster = LocalCluster(...)
external = Client(cluster)
with dask_session(client=external) as session_client:
    result = safe_compute(dask.delayed(lambda: 42)(), dask_client=session_client)
# external.close()  # manual lifecycle
```

## Development

```bash
uv sync --dev
uv run pytest -q
```

## Examples

```bash
uv run python examples/data_facade_dask_resilience.py
uv run python examples/smoke_all_examples.py
```

See `examples/README.md` for details.
