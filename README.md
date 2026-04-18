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

