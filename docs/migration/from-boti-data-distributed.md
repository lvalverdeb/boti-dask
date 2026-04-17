# Migration from `boti_data.distributed`

This guide helps migrate imports from `boti_data.distributed` to `boti_dask`.

## Import mapping

| Old import path | New import path |
| --- | --- |
| `from boti_data.distributed import DaskSession` | `from boti_dask import DaskSession` |
| `from boti_data.distributed import dask_session` | `from boti_dask import dask_session` |
| `from boti_data.distributed import safe_compute` | `from boti_dask import safe_compute` |
| `from boti_data.distributed import safe_persist` | `from boti_dask import safe_persist` |
| `from boti_data.distributed import safe_wait` | `from boti_dask import safe_wait` |
| `from boti_data.distributed import safe_head` | `from boti_dask import safe_head` |
| `from boti_data.distributed import safe_gather` | `from boti_dask import safe_gather` |
| `from boti_data.distributed import async_safe_compute` | `from boti_dask import async_safe_compute` |
| `from boti_data.distributed import async_safe_persist` | `from boti_dask import async_safe_persist` |
| `from boti_data.distributed import async_safe_wait` | `from boti_dask import async_safe_wait` |
| `from boti_data.distributed import async_safe_head` | `from boti_dask import async_safe_head` |
| `from boti_data.distributed import async_safe_gather` | `from boti_dask import async_safe_gather` |
| `from boti_data.distributed import inspect_graph` | `from boti_dask import inspect_graph` |
| `from boti_data.distributed import describe_frame` | `from boti_dask import describe_frame` |
| `from boti_data.distributed import diagnostics_logger` | `from boti_dask import diagnostics_logger` |
| `from boti_data.distributed import UniqueValuesExtractor` | `from boti_dask import UniqueValuesExtractor` |
| `from boti_data.distributed import dask_is_probably_empty` | `from boti_dask import dask_is_probably_empty` |
| `from boti_data.distributed import dask_is_empty` | `from boti_dask import dask_is_empty` |
| `from boti_data.distributed import apply_recommended_dask_config` | `from boti_dask import apply_recommended_dask_config` |

## Before/after

### Before

```python
from boti_data.distributed import dask_session, safe_compute, inspect_graph
```

### After

```python
from boti_dask import dask_session, safe_compute, inspect_graph
```

## Compatibility strategy

- Keep call sites unchanged beyond import path updates.
- Migrate module by module to keep PRs small and easy to validate.

