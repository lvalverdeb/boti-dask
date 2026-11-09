# Resilience API

`boti_dask.resilience` provides resilient sync/async wrappers around Dask execution.

## Primary symbols

- `safe_compute(...)`
- `safe_persist(...)`
- `safe_wait(...)`
- `safe_head(...)`
- `safe_gather(...)`
- `async_safe_compute(...)`
- `async_safe_persist(...)`
- `async_safe_wait(...)`
- `async_safe_head(...)`
- `async_safe_gather(...)`
- `dask_is_probably_empty(...)`
- `dask_is_empty(...)`

## Typical usage

```python
from boti_dask import safe_compute, safe_persist, safe_wait

persisted = safe_persist(ddf, dask_client=client)
safe_wait(persisted, dask_client=client, timeout=30)
result = safe_compute(persisted["id"].count(), dask_client=client)
```

## Notes

- Handles recoverable communication errors with a single retry strategy.
- Translates common orphaned graph failures into actionable runtime errors.

