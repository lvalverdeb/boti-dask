# Diagnostics API

`boti_dask.diagnostics` contains reusable diagnostics and metadata utilities.

## Primary symbols

- `inspect_graph(...)`
- `describe_frame(...)`
- `diagnostics_logger(...)`
- `UniqueValuesExtractor`

## Typical usage

```python
from boti_dask import inspect_graph, describe_frame, UniqueValuesExtractor

print(inspect_graph(ddf))
print(describe_frame(ddf))
values = await UniqueValuesExtractor().extract_unique_values(ddf, "id", limit=100)
```

## Notes

- `inspect_graph` materialises the whole graph, so its cost grows with the task count. It logs
  its metrics line at `INFO` when called directly (`level="debug"` lowers it). The `safe_*`
  wrappers only call it when their logger is enabled for `DEBUG`, and log at that level, so
  an `INFO` logger neither pays for the inspection nor sees the line (dry runs included).
- `describe_frame` supports Dask, pandas, pyarrow, and polars objects.
- `UniqueValuesExtractor` is best-effort and supports optional truncation via `limit`.

