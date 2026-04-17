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

- `describe_frame` supports Dask, pandas, pyarrow, and polars objects.
- `UniqueValuesExtractor` is best-effort and supports optional truncation via `limit`.

