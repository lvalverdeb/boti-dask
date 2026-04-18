# Examples

Run examples from the repository root with:

```bash
uv run python examples/data_facade_dask_resilience.py
uv run python examples/session_from_env_prefix.py
uv run python examples/smoke_all_examples.py
```

The smoke script runs every `*.py` example in this directory (except itself)
and reports a compact pass/fail summary.

`data_facade_dask_resilience.py` demonstrates sync/async `safe_*` helpers,
shared-session reuse, emptiness probes (`dask_is_probably_empty` / `dask_is_empty`),
and `UniqueValuesExtractor` for best-effort unique diagnostics.

