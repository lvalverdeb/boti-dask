# Examples

Run examples from the repository root with:

```bash
uv run python examples/<script>.py
uv run python examples/smoke_all_examples.py   # run all examples in sequence
```

The smoke script runs every `*.py` example in this directory (except itself)
and reports a compact pass/fail summary.

## Available examples

| File | Demonstrates |
|------|-------------|
| `data_facade_dask_resilience.py` | Sync/async `safe_*` helpers, shared-session reuse, emptiness probes (`dask_is_probably_empty` / `dask_is_empty`), `UniqueValuesExtractor`, `inspect_graph` |
| `async_distributed_pipeline.py` | Full async pipeline: `async_safe_persist` → `async_safe_wait` → `async_safe_head` → `async_safe_compute` → `async_safe_gather`, plus `UniqueValuesExtractor` for concurrent column extraction |
| `multi_worker_cluster.py` | 4-worker / 8-thread cluster with `apply_recommended_dask_config`, distributed `groupby` aggregation, `dask_is_empty` probe, and `describe_client` diagnostics |
| `shared_session_lifecycle.py` | Shared-session ref-counting across nested contexts, client reuse verification, and auto-cleanup when the last holder exits |
| `orphaned_graph_scenarios.py` | 4 scenarios: orphaned graph from a managed session, external client that survives session close, shared-session staggered release, and local fallback when no client is available |
| `session_from_env_prefix.py` | Loading session defaults from prefixed environment variables / dotenv files |
