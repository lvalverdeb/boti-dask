# AGENTS.md
## Architecture Overview
**boti-dask** provides three core modules for Dask distributed computing within the Boti ecosystem:
- **`session.py`**: Manages Dask client/cluster lifecycle using `DaskSession` (a `ManagedResource` subclass) and `SessionPool` (replaces module-level registries)
- **`resilience.py`**: Fault-tolerant computation wrappers (`safe_compute`, `safe_persist`, `safe_wait`, `safe_head`, `safe_gather`) with automatic retry on recoverable errors and fallback to local computation
- **`diagnostics.py`**: Graph inspection and frame introspection utilities (`inspect_graph`, `describe_frame`) for runtime diagnostics
All public APIs are re-exported in `__init__.py` with explicit `__all__` list.
## Critical Patterns
### Session Management: Shared vs Managed
- **Managed sessions** (`shared=False`, default): DaskSession owns client/cluster lifecycle; closes on context exit
- **Shared sessions** (`shared=True`): Multiple contexts reuse same client via registry lookup (by factory/kwargs); only closes when ref_count reaches zero
  - Key collision logic uses stable repr of kwargs + factory name
  - Use explicit `shared_key` to force identity across different configurations
  - See `SessionPool.register_shared_session()` and `SessionPool.release_shared_session()` in `session.py`
### SessionPool
- `SessionPool` class replaces the previous bare module-level dicts (`_MANAGED_CLIENT_REGISTRY`, `_PERSISTED_COLLECTION_REGISTRY`, `_SHARED_SESSION_REGISTRY`, `_REGISTRY_LOCK`)
- A module-level singleton `pool` is created at import time
- Backward-compat function aliases (`_register_managed_client`, `_track_persisted_collection`, etc.) delegate to `pool`
- Resilience helpers import `pool` directly and call `pool.track_persisted_collection(...)`
### DaskSession as ManagedResource
- `DaskSession` is a `ManagedResource` subclass, inheriting lifecycle hooks, close idempotency, pickle gating, and logger integration
- Context manager (`__enter__`/`__aenter__`) overrides the `@final` ManagedResource versions to return the Dask `Client` instead of `self`, preserving the `with session as client` pattern
- Cleanup logic lives in `_cleanup()` / `_acleanup()`, called by `ManagedResource.close()` / `aclose()`
- `DaskSession.__init__` accepts the same kwargs plus an optional `config: ResourceConfig` and `verbose`/`debug` flags
- Logger integration: `self.logger` is set via `ManagedResource._configure_logger()`; defaults to `None` when no logger is passed (with `skip_logger=True` in `ResourceConfig`)
### Resilience: Error Recovery & Locality
- `safe_*` functions resolve active client via `_resolve_active_client()`: tries passed `dask_client` param, then attempts `get_client()` from context
- On `RECOVERABLE_DASK_ERRORS` (CommClosedError, StreamClosedError, TimeoutError, ConnectionError, OSError): retry once with rebound client
- **Orphaned graph detection**: Dask collections created in one session but accessed after it closes raise "Missing dependency" → translated to helpful RuntimeError about rebuilding
- DataFrame-specific: Cannot fallback to local for Dask DataFrame-like collections; must have active client or rebuild
- `safe_persist()` tracks persisted collections via `pool.track_persisted_collection()` to warn on session close
## Developer Workflows
### Build & Test
```bash
uv sync --dev          # Install with dev deps
uv run pytest -q       # Run tests (quiet mode per tool.pytest.ini_options)
uv run python examples/data_facade_dask_resilience.py  # Run single example
uv run python examples/smoke_all_examples.py           # Smoke test all examples
```
### Key Test Locations
- Migration & export contracts: `tests/api/test_migration_imports.py`
- API-specific tests nested: `tests/api/{session,resilience,diagnostics,runtime}/test_*.py`
- Example smoke tests: `tests/examples/test_examples.py`
### Configuration
Recommended Dask config profile in `session.py`:
- Worker memory: `0.6` target, `0.7` spill, `0.8` pause thresholds
- Distributed: `20s` connect, `120s` tcp timeouts; `3` allowed failures; `60s` lost-worker timeout
- DataFrame shuffle: `tasks` method (not `p2p`)
Apply via `apply_recommended_dask_config(**overrides)` context manager or query with `recommended_dask_config(overrides={...})`.
## Integration Points
### External Dependencies
- **dask[dataframe,distributed]** ≥2026.3.0: Core distributed computing
- **pandas**: Required for all DataFrame operations (transitive dep of dask)
- **boti** ≥0.1.0: Provides `ManagedResource` lifecycle, `Logger`, `ResourceConfig`
- **numpy, polars, pyarrow**: Optional extras (`boti-dask[extra]`), only needed for `describe_frame` (pyarrow, polars) and `_to_int_safe` (numpy)
- **distributed.comm.core.CommClosedError, tornado.iostream.StreamClosedError**: Recoverable error detection
### Client Detection & Context
- Optional `dask_client` kwarg on all `safe_*` functions and `dask_is_empty()`
- If not provided, attempts `get_client()` from thread-local Dask context
- Gracefully degrades to local computation or raises RuntimeError for unsupported operations
### Async Support
All `safe_*` operations have `async_` variants that wrap sync via `asyncio.to_thread()` (e.g., `async_safe_compute()`). Used by `UniqueValuesExtractor.extract_unique_values()` which parallelizes multi-column extraction via `asyncio.gather()`.

### Serialization & Pickling

- **ManagedResource pickle gating**: `DaskSession` inherits `ManagedResource.__getstate__` / `__setstate__` with `allow_pickle` guard and `trusted_unpickle_scope`
- Dask uses `cloudpickle` for serialization (handles functions, lambdas, custom classes better than stdlib pickle)
- **Dask collections** (DataFrame, Delayed, etc.) are never pickled directly—only their task graphs are serialized
- **Regular objects** must be pickleable for distributed execution; non-pickleable objects automatically fail over to local scheduler
- Local fallback via `scheduler="threads"` avoids serialization entirely (single-machine evaluation)
- See `_sync_local_compute()`, `_sync_local_persist()` in `resilience.py` for fallback implementation

## Code Style & Conventions
- **Python 3.10+**: Requires `from __future__ import annotations`
- **Internal helpers**: Prefixed with `_`, module-level when shared state is needed (not methods)
- **Logging**: Optional `logger` kwarg; uses duck-typing (`logger.info()`, `logger.warning()`, etc.)
- **Type checks**: Use `hasattr(obj, "_meta")` for Dask DataFrame-like detection; `_has_dask_graph()` for delayed/lazy collections
- **Error translation**: `_translate_orphaned_graph_error()` converts vague "Missing dependency" → actionable guidance
## Discovery Notes
- **Weak references**: Orphaned collections/clients cleaned up automatically via weakref callbacks
- **ManagedResource subclass**: `DaskSession` replaces `@dataclass(slots=True)` with `ManagedResource` inheritance for unified lifecycle
- **Stable repr**: `_stable_mapping_repr()` sorts kwargs for deterministic shared-session key generation
- **Async-to-sync**: All async ops use `asyncio.to_thread()` for thread-safety with distributed client
