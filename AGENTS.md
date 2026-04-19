# AGENTS.md
## Architecture Overview
**boti-dask** provides three core modules for Dask distributed computing within the Boti ecosystem:
- **`session.py`**: Manages Dask client/cluster lifecycle with shared-session support using `DaskSession` and `dask_session()` helpers
- **`resilience.py`**: Fault-tolerant computation wrappers (`safe_compute`, `safe_persist`, `safe_wait`, `safe_head`, `safe_gather`) with automatic retry on recoverable errors and fallback to local computation
- **`diagnostics.py`**: Graph inspection and frame introspection utilities (`inspect_graph`, `describe_frame`) for runtime diagnostics
All public APIs are re-exported in `__init__.py` with explicit `__all__` list.
## Critical Patterns
### Session Management: Shared vs Managed
- **Managed sessions** (`shared=False`, default): DaskSession owns client/cluster lifecycle; closes on context exit
- **Shared sessions** (`shared=True`): Multiple contexts reuse same client via registry lookup (by factory/kwargs); only closes when ref_count reaches zero
  - Key collision logic uses stable repr of kwargs + factory name
  - Use explicit `shared_key` to force identity across different configurations
  - See `_SHARED_SESSION_REGISTRY` and `_register_shared_session()` in `session.py:109-116`
### Resilience: Error Recovery & Locality
- `safe_*` functions resolve active client via `_resolve_active_client()`: tries passed `dask_client` param, then attempts `get_client()` from context
- On `RECOVERABLE_DASK_ERRORS` (CommClosedError, StreamClosedError, TimeoutError, ConnectionError, OSError): retry once with rebound client
- **Orphaned graph detection**: Dask collections created in one session but accessed after it closes raise "Missing dependency" → translated to helpful RuntimeError about rebuilding
- DataFrame-specific: Cannot fallback to local for Dask DataFrame-like collections; must have active client or rebuild
- `safe_persist()` tracks persisted collections via `_track_persisted_collection()` and `_PERSISTED_COLLECTION_REGISTRY` to warn on session close
### Module-Level Registries
All registries are intentionally module-level (not instance state) so resilience helpers can query session state:
- `_MANAGED_CLIENT_REGISTRY[id(client)]`: Weakref of currently open managed clients
- `_PERSISTED_COLLECTION_REGISTRY[client_id]`: Weakref list of persisted collections per client
- `_SHARED_SESSION_REGISTRY[key]`: Dict with `client`, `cluster`, `ref_count` for shared sessions
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
Recommended Dask config profile in `session.py:21-31`:
- Worker memory: `0.6` target, `0.7` spill, `0.8` pause thresholds
- Distributed: `20s` connect, `120s` tcp timeouts; `3` allowed failures; `60s` lost-worker timeout
- DataFrame shuffle: `tasks` method (not `p2p`)
Apply via `apply_recommended_dask_config(**overrides)` context manager or query with `recommended_dask_config(overrides={...})`.
## Integration Points
### External Dependencies
- **dask[dataframe,distributed]** ≥2026.3.0: Core distributed computing
- **numpy, pandas, polars, pyarrow**: Data frame inspection support (describe_frame handles all four)
- **distributed.comm.core.CommClosedError, tornado.iostream.StreamClosedError**: Recoverable error detection
### Client Detection & Context
- Optional `dask_client` kwarg on all `safe_*` functions and `dask_is_empty()`
- If not provided, attempts `get_client()` from thread-local Dask context
- Gracefully degrades to local computation or raises RuntimeError for unsupported operations
### Async Support
All `safe_*` operations have `async_` variants that wrap sync via `asyncio.to_thread()` (e.g., `async_safe_compute()`). Used by `UniqueValuesExtractor.extract_unique_values()` which parallelizes multi-column extraction via `asyncio.gather()`.

### Serialization & Pickling

- **No explicit pickle handling**: boti-dask delegates serialization entirely to Dask's distributed client
- Dask uses `cloudpickle` for serialization (handles functions, lambdas, custom classes better than stdlib pickle)
- **Dask collections** (DataFrame, Delayed, etc.) are never pickled directly—only their task graphs are serialized
- **Regular objects** must be pickleable for distributed execution; non-pickleable objects automatically fail over to local scheduler
- Local fallback via `scheduler="threads"` avoids serialization entirely (single-machine evaluation)
- See `_sync_local_compute()`, `_sync_local_persist()` in `resilience.py:107-135` for fallback implementation

## Code Style & Conventions
- **Python 3.13+**: Requires `from __future__ import annotations`
- **Internal helpers**: Prefixed with `_`, module-level when shared state is needed (not methods)
- **Logging**: Optional `logger` kwarg; uses duck-typing (`logger.info()`, `logger.warning()`, etc.)
- **Type checks**: Use `hasattr(obj, "_meta")` for Dask DataFrame-like detection; `_has_dask_graph()` for delayed/lazy collections
- **Error translation**: `_translate_orphaned_graph_error()` converts vague "Missing dependency" → actionable guidance
## Discovery Notes
- **Weak references**: Orphaned collections/clients cleaned up automatically via weakref callbacks
- **Slot dataclass**: `DaskSession` uses `@dataclass(slots=True)` for memory efficiency
- **Stable repr**: `_stable_mapping_repr()` sorts kwargs for deterministic shared-session key generation
- **Async-to-sync**: All async ops use `asyncio.to_thread()` for thread-safety with distributed client
