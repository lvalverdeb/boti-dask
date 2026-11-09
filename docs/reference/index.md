# API reference

Generated from the docstrings with mkdocstrings. The public surface is what `boti_dask` exports
lazily (`boti_dask.__all__`); the module pages also show the helpers behind it.

| Module | Holds |
|--------|-------|
| [`session`](session.md) | `DaskSession`, `dask_session`, `describe_client`, `current_client_summary` |
| [`session_settings`](session_settings.md) | `DaskSessionSettings` and the `{PREFIX}…` environment loading |
| [`session_helpers`](session_helpers.md) | `ConnectivityCheck`, `DaskSessionUnavailable`, recommended config |
| [`session_pool`](session_pool.md) | `SessionPool`: shared sessions, persisted collections, primitives, rebinds |
| [`resilience`](resilience.md) | `RECOVERABLE_DASK_ERRORS`, `dask_is_probably_empty` |
| [`resilience_ops`](resilience_ops.md) | `safe_compute`, `safe_persist`, `safe_wait`, `safe_head`, `safe_gather` |
| [`resilience_async`](resilience_async.md) | `dask_is_empty` and the `async_safe_*` wrappers |
| [`diagnostics`](diagnostics.md) | `inspect_graph`, `describe_frame`, `UniqueValuesExtractor` |
| [`primitives`](primitives.md) | `cluster_semaphore`, `cluster_lock`, `cluster_queue` |
| [`clusters`](clusters.md) | `cluster_factory`, `make_cluster`, `CLUSTER_KINDS` |
| [`watchdog`](watchdog.md) | `Watchdog`, `WatchdogConfig` |
| [`telemetry`](telemetry.md) | `TelemetryProvider` and the three providers |
| [`upload`](upload.md) | `sync_uploads`, `classify_payload` |
