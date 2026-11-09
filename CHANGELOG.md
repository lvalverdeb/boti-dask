# Changelog

All notable changes to boti-dask are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Behaviour changes are in **bold**
under *Changed*.

## [Unreleased]

### Security

- **`{PREFIX}UPLOAD` keeps extras and version ranges in one entry.** The value
  was split on every comma, so `pkg[a,b]>=1,<2` became four entries. Commas
  inside brackets or followed by a version operator (`<`, `>`, `=`, `!`, `~`)
  no longer split; a value starting with `[` is read as a JSON list of strings.
  `./src/etl_jobs,rich>=13` parses as before.
- **A directory upload is resolved before it is zipped.** `.` or `..` has no
  name of its own, so the archive held entries with a leading slash that no
  worker could import; the resolved directory's name is used instead, and the
  filesystem root is refused with a `ValueError`.
- **Derived shared keys, upload paths and settings kwargs stay out of logs and
  the scheduler dataset.** The `Reusing shared Dask session` and `Rebound Dask
  session` lines log the session `namespace` (the explicit `shared_key`, or a
  digest of the derived key, which embeds `repr(client_kwargs)`) rather than
  the key text. Entries of the `boti-dask:uploads` dataset, readable by every
  client of the scheduler, are now `kind:basename@digest` rather than the
  local absolute path; the session log still names the full source.
  `repr(DaskSessionSettings)` shows the key names of `cluster_kwargs` and
  `client_kwargs` but not their values.
- **`safe_*` inspect the graph only when their logger is enabled for `DEBUG`.**
  `inspect_graph` materialises the whole graph, and the wrappers ran it on every
  call (dry runs included) to log a line at `INFO`. They now skip it unless the
  logger would emit `DEBUG`, and log at that level; `inspect_graph` gains a
  `level` keyword (default `"info"`) for direct use.
- **`{PREFIX}CLUSTER_KWARGS` accepts only sizing, placement and naming keys**
  (`ENV_CLUSTER_KWARGS`). Dask cluster classes also take keys that load code
  at start-up (`preload`, `worker_class`, `security`, job-script prologues),
  and an environment file is not where that decision belongs. Other keys are
  refused with an error naming the accepted set; pass them as
  `DaskSession(cluster_kwargs=...)` in code.
- **An upload entry that names a missing path is an error.** It was silently
  reclassified as a pip requirement and installed on every worker, so a
  mistyped path became a package install. Requirement strings must parse as
  PEP 508 (`packaging.requirements.Requirement`); option lines, `${VAR}`
  expansion and direct-URL requirements are refused.
- **A shared session is rebound once, for every holder of its key.** The pool
  entry now records its holders; a rebind points each of them at the new
  client, so the other holders' watchdogs probe the replacement instead of
  the client that was just closed and rebinding it in turn. The watchdog
  passes the client it probed as `rebind(stale=...)`, which is a no-op when
  that client has already been replaced.
- **`DaskSession.rebind()` swaps only on success.** The replacement is opened
  (and verified, when verification is on) before the old client is closed; a
  failed reconnect raises and leaves the session on its previous client, open
  and registered. The watchdog then keeps probing, doubling its interval after
  each failed rebind up to `WatchdogConfig.max_backoff` (default 300 s), rather
  than stopping. `Watchdog.rebind_failures` and `Watchdog.delay` expose the
  state; `rebind_failed` events carry `next_probe_in`.
- **`safe_*` no longer fall back to local compute in place of a cluster.** A
  `dask_client` that was passed but is not running, with no running
  replacement discoverable, is a `RuntimeError`; so is losing the client
  mid-operation without a replacement to retry on (previously only
  DataFrame-like collections raised; delayed objects and futures were computed
  on the calling host). Calls that never named a client still run locally.
- **Cluster primitives refuse a client that is not running** (`RuntimeError`)
  instead of quietly returning a process-local lock, semaphore or queue that
  gives no cluster-wide exclusion.
- **A semaphore re-requested with a different limit is a `ValueError`**, on
  the cluster and in the process-local fallback alike; `strict=False` adopts
  the existing limit with a warning, as before.
- `cluster_semaphore`, `cluster_lock` and `cluster_queue` take `namespace`,
  prefixed into the scheduler name (`boti-dask:<namespace>:<name>`), and
  `DaskSession.namespace` supplies one (the `shared_key`, else a digest unique
  to the session) so two sessions on one scheduler do not share a primitive
  by accident.
- **Symbolic links inside an uploaded directory are not shipped.** A link in
  the tree used to send its target's bytes to every worker; links to files
  and directories are now skipped, and every file must resolve inside the
  resolved root.

## [1.2.0] - 2026-11-09

### Added
- `boti_dask.clusters`: `cluster_factory(kind)`, `make_cluster(kind, **kwargs)` and `CLUSTER_KINDS` select `local`, `kubernetes` (`dask-kubernetes`), `slurm` (`dask-jobqueue`) or `coiled` by name, importing the backend on first use. A missing backend raises `ModuleNotFoundError` naming the extra (`k8s`, `slurm`, `coiled`). `DaskSessionSettings.cluster_kind` / `{PREFIX}CLUSTER_KIND` maps to `cluster_factory`.
- `boti_dask.watchdog`: `WatchdogConfig` and `Watchdog` probe the client on a daemon thread after open and call the new `DaskSession.rebind()` after `failures_before_rebind` consecutive failures. A managed session reopens; a shared session swaps the pool entry keeping its holders. Events reach the logger and an `on_event` callback; `SessionPool.rebind_count()` counts rebinds. `DaskSession(watchdog=...)`, `{PREFIX}WATCHDOG_INTERVAL`, `{PREFIX}WATCHDOG_FAILURES`.
- `boti_dask.telemetry`: `TelemetryProvider` protocol with `NoopProvider` (default), `InMemoryProvider` and `OpenTelemetryProvider` (`otel` extra). Spans around every `safe_*` and `async_safe_*` call; counters `retries`, `rebinds`, `recoverable_errors`; gauges `workers`, `task_count`, `persisted_collections` from `DaskSession.record_gauges()`. `DaskSession(telemetry=...)`, `configure_telemetry()`, `{PREFIX}TELEMETRY=none|memory|otel`.
- `boti_dask.upload`: `DaskSession(upload=[...])` / `{PREFIX}UPLOAD` ships `.py`/`.zip`/`.egg` files, deterministically zipped directories, pure-Python wheels and pip requirement strings to the workers on open and after a rebind, skipping payloads whose SHA-256 the scheduler dataset `boti-dask:uploads` already holds. `sync_uploads(client, items)` does the same for any client.
- `boti_dask.STABLE` and `boti_dask.EXPERIMENTAL` name each export's tier, and `boti_dask.deprecated(name, removal, replacement=...)` emits the `DeprecationWarning` used for transitions. The four modules above are experimental in 1.2.
- A pytest-benchmark suite (`benchmarks/`, `make bench`) with a local baseline and a Benchmarks workflow; `safe_compute` adds about 2% to a bare `client.compute` locally.
- A mkdocs-material site (`docs/`, `uv run mkdocs serve`) with an mkdocstrings reference, deployed per minor version by mike on release.

### Changed
- **The `extra` optional dependency is now `diagnostics`**; `boti-dask[extra]` still resolves, as an alias of `boti-dask[diagnostics]`.
- `DaskSession.__init__` accepts `watchdog`, `telemetry` and `upload`; existing arguments are unchanged.
- `SessionPool.debug_set_shared_ref_count` is `set_shared_ref_count`.
- The README is shorter; guides live in `docs/guides/` and the reference in `docs/reference/`.

## [1.1.0] - 2026-11-09

### Added
- `cluster_semaphore`, `cluster_lock` and `cluster_queue`: coordination primitives shared by name across the cluster when a running client exists (passed in, or discovered inside a worker task) and across the process otherwise, so dependents such as boti-data's partition fetch gate write one code path and never import `distributed`. Primitives obtained through a client are closed with it. A semaphore re-requested with a different limit keeps the first creator's limit and logs a warning.
- `ConnectivityCheck` and `DaskSessionUnavailable`. `DaskSessionSettings` reads `{PREFIX}CONNECTIVITY_TIMEOUT` and `{PREFIX}MIN_WORKERS`.
- A compatibility-matrix workflow that tests the floor, locked and latest versions of Dask and boti on every push, weekly, and on `repository_dispatch` type `upstream-release`.
- This changelog.

### Changed
- **`DaskSession` verifies connectivity by default**: a scheduler ping, a wait for `min_workers` (1) and one round-trip task, all within 10 s; a connect to a `scheduler_address` gets the same deadline unless `client_kwargs` sets `timeout`. Failure raises `DaskSessionUnavailable` naming the stage instead of hanging. Pass `verify_connectivity=False` to skip (for example around a stub client), or a `ConnectivityCheck(timeout=..., min_workers=...)` to tune it. `DaskSessionSettings.verify_connectivity` now defaults to `True`.
- **`import boti_dask` no longer imports Dask.** Public names resolve lazily; `DaskSessionSettings`, `describe_frame` and `inspect_graph` work with no Dask on the path. Import time drops from about 510 ms to about 2 ms. Touching `DaskSession` or a `safe_*` wrapper imports `distributed` as before.
- The standalone lockfile resolves against boti 1.2.0 (it pinned 1.0.5).

## [1.0.7] - 2026-08-02

Last release before this changelog; see the git history.

[Unreleased]: https://github.com/lvalverdeb/boti-dask/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/lvalverdeb/boti-dask/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/lvalverdeb/boti-dask/compare/v1.0.7...v1.1.0
[1.0.7]: https://github.com/lvalverdeb/boti-dask/releases/tag/v1.0.7
