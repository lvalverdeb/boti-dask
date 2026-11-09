# Changelog

All notable changes to boti-dask are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Behaviour changes are in **bold**
under *Changed*.

## [Unreleased]

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

[Unreleased]: https://github.com/lvalverdeb/boti-dask/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/lvalverdeb/boti-dask/compare/v1.0.7...v1.1.0
[1.0.7]: https://github.com/lvalverdeb/boti-dask/releases/tag/v1.0.7
