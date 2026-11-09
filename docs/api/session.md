# Session API

`boti_dask.session` provides explicit Dask client/session lifecycle helpers.

## Primary symbols

- `DaskSession`
- `DaskSessionSettings`
- `dask_session(...)`
- `dask_session_from_env_prefix(...)`
- `recommended_dask_config(...)`
- `apply_recommended_dask_config(...)`
- `describe_client(...)`
- `current_client_summary()`
- `ConnectivityCheck`
- `DaskSessionUnavailable`
- `WatchdogConfig` (see `watchdog.md`)

## Typical usage

```python
from boti_dask import dask_session, describe_client

with dask_session(
    verify_connectivity=True,
    cluster_kwargs={"n_workers": 1, "threads_per_worker": 1, "processes": False, "dashboard_address": ":0"},
) as client:
    print(describe_client(client))
```

## Connectivity verification

`verify_connectivity` is on by default. Before `DaskSession` hands out a client it pings the
scheduler, waits for `min_workers` (default 1) and runs one round-trip task, all within
`ConnectivityCheck.timeout` (default 10 s). A connect to a `scheduler_address` is given the
same deadline unless `client_kwargs` already sets `timeout`. Any failure raises
`DaskSessionUnavailable` naming the stage that failed, instead of hanging on distributed's
own timeouts.

```python
from boti_dask import ConnectivityCheck, DaskSessionUnavailable, dask_session

try:
    with dask_session(
        scheduler_address="tcp://scheduler:8786",
        verify_connectivity=ConnectivityCheck(timeout=5, min_workers=2),
    ) as client:
        ...
except DaskSessionUnavailable as exc:
    ...  # fail fast; the cause is chained
```

Pass `verify_connectivity=False` to skip the check (for example around a stub client in tests).
From the environment: `{PREFIX}VERIFY_CONNECTIVITY`, `{PREFIX}CONNECTIVITY_TIMEOUT` (seconds)
and `{PREFIX}MIN_WORKERS`.

## Notes

- `shared=True` + `shared_key=...` enables cross-context shared session reuse.
- Session close emits a runtime warning when live persisted collections are tracked.
- When `dask_session(...)` creates a local `LocalCluster` and `dashboard_address` is not set,
  it defaults to `":0"` to avoid port `8787` contention.
- `DaskSessionSettings.from_env_prefix(...)` supports typed env-backed loading for
  `scheduler_address`, `shared`, `shared_key`, `verify_connectivity`,
  `connectivity_timeout`, `min_workers`, `cluster_kwargs` (JSON), `client_kwargs` (JSON),
  `cluster_kind`, `watchdog_interval` and `watchdog_failures`.
- `DaskSession.rebind()` replaces a dead client with a fresh connection; the watchdog calls it.

