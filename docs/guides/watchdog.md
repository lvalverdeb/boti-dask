# Watchdog API

`boti_dask.watchdog` keeps a `DaskSession` honest after it has opened. A `Watchdog` runs the
same probe the session runs on open (scheduler ping, `min_workers` wait, one round-trip task)
every `interval` seconds on a daemon thread. After `failures_before_rebind` consecutive failures
it calls `DaskSession.rebind()`, which replaces the dead client with a fresh connection to the
same scheduler address or cluster factory.

```python
from boti_dask import ConnectivityCheck, WatchdogConfig, dask_session

def on_event(event: dict) -> None:
    print(event["event"], event)   # probe_failed / rebinding / rebound / rebind_failed / recovered

with dask_session(
    scheduler_address="tcp://scheduler:8786",
    watchdog=WatchdogConfig(interval=30, failures_before_rebind=3,
                            check=ConnectivityCheck(timeout=5), on_event=on_event),
) as client:
    ...
```

From the environment: `{PREFIX}WATCHDOG_INTERVAL` (seconds, enables the watchdog) and
`{PREFIX}WATCHDOG_FAILURES` (default 3).

## What rebind does

- **Managed session**: closes the client and any owned cluster, then opens again. Live persisted
  collections on the old client are lost, exactly as on close, and the same warning is raised.
- **Shared session**: swaps the pool entry under the per-key creation lock and keeps the holder
  count, so every holder's `safe_*` retry and every later `dask_session(shared=True)` with the same
  key sees the new client. Holders that cached the old client object must read `session.client`
  again.
- **External client**: cannot be rebound; `rebind()` raises `RuntimeError`.

`SessionPool.rebind_count()` counts rebinds across the process; the telemetry provider exports
it as the `rebinds` counter.

## Events

Every transition goes to the session logger and to `on_event` as a dict with `event` and
`shared_key` plus event-specific fields (`failures`, `error`, `rebinds`). Exceptions raised by
`on_event` are swallowed and logged at debug level; they never stop the watchdog.
