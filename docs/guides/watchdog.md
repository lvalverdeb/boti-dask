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

- The replacement is opened (and verified, when verification is on) before the old client is
  closed. A failed reconnect raises and leaves the session on its previous client; the watchdog
  keeps probing, doubling its interval after each failed rebind up to `max_backoff`.
- **Managed session**: opens a new client (and cluster), then closes the old ones. Live persisted
  collections on the old client are lost, exactly as on close, and the same warning is raised.
- **Shared session**: swaps the pool entry under the per-key creation lock, keeps the holder
  count and points every holder of the key at the new client, so one rebind serves them all.
  `rebind(stale=client)` does nothing when `client` has already been replaced; the watchdog uses
  it so two holders never rebind the same outage twice.
- **External client**: cannot be rebound; `rebind()` raises `RuntimeError`.

`SessionPool.rebind_count()` counts rebinds across the process; the telemetry provider exports
it as the `rebinds` counter.

## Events

The `Rebound Dask session key=...` line names a shared session by its `namespace`: the
explicit `shared_key`, or a digest of the derived pool key (which embeds the cluster and client
keyword arguments and is never logged in full); `None` for a managed session.

Every transition goes to the session logger and to `on_event` as a dict with `event` and
`shared_key` plus event-specific fields (`failures`, `error`, `rebinds`). Exceptions raised by
`on_event` are swallowed and logged at debug level; they never stop the watchdog.
