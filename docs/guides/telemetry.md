# Telemetry API

`boti_dask.telemetry` reports what the resilient wrappers and sessions do, through a provider
of your choosing. The default provider records nothing and costs one attribute lookup per call.

| Provider                | What it does                                            | Needs            |
|-------------------------|---------------------------------------------------------|------------------|
| `NoopProvider`          | Nothing (default).                                      |                  |
| `InMemoryProvider`      | Keeps spans, counters and gauges in lists and dicts.    |                  |
| `OpenTelemetryProvider` | Spans via `opentelemetry.trace`, instruments via `opentelemetry.metrics`, using the tracer and meter providers the application configured. | `boti-dask[otel]` |

## What is recorded

- **Spans**: one per `safe_compute`, `safe_persist`, `safe_wait`, `safe_head` (through its inner
  `safe_compute`) and `safe_gather` call, named after the wrapper with a `remote` attribute. The
  `async_safe_*` wrappers open an outer span in the caller's context around the thread offload.
- **Counters**: `recoverable_errors` (a `RECOVERABLE_DASK_ERRORS` exception caught), `retries` (a
  retry against a live client attempted) and `rebinds` (`DaskSession.rebind()`, explicit or by the
  watchdog).
- **Gauges**: `workers`, `task_count` (tasks executing across workers) and `persisted_collections`
  (tracked by the session pool), published by `DaskSession.record_gauges()` on open and after a
  rebind, and callable at any time.

OpenTelemetry instrument names are prefixed `boti_dask.`.

## Selecting a provider

```python
from boti_dask import OpenTelemetryProvider, configure_telemetry, dask_session

configure_telemetry(OpenTelemetryProvider())        # process-wide
# or per session, which installs it on open:
with dask_session(telemetry=OpenTelemetryProvider()) as client:
    ...
```

From the environment: `{PREFIX}TELEMETRY=none|memory|otel`.

Any object with `span(name, **attributes)` returning a context manager, `add(counter, value=1,
**attributes)` and `gauge(name, value, **attributes)` satisfies `TelemetryProvider`.
