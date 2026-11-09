# Stability

`boti_dask.STABLE` and `boti_dask.EXPERIMENTAL` name every exported symbol's tier.

- **Stable**: the session (`DaskSession`, `dask_session`, `DaskSessionSettings`, connectivity),
  the `safe_*` and `async_safe_*` wrappers, diagnostics and the cluster primitives. Breaking
  changes to these arrive only with a major version, after at least one minor version in which
  the old form warns.
- **Experimental**: `clusters`, `watchdog`, `telemetry` and `upload`, new in 1.2. Their
  signatures may change in a minor version; the changelog records every change in **bold**.

`boti_dask.deprecated(name, removal)` emits the `DeprecationWarning` boti-dask uses for the
transition, naming the version in which the symbol goes away.
