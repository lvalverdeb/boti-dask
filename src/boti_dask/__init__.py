"""Dask runtime and resilience utilities for the Boti ecosystem."""

from __future__ import annotations

import importlib
import warnings
from typing import Any

# Every public name is loaded lazily (PEP 562), mirroring boti_data: importing
# boti_dask -- or reaching for a Dask-free piece such as DaskSessionSettings --
# never pulls in dask, distributed, pandas, polars or pyarrow. Only touching a
# name that genuinely needs Dask (DaskSession, safe_compute, ...) imports it.
_LAZY = {
    # Dask-free: settings and frame/graph descriptions that duck-type.
    "DaskSessionSettings": ("boti_dask.session_settings", "DaskSessionSettings"),
    "UniqueValuesExtractor": ("boti_dask.diagnostics", "UniqueValuesExtractor"),
    "describe_frame": ("boti_dask.diagnostics", "describe_frame"),
    "diagnostics_logger": ("boti_dask.diagnostics", "diagnostics_logger"),
    "inspect_graph": ("boti_dask.diagnostics", "inspect_graph"),
    # Named cluster backends (kubernetes/slurm/coiled), each imported on first use.
    "CLUSTER_KINDS": ("boti_dask.clusters", "CLUSTER_KINDS"),
    "cluster_factory": ("boti_dask.clusters", "cluster_factory"),
    "make_cluster": ("boti_dask.clusters", "make_cluster"),
    # Telemetry: spans around safe_* and session counters/gauges; no-op by default.
    "InMemoryProvider": ("boti_dask.telemetry", "InMemoryProvider"),
    "NoopProvider": ("boti_dask.telemetry", "NoopProvider"),
    "OpenTelemetryProvider": ("boti_dask.telemetry", "OpenTelemetryProvider"),
    "TelemetryProvider": ("boti_dask.telemetry", "TelemetryProvider"),
    "configure_telemetry": ("boti_dask.telemetry", "configure_telemetry"),
    "get_telemetry": ("boti_dask.telemetry", "get_telemetry"),
    # Code shipping: files, directories, wheels and requirements sent once per scheduler.
    "sync_uploads": ("boti_dask.upload", "sync_uploads"),
    # Cluster-wide primitives with process-local fallbacks (Dask only if a client exists).
    "cluster_lock": ("boti_dask.primitives", "cluster_lock"),
    "cluster_queue": ("boti_dask.primitives", "cluster_queue"),
    "cluster_semaphore": ("boti_dask.primitives", "cluster_semaphore"),
    # Dask-touching: the session and the resilient execution wrappers.
    "ConnectivityCheck": ("boti_dask.session_helpers", "ConnectivityCheck"),
    "DaskSession": ("boti_dask.session", "DaskSession"),
    "DaskSessionUnavailable": ("boti_dask.session_helpers", "DaskSessionUnavailable"),
    "Watchdog": ("boti_dask.watchdog", "Watchdog"),
    "WatchdogConfig": ("boti_dask.watchdog", "WatchdogConfig"),
    "apply_recommended_dask_config": ("boti_dask.session", "apply_recommended_dask_config"),
    "current_client_summary": ("boti_dask.session", "current_client_summary"),
    "dask_session": ("boti_dask.session", "dask_session"),
    "dask_session_from_env_prefix": ("boti_dask.session", "dask_session_from_env_prefix"),
    "describe_client": ("boti_dask.session", "describe_client"),
    "recommended_dask_config": ("boti_dask.session", "recommended_dask_config"),
    "RECOVERABLE_DASK_ERRORS": ("boti_dask.resilience", "RECOVERABLE_DASK_ERRORS"),
    "dask_is_probably_empty": ("boti_dask.resilience", "dask_is_probably_empty"),
    "async_safe_compute": ("boti_dask.resilience_async", "async_safe_compute"),
    "async_safe_gather": ("boti_dask.resilience_async", "async_safe_gather"),
    "async_safe_head": ("boti_dask.resilience_async", "async_safe_head"),
    "async_safe_persist": ("boti_dask.resilience_async", "async_safe_persist"),
    "async_safe_wait": ("boti_dask.resilience_async", "async_safe_wait"),
    "dask_is_empty": ("boti_dask.resilience_async", "dask_is_empty"),
    "safe_compute": ("boti_dask.resilience_ops", "safe_compute"),
    "safe_gather": ("boti_dask.resilience_ops", "safe_gather"),
    "safe_head": ("boti_dask.resilience_ops", "safe_head"),
    "safe_persist": ("boti_dask.resilience_ops", "safe_persist"),
    "safe_wait": ("boti_dask.resilience_ops", "safe_wait"),
}

# Stability tiers. Stable names change only with a major version, after a
# minor version in which the old form warns through deprecated(). Experimental
# names (new in 1.2) may change in a minor version; the changelog records it.
EXPERIMENTAL: tuple[str, ...] = (
    "CLUSTER_KINDS",
    "cluster_factory",
    "make_cluster",
    "Watchdog",
    "WatchdogConfig",
    "InMemoryProvider",
    "NoopProvider",
    "OpenTelemetryProvider",
    "TelemetryProvider",
    "configure_telemetry",
    "get_telemetry",
    "sync_uploads",
)
STABLE: tuple[str, ...] = tuple(sorted(set(_LAZY) - set(EXPERIMENTAL)))

__all__ = sorted(_LAZY) + ["EXPERIMENTAL", "STABLE", "deprecated"]


def deprecated(name: str, removal: str, *, replacement: str | None = None) -> None:
    """Warn that *name* goes away in boti-dask *removal*; point at *replacement* if any."""
    message = f"{name} is deprecated and will be removed in boti-dask {removal}."
    if replacement:
        message += f" Use {replacement} instead."
    warnings.warn(message, DeprecationWarning, stacklevel=3)


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        module_name, attr = _LAZY[name]
        value = getattr(importlib.import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY))
