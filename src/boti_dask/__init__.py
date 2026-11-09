"""Dask runtime and resilience utilities for the Boti ecosystem."""

from __future__ import annotations

import importlib
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
    # Dask-touching: the session and the resilient execution wrappers.
    "DaskSession": ("boti_dask.session", "DaskSession"),
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

__all__ = sorted(_LAZY)


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        module_name, attr = _LAZY[name]
        value = getattr(importlib.import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY))
