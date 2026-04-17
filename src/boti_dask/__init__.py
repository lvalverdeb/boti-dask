"""Dask runtime and resilience utilities for the Boti ecosystem."""

from boti_dask.diagnostics import (
    UniqueValuesExtractor,
    describe_frame,
    diagnostics_logger,
    inspect_graph,
)
from boti_dask.resilience import (
    RECOVERABLE_DASK_ERRORS,
    async_safe_compute,
    async_safe_gather,
    async_safe_head,
    async_safe_persist,
    async_safe_wait,
    dask_is_empty,
    dask_is_probably_empty,
    safe_compute,
    safe_gather,
    safe_head,
    safe_persist,
    safe_wait,
)
from boti_dask.session import (
    DaskSession,
    apply_recommended_dask_config,
    current_client_summary,
    dask_session,
    describe_client,
    recommended_dask_config,
)

__all__ = [
    "DaskSession",
    "RECOVERABLE_DASK_ERRORS",
    "UniqueValuesExtractor",
    "apply_recommended_dask_config",
    "async_safe_compute",
    "async_safe_gather",
    "async_safe_head",
    "async_safe_persist",
    "async_safe_wait",
    "current_client_summary",
    "dask_is_empty",
    "dask_is_probably_empty",
    "dask_session",
    "describe_frame",
    "describe_client",
    "diagnostics_logger",
    "inspect_graph",
    "recommended_dask_config",
    "safe_compute",
    "safe_gather",
    "safe_head",
    "safe_persist",
    "safe_wait",
]

