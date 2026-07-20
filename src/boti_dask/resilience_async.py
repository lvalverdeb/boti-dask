"""dask_is_empty() and the async_safe_* wrappers.

Split out of resilience.py to keep files under the length threshold. Depends
on resilience.py (dask_is_probably_empty, _to_int_safe) and resilience_ops.py
(safe_compute/persist/wait/head/gather); neither of those depends on this
module, so the import direction is one-way.
"""

from __future__ import annotations

import asyncio
from typing import Any

from ._internal import _is_dask_dataframe_like, _log
from .resilience import _to_int_safe, dask_is_probably_empty
from .resilience_ops import safe_compute, safe_gather, safe_head, safe_persist, safe_wait


def _non_dask_is_empty(obj: Any, *, logger: Any | None) -> bool:
    try:
        return len(obj) == 0
    except Exception:
        _log(logger, "debug", "len() call failed for non-DataFrame object, returning False")
        return False


def _compute_dask_expr(expr: Any, *, dask_client: Any | None, logger: Any | None) -> Any:
    if dask_client is not None:
        return safe_compute(expr, dask_client=dask_client, logger=logger)
    return expr.compute(scheduler="threads")


def _any_partition_nonempty(
    obj: Any, probes: int, *, dask_client: Any | None, logger: Any | None
) -> bool:
    for index in range(probes):
        count_expr = obj.get_partition(index).map_partitions(len, meta=("n", "int64")).sum()
        count = _compute_dask_expr(count_expr, dask_client=dask_client, logger=logger)
        if _to_int_safe(count) > 0:
            return True
    return False


def _remaining_rows_are_empty(obj: Any, *, dask_client: Any | None, logger: Any | None) -> bool:
    total_expr = obj.map_partitions(len, meta=("n", "int64")).sum()
    total = _compute_dask_expr(total_expr, dask_client=dask_client, logger=logger)
    return _to_int_safe(total) == 0


def _dask_dataframe_is_empty(
    obj: Any, *, sample: int, dask_client: Any | None, logger: Any | None
) -> bool:
    partitions = int(getattr(obj, "npartitions", 0) or 0)
    probes = min(max(sample, 1), partitions)
    try:
        if _any_partition_nonempty(obj, probes, dask_client=dask_client, logger=logger):
            return False
        return probes == partitions or _remaining_rows_are_empty(
            obj, dask_client=dask_client, logger=logger
        )
    except Exception as exc:
        _log(logger, "warning", f"dask_is_empty probe failed: {exc}")
        return False


def dask_is_empty(
    obj: Any,
    *,
    sample: int = 4,
    dask_client: Any | None = None,
    logger: Any | None = None,
) -> bool:
    if dask_is_probably_empty(obj):
        return True
    if not _is_dask_dataframe_like(obj):
        return _non_dask_is_empty(obj, logger=logger)
    return _dask_dataframe_is_empty(obj, sample=sample, dask_client=dask_client, logger=logger)


# The "underlying object" is asyncio.to_thread itself, a stdlib function
# with no further surface to expose — this is the standard sync-to-async
# offload idiom, not an accidental thin wrapper.
# spaghetti-ignore[pass-through-method]
async def async_safe_compute(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return await asyncio.to_thread(
        safe_compute,
        obj,
        dask_client=dask_client,
        logger=logger,
        dry_run=dry_run,
    )


# See async_safe_compute's comment above.
# spaghetti-ignore[pass-through-method]
async def async_safe_persist(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
) -> Any:
    return await asyncio.to_thread(
        safe_persist,
        obj,
        dask_client=dask_client,
        logger=logger,
    )


# See async_safe_compute's comment above.
# spaghetti-ignore[pass-through-method]
async def async_safe_wait(
    obj: Any,
    *,
    dask_client: Any | None = None,
    timeout: float | None = None,
    logger: Any | None = None,
) -> Any:
    return await asyncio.to_thread(
        safe_wait,
        obj,
        dask_client=dask_client,
        timeout=timeout,
        logger=logger,
    )


# See async_safe_compute's comment above.
# spaghetti-ignore[pass-through-method]
async def async_safe_head(
    obj: Any,
    *,
    n: int = 5,
    npartitions: int = 1,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return await asyncio.to_thread(
        safe_head,
        obj,
        n=n,
        npartitions=npartitions,
        dask_client=dask_client,
        logger=logger,
        dry_run=dry_run,
    )


# See async_safe_compute's comment above.
# spaghetti-ignore[pass-through-method]
async def async_safe_gather(
    obj: Any,
    *,
    dask_client: Any | None = None,
    logger: Any | None = None,
    dry_run: bool = False,
) -> Any:
    return await asyncio.to_thread(
        safe_gather,
        obj,
        dask_client=dask_client,
        logger=logger,
        dry_run=dry_run,
    )


__all__ = [
    "async_safe_compute",
    "async_safe_gather",
    "async_safe_head",
    "async_safe_persist",
    "async_safe_wait",
    "dask_is_empty",
]
