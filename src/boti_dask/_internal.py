from __future__ import annotations

from typing import Any

import dask.dataframe as dd


def _log(logger: Any | None, level: str, message: str) -> None:
    if logger is None:
        return
    log_fn = getattr(logger, level, None)
    if callable(log_fn):
        log_fn(message)


def _is_dask_dataframe_like(obj: Any) -> bool:
    # `_meta` is dask's own duck-typing convention for "quacks like a Dask
    # collection" (e.g. dask-cudf), not a boti class's private attribute.
    # spaghetti-ignore[encapsulation-violation]
    return isinstance(obj, (dd.DataFrame, dd.Series)) or hasattr(obj, "_meta")


def _is_running_client(client: Any | None) -> bool:
    if client is None:
        return False
    return getattr(client, "status", "running") in {"running", "started"}
