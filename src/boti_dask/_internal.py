from __future__ import annotations

import sys
from typing import Any


def _log(logger: Any | None, level: str, message: str) -> None:
    if logger is None:
        return
    log_fn = getattr(logger, level, None)
    if callable(log_fn):
        log_fn(message)


def _loaded(module_name: str) -> Any | None:
    """Return an already-imported module, or None without importing it.

    An object can only be an instance of a class from a module that has been
    imported, so checking ``sys.modules`` is both exact and free -- it is how
    the package stays Dask-free until a caller brings Dask in themselves.
    """
    return sys.modules.get(module_name)


def _is_dask_dataframe_like(obj: Any) -> bool:
    # `_meta` is dask's own duck-typing convention for "quacks like a Dask
    # collection" (e.g. dask-cudf), not a boti class's private attribute.
    # spaghetti-ignore[encapsulation-violation]
    dd = _loaded("dask.dataframe")
    if dd is not None and isinstance(obj, (dd.DataFrame, dd.Series)):
        return True
    return hasattr(obj, "_meta")


def _is_running_client(client: Any | None) -> bool:
    if client is None:
        return False
    return getattr(client, "status", "running") in {"running", "started"}
