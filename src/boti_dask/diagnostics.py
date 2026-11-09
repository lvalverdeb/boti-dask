from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ._internal import _loaded, _log

if TYPE_CHECKING:
    import dask.dataframe as dd


def _has_dask_graph(obj: Any) -> bool:
    return hasattr(obj, "__dask_graph__")


def inspect_graph(obj: Any, *, logger: Any | None = None, level: str = "info") -> dict[str, Any]:
    """Return compact Dask graph metrics for diagnostics and dry-run usage.

    Materialises the full graph, so the cost grows with the task count. The
    metrics line goes to *logger* at *level* (``"info"`` by default; the
    ``safe_*`` wrappers pass ``"debug"``).
    """
    if not _has_dask_graph(obj):
        metrics = {"type": type(obj).__name__, "is_dask": False}
        _log(logger, level, f"Dask graph inspection metrics={metrics}")
        return metrics

    graph = obj.__dask_graph__()
    graph_layers: int | None = None
    if hasattr(getattr(obj, "dask", None), "layers"):
        graph_layers = len(obj.dask.layers)
    elif hasattr(graph, "layers"):
        graph_layers = len(graph.layers)
    metrics = {
        "type": type(obj).__name__,
        "is_dask": True,
        "task_count": len(graph) if hasattr(graph, "__len__") else None,
        "npartitions": getattr(obj, "npartitions", None),
        "graph_layers": graph_layers,
    }
    _log(logger, level, f"Dask graph inspection metrics={metrics}")
    return metrics


def _describe_dask_frame(frame: Any) -> dict[str, Any]:
    graph = frame.__dask_graph__()
    return {
        "engine": "dask",
        "columns": len(frame.columns),
        "npartitions": frame.npartitions,
        "graph_tasks": len(graph) if hasattr(graph, "__len__") else None,
        "graph_layers": len(frame.dask.layers) if hasattr(frame.dask, "layers") else None,
        "known_divisions": frame.known_divisions,
    }


def _describe_pandas_frame(frame: Any) -> dict[str, Any]:
    return {
        "engine": "pandas",
        "rows": len(frame.index),
        "columns": len(frame.columns),
    }


def _describe_arrow_table(frame: Any) -> dict[str, Any]:
    return {
        "engine": "arrow",
        "rows": frame.num_rows,
        "columns": len(frame.column_names),
    }


def _describe_polars_frame(frame: Any) -> dict[str, Any]:
    return {
        "engine": "polars",
        "rows": frame.height,
        "columns": frame.width,
    }


def _is_instance_from(module_name: str, class_name: str) -> Callable[[Any], bool]:
    """Predicate that is true only if *module_name* is already imported and the
    object is an instance of its *class_name* -- so no engine is imported here."""

    def predicate(frame: Any) -> bool:
        module = _loaded(module_name)
        return module is not None and isinstance(frame, getattr(module, class_name))

    return predicate


# Ordered like the isinstance chain they replace: first matching predicate wins.
_FRAME_ENGINE_HANDLERS: list[tuple[Callable[[Any], bool], Callable[[Any], dict[str, Any]]]] = [
    (_is_instance_from("dask.dataframe", "DataFrame"), _describe_dask_frame),
    (_is_instance_from("pandas", "DataFrame"), _describe_pandas_frame),
    (_is_instance_from("pyarrow", "Table"), _describe_arrow_table),
    (_is_instance_from("polars", "DataFrame"), _describe_polars_frame),
]


def describe_frame(frame: Any) -> dict[str, Any]:
    """Return compact frame metrics suitable for runtime diagnostics logs."""
    for predicate, handler in _FRAME_ENGINE_HANDLERS:
        if predicate(frame):
            return handler(frame)
    return {"engine": type(frame).__name__}


def diagnostics_logger(logger: Any | None, *, name: str) -> Any:
    """Return provided logger or a :class:`boti.Logger` fallback scoped by *name*."""
    if logger is not None:
        return logger
    from boti.core.logger import Logger

    return Logger.default_logger(logger_name=name)


class UniqueValuesExtractor:
    """Best-effort unique value extraction for Dask-backed columns."""

    def __init__(self, *, dask_client: Any | None = None, logger: Any | None = None) -> None:
        self.dask_client = dask_client
        self.logger = logger

    def _extract_one(self, frame: dd.DataFrame, column: str, limit: int) -> tuple[str, list[Any]]:
        from .resilience_ops import safe_compute

        if column not in frame.columns:
            _log(self.logger, "warning", f"Column '{column}' not found for unique extraction.")
            return column, []

        expression = frame[column].dropna().drop_duplicates()
        if self.dask_client is not None:
            values = safe_compute(expression, dask_client=self.dask_client, logger=self.logger)
        else:
            values = expression.compute(scheduler="threads")
        import pandas as pd

        as_series = pd.Series(values)
        unique_values = as_series.dropna().unique().tolist()
        if len(unique_values) > limit:
            _log(
                self.logger,
                "warning",
                f"Unique value extraction for column '{column}' truncated at {limit} items.",
            )
            unique_values = unique_values[:limit]
        return column, unique_values

    async def extract_unique_values(
        self,
        frame: dd.DataFrame,
        *columns: str,
        limit: int = 100_000,
    ) -> dict[str, list[Any]]:
        from .resilience_ops import safe_persist

        if self.dask_client is not None:
            frame = safe_persist(frame, dask_client=self.dask_client, logger=self.logger)
        else:
            frame = frame.persist()
        _log(
            self.logger,
            "info",
            f"Persisted {frame.npartitions}-partition frame for unique extraction.",
        )
        pairs = await asyncio.gather(
            *(asyncio.to_thread(self._extract_one, frame, column, limit) for column in columns)
        )
        return dict(pairs)


__all__ = [
    "UniqueValuesExtractor",
    "describe_frame",
    "diagnostics_logger",
    "inspect_graph",
]
