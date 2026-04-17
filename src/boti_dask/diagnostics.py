from __future__ import annotations

import asyncio
from typing import Any

import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow as pa


def _log(logger: Any | None, level: str, message: str) -> None:
    if logger is None:
        return
    log_fn = getattr(logger, level, None)
    if callable(log_fn):
        log_fn(message)


def _has_dask_graph(obj: Any) -> bool:
    return hasattr(obj, "__dask_graph__")


def inspect_graph(obj: Any, *, logger: Any | None = None) -> dict[str, Any]:
    """Return compact Dask graph metrics for diagnostics and dry-run usage."""
    if not _has_dask_graph(obj):
        metrics = {"type": type(obj).__name__, "is_dask": False}
        _log(logger, "info", f"Dask graph inspection metrics={metrics}")
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
    _log(logger, "info", f"Dask graph inspection metrics={metrics}")
    return metrics


def describe_frame(frame: Any) -> dict[str, Any]:
    """Return compact frame metrics suitable for runtime diagnostics logs."""
    if isinstance(frame, dd.DataFrame):
        graph = frame.__dask_graph__()
        return {
            "engine": "dask",
            "columns": len(frame.columns),
            "npartitions": frame.npartitions,
            "graph_tasks": len(graph) if hasattr(graph, "__len__") else None,
            "graph_layers": len(frame.dask.layers) if hasattr(frame.dask, "layers") else None,
            "known_divisions": frame.known_divisions,
        }
    if isinstance(frame, pd.DataFrame):
        return {
            "engine": "pandas",
            "rows": len(frame.index),
            "columns": len(frame.columns),
        }
    if isinstance(frame, pa.Table):
        return {
            "engine": "arrow",
            "rows": frame.num_rows,
            "columns": len(frame.column_names),
        }
    if isinstance(frame, pl.DataFrame):
        return {
            "engine": "polars",
            "rows": frame.height,
            "columns": frame.width,
        }
    return {"engine": type(frame).__name__}


def diagnostics_logger(logger: Any | None, *, name: str) -> Any:
    """Return provided logger or a stdlib fallback logger scoped by name."""
    if logger is not None:
        return logger
    import logging

    resolved = logging.getLogger(name)
    if not resolved.handlers:
        resolved.addHandler(logging.StreamHandler())
    return resolved


class UniqueValuesExtractor:
    """Best-effort unique value extraction for Dask-backed columns."""

    def __init__(self, *, dask_client: Any | None = None, logger: Any | None = None) -> None:
        self.dask_client = dask_client
        self.logger = logger

    def _extract_one(self, frame: dd.DataFrame, column: str, limit: int) -> tuple[str, list[Any]]:
        from .resilience import safe_compute

        if column not in frame.columns:
            _log(self.logger, "warning", f"Column '{column}' not found for unique extraction.")
            return column, []

        expression = frame[column].dropna().drop_duplicates()
        if self.dask_client is not None:
            values = safe_compute(expression, dask_client=self.dask_client, logger=self.logger)
        else:
            values = expression.compute(scheduler="threads")
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

