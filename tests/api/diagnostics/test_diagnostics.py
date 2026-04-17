from __future__ import annotations

import dask.dataframe as dd
import pandas as pd
import pytest

from boti_dask.diagnostics import UniqueValuesExtractor, describe_frame, diagnostics_logger


def test_describe_frame_reports_dask_metrics():
    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)
    metrics = describe_frame(frame)
    assert metrics["engine"] == "dask"
    assert metrics["npartitions"] == 2


def test_describe_frame_reports_pandas_metrics():
    frame = pd.DataFrame({"id": [1, 2]})
    metrics = describe_frame(frame)
    assert metrics["engine"] == "pandas"
    assert metrics["rows"] == 2


@pytest.mark.asyncio
async def test_unique_values_extractor_reads_unique_values():
    frame = dd.from_pandas(
        pd.DataFrame({"id": [1, 1, 2, 2], "status": ["a", "a", "b", "b"]}),
        npartitions=2,
    )
    result = await UniqueValuesExtractor().extract_unique_values(frame, "id", "status", limit=10)
    assert set(result["id"]) == {1, 2}
    assert set(result["status"]) == {"a", "b"}


def test_diagnostics_logger_falls_back_to_stdlib_logger():
    logger = diagnostics_logger(None, name="boti_dask.tests")
    assert hasattr(logger, "info")

