from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


def _load_example_module(filename: str):
    path = EXAMPLES_DIR / filename
    spec = importlib.util.spec_from_file_location(filename.replace(".py", ""), path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load example module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dask_resilience_example_runs_and_returns_summary(capsys):
    module = _load_example_module("data_facade_dask_resilience.py")

    result = module.main()
    output = capsys.readouterr().out

    assert result["sync_total"] == 10
    assert result["async_total"] == 42
    assert result["sync_preview_ids"] == [1, 2]
    assert result["sync_gather"] == [10, 20]
    assert result["async_preview_ids"] == [1, 2]
    assert result["async_gather"] == [2, 3]
    assert result["partitions"] == 2
    assert result["shared_client_reused"] is True
    assert result["graph_metrics"]["is_dask"] is True
    assert "Resilience graph metrics:" in output
    assert "Distributed session client:" in output
    assert "Shared session reused: True" in output
    assert "Sync resilient total: 10" in output
    assert "Sync preview ids: [1, 2]" in output
    assert "Sync gathered values: [10, 20]" in output
    assert "Async resilient total: 42" in output
    assert "Async preview ids: [1, 2]" in output
    assert "Async gathered values: [2, 3]" in output

