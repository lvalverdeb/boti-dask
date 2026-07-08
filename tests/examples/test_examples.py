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
    assert result["probably_empty"] is False
    assert result["is_empty"] is False
    assert set(result["unique_values"]["status"]) == {"active", "inactive"}
    assert set(result["unique_values"]["id"]) == {1, 2, 3, 4}
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
    assert "Probably empty: False" in output
    assert "Is empty: False" in output


def test_shared_session_lifecycle_example_runs(capsys):
    module = _load_example_module("shared_session_lifecycle.py")
    result = module.main()
    output = capsys.readouterr().out

    assert result["outer_before_workers"] == 2
    assert result["same_client"] is True
    assert result["outer_after_workers"] == 2
    assert result["session_status"] == "closed"
    assert "[outer] workers:" in output
    assert "[inner] same client?" in output
    assert "[outer] still connected:" in output
    assert "[done] client status:" in output


def test_async_distributed_pipeline_example_runs(capsys):
    module = _load_example_module("async_distributed_pipeline.py")
    result = module.main()
    output = capsys.readouterr().out

    assert result["preview_ids"] == [0, 1, 2, 3, 4]
    assert result["gathered"] == [10, 20, 30]
    assert result["total"] == 4950
    assert set(result["unique_groups"]) == {"A", "B"}
    assert "preview_ids:" in output
    assert "total:" in output


def test_multi_worker_cluster_example_runs(capsys):
    module = _load_example_module("multi_worker_cluster.py")
    result = module.main()
    output = capsys.readouterr().out

    assert result["workers"] == 4
    assert result["threads"] == 8
    assert result["total"] == 49_985_001
    assert result["grouped"] == {"X": 16_658_334, "Y": 16_661_667, "Z": 16_665_000}
    assert result["empty"] is False
    assert "Scheduler:" in output
    assert "Sum of 'value':" in output


def test_orphaned_graph_scenarios_example_runs(capsys):
    module = _load_example_module("orphaned_graph_scenarios.py")
    result = module.main()
    output = capsys.readouterr().out

    assert result["scenario1_warning_count"] >= 1
    assert result["scenario1_raised"] is True
    assert "orphaned" in result.get("scenario1_error", "").lower()
    assert result["scenario2_client_alive"] is True
    assert result["scenario2_result"] == 42
    assert result["scenario3_outer"] == "outer"
    assert result["scenario3_inner"] == "inner"
    assert result["scenario3_client_alive_at_inner"] is True
    assert result["scenario4_local_result"] == 99
    assert "orphaned graph from managed session" in output.lower()
    assert "Local fallback result:" in output
