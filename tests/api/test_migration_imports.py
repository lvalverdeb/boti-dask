from __future__ import annotations

import boti_dask


def test_boti_dask_exports_migration_symbols():
    expected = {
        "DaskSession",
        "DaskSessionSettings",
        "UniqueValuesExtractor",
        "apply_recommended_dask_config",
        "async_safe_compute",
        "async_safe_gather",
        "async_safe_head",
        "async_safe_persist",
        "async_safe_wait",
        "dask_is_empty",
        "dask_is_probably_empty",
        "dask_session",
        "dask_session_from_env_prefix",
        "describe_client",
        "describe_frame",
        "diagnostics_logger",
        "inspect_graph",
        "safe_compute",
        "safe_gather",
        "safe_head",
        "safe_persist",
        "safe_wait",
    }

    for symbol in expected:
        assert hasattr(boti_dask, symbol), f"Missing boti_dask export: {symbol}"

