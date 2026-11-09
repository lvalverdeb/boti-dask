from __future__ import annotations

import boti_dask


def test_boti_dask_exports_public_symbols():
    expected = {
        "DaskSession",
        "DaskSessionSettings",
        "CLUSTER_KINDS",
        "cluster_factory",
        "make_cluster",
        "Watchdog",
        "WatchdogConfig",
        "InMemoryProvider",
        "NoopProvider",
        "OpenTelemetryProvider",
        "TelemetryProvider",
        "configure_telemetry",
        "get_telemetry",
        "sync_uploads",
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


def test_every_export_has_exactly_one_stability_tier():
    tiers = set(boti_dask.STABLE) | set(boti_dask.EXPERIMENTAL)
    assert not set(boti_dask.STABLE) & set(boti_dask.EXPERIMENTAL)
    assert tiers == set(boti_dask.__all__) - {"STABLE", "EXPERIMENTAL", "deprecated"}
    assert {"DaskSession", "safe_compute", "cluster_semaphore"} <= set(boti_dask.STABLE)
    assert {"WatchdogConfig", "sync_uploads", "cluster_factory"} <= set(boti_dask.EXPERIMENTAL)


def test_deprecated_emits_a_deprecation_warning_naming_the_removal():
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        boti_dask.deprecated("old_name", "2.0", replacement="new_name")
    assert len(caught) == 1 and caught[0].category is DeprecationWarning
    assert "old_name" in str(caught[0].message) and "2.0" in str(caught[0].message)
    assert "new_name" in str(caught[0].message)


def test_extra_alias_resolves_to_the_diagnostics_extra():
    import tomllib
    from pathlib import Path

    extras = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())["project"][
        "optional-dependencies"
    ]
    assert extras["extra"] == ["boti-dask[diagnostics]"]
    assert {"numpy", "polars", "pyarrow"} == {spec.split(">=")[0] for spec in extras["diagnostics"]}
