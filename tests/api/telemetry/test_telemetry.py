"""Spans around safe_*, counters for retries/rebinds, gauges from the session."""

from __future__ import annotations

import asyncio
import importlib.util

import pytest

from boti_dask import (
    DaskSession,
    DaskSessionSettings,
    InMemoryProvider,
    NoopProvider,
    TelemetryProvider,
    async_safe_compute,
    configure_telemetry,
    get_telemetry,
    safe_compute,
)
from boti_dask import session as session_module
from boti_dask.session_pool import pool
from boti_dask.telemetry import telemetry_provider


@pytest.fixture
def memory():
    provider = configure_telemetry(InMemoryProvider())
    try:
        yield provider
    finally:
        configure_telemetry(None)


def test_default_provider_is_noop_and_satisfies_the_protocol():
    assert isinstance(get_telemetry(), NoopProvider)
    assert isinstance(get_telemetry(), TelemetryProvider)
    assert isinstance(InMemoryProvider(), TelemetryProvider)


def test_provider_names():
    assert isinstance(telemetry_provider("NONE"), NoopProvider)
    assert isinstance(telemetry_provider("memory"), InMemoryProvider)
    with pytest.raises(ValueError, match="none, memory, otel"):
        telemetry_provider("datadog")


class Delayed:
    """A compute-able object with no distributed client involved."""

    def compute(self, scheduler=None):
        return 42


def test_safe_compute_opens_a_span(memory):
    assert safe_compute(Delayed()) == 42
    assert [s["name"] for s in memory.spans] == ["safe_compute"]
    assert memory.spans[0]["attributes"] == {"remote": False}
    assert memory.spans[0]["error"] is None


def test_async_wrapper_nests_an_outer_span(memory):
    assert asyncio.run(async_safe_compute(Delayed())) == 42
    assert [s["name"] for s in memory.spans] == ["safe_compute", "async_safe_compute"]


def test_span_records_errors(memory):
    class Broken:
        def compute(self, scheduler=None):
            raise KeyError("boom")

    with pytest.raises(KeyError):
        safe_compute(Broken())
    assert memory.spans[0]["error"].startswith("KeyError")


def test_recoverable_error_counts_and_retry_counts(memory, monkeypatch):
    from boti_dask import resilience_ops

    class FlakyClient:
        status = "running"
        calls = 0

        def compute(self, obj):
            FlakyClient.calls += 1
            if FlakyClient.calls == 1:
                raise OSError("connection dropped")
            return 7

    client = FlakyClient()
    monkeypatch.setattr(resilience_ops, "_resolve_active_client", lambda provided=None: client)
    assert safe_compute(Delayed(), dask_client=client) == 7
    assert memory.counters == {"recoverable_errors": 1, "retries": 1}


class StubClient:
    def __init__(self, address, **_kwargs) -> None:
        self.status = "running"

    def scheduler_info(self):
        return {
            "workers": {
                "a": {"nthreads": 2, "metrics": {"executing": 3}},
                "b": {"nthreads": 2, "metrics": {"executing": 1}},
            }
        }

    def close(self) -> None:
        self.status = "closed"


def test_session_installs_provider_and_publishes_gauges(monkeypatch):
    monkeypatch.setattr(session_module, "Client", StubClient)
    provider = InMemoryProvider()
    session = DaskSession(
        scheduler_address="tcp://stub:1", verify_connectivity=False, telemetry=provider
    )
    try:
        session.open()
        assert get_telemetry() is provider
        assert provider.gauges == {"workers": 2.0, "task_count": 4.0, "persisted_collections": 0.0}
        rebinds = pool.rebind_count()
        session.rebind()
        assert provider.counters == {"rebinds": 1}
        assert pool.rebind_count() == rebinds + 1
    finally:
        session.close()
        configure_telemetry(None)


def test_settings_build_provider(monkeypatch):
    monkeypatch.setenv("DASK_SESSION_TELEMETRY", "memory")
    settings = DaskSessionSettings.from_env_prefix("DASK_SESSION_")
    assert isinstance(settings.to_session_kwargs()["telemetry"], InMemoryProvider)
    assert "telemetry" not in DaskSessionSettings().to_session_kwargs()


_HAS_OTEL = importlib.util.find_spec("opentelemetry") is not None


@pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry-sdk not installed (otel extra)")
def test_opentelemetry_provider_exports_spans_and_metrics():
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from boti_dask import OpenTelemetryProvider

    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    provider = OpenTelemetryProvider(
        tracer=tracer_provider.get_tracer("t"), meter=meter_provider.get_meter("m")
    )
    configure_telemetry(provider)
    try:
        assert safe_compute(Delayed()) == 42
        provider.add("rebinds")
        provider.gauge("workers", 3)
    finally:
        configure_telemetry(None)

    assert [s.name for s in exporter.get_finished_spans()] == ["boti_dask.safe_compute"]
    metrics = {
        m.name: m
        for rm in reader.get_metrics_data().resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
    }
    assert metrics["boti_dask.rebinds"].data.data_points[0].value == 1
    assert metrics["boti_dask.workers"].data.data_points[0].value == 3


def test_opentelemetry_provider_names_the_extra_when_missing(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    from boti_dask import OpenTelemetryProvider

    with pytest.raises(ModuleNotFoundError, match=r"boti-dask\[otel\]"):
        OpenTelemetryProvider()
