"""Telemetry hooks: spans around the ``safe_*`` wrappers and session counters/gauges.

A :class:`TelemetryProvider` is anything with ``span``, ``add`` and ``gauge``.
Three are provided: :class:`NoopProvider` (the default, costs one attribute
lookup), :class:`InMemoryProvider` (records everything, for tests and
diagnostics) and :class:`OpenTelemetryProvider` (behind the ``otel`` extra).
One provider is active per process (:func:`configure_telemetry`); a
:class:`~boti_dask.DaskSession` built with ``telemetry=`` installs it on open.

Counters: ``retries``, ``rebinds``, ``recoverable_errors``.
Gauges: ``workers``, ``persisted_collections``, ``task_count``.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "COUNTERS",
    "GAUGES",
    "InMemoryProvider",
    "NoopProvider",
    "OpenTelemetryProvider",
    "TelemetryProvider",
    "configure_telemetry",
    "get_telemetry",
    "telemetry_provider",
]

COUNTERS: tuple[str, ...] = ("retries", "rebinds", "recoverable_errors")
GAUGES: tuple[str, ...] = ("workers", "persisted_collections", "task_count")
_INSTRUMENTATION_NAME = "boti_dask"


@runtime_checkable
class TelemetryProvider(Protocol):
    def span(self, name: str, **attributes: Any) -> AbstractContextManager[Any]: ...

    def add(self, counter: str, value: int = 1, **attributes: Any) -> None: ...

    def gauge(self, name: str, value: float, **attributes: Any) -> None: ...


class NoopProvider:
    """Records nothing; the default."""

    # spaghetti-ignore[pass-through-method]: a no-op span is a null context by definition
    def span(self, name: str, **attributes: Any) -> AbstractContextManager[Any]:
        return contextlib.nullcontext()

    def add(self, counter: str, value: int = 1, **attributes: Any) -> None:
        return None

    def gauge(self, name: str, value: float, **attributes: Any) -> None:
        return None


@dataclass
class InMemoryProvider:
    """Keeps spans, counters and gauges in memory; thread-safe."""

    spans: list[dict[str, Any]] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @contextlib.contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[dict[str, Any]]:
        record = {"name": name, "attributes": attributes, "error": None}
        try:
            yield record
        except BaseException as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            with self._lock:
                self.spans.append(record)

    def add(self, counter: str, value: int = 1, **attributes: Any) -> None:
        with self._lock:
            self.counters[counter] = self.counters.get(counter, 0) + value

    def gauge(self, name: str, value: float, **attributes: Any) -> None:
        with self._lock:
            self.gauges[name] = value

    def reset(self) -> None:
        with self._lock:
            self.spans.clear()
            self.counters.clear()
            self.gauges.clear()


class OpenTelemetryProvider:
    """Spans through ``opentelemetry.trace`` and instruments through ``opentelemetry.metrics``.

    Uses whatever tracer and meter providers the application configured
    globally; boti-dask never installs exporters itself. Needs the ``otel``
    extra (``opentelemetry-api`` and ``opentelemetry-sdk``).
    """

    def __init__(self, *, tracer: Any | None = None, meter: Any | None = None) -> None:
        try:
            from opentelemetry import metrics, trace
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "OpenTelemetryProvider needs boti-dask's 'otel' extra: "
                "\"pip install 'boti-dask[otel]'\" (or \"uv add 'boti-dask[otel]'\").",
                name=exc.name,
                path=exc.path,
            ) from exc
        self._tracer = tracer or trace.get_tracer(_INSTRUMENTATION_NAME)
        self._meter = meter or metrics.get_meter(_INSTRUMENTATION_NAME)
        self._counters = {
            name: self._meter.create_counter(f"boti_dask.{name}") for name in COUNTERS
        }
        self._gauges = {name: self._meter.create_gauge(f"boti_dask.{name}") for name in GAUGES}

    def span(self, name: str, **attributes: Any) -> AbstractContextManager[Any]:
        return self._tracer.start_as_current_span(f"boti_dask.{name}", attributes=attributes)

    def add(self, counter: str, value: int = 1, **attributes: Any) -> None:
        instrument = self._counters.get(counter) or self._meter.create_counter(
            f"boti_dask.{counter}"
        )
        self._counters[counter] = instrument
        instrument.add(value, attributes=attributes)

    def gauge(self, name: str, value: float, **attributes: Any) -> None:
        instrument = self._gauges.get(name) or self._meter.create_gauge(f"boti_dask.{name}")
        self._gauges[name] = instrument
        instrument.set(value, attributes=attributes)


_PROVIDERS = {"none": NoopProvider, "memory": InMemoryProvider, "otel": OpenTelemetryProvider}


def telemetry_provider(name: str) -> Any:
    """Build a provider from its short name: ``none``, ``memory`` or ``otel``."""
    normalised = name.strip().lower()
    factory = _PROVIDERS.get(normalised)
    if factory is None:
        raise ValueError(
            f"Unknown telemetry provider {name!r}. Known providers: {', '.join(_PROVIDERS)}."
        )
    return factory()


@dataclass
class _Registry:
    """Holds the process-wide provider; swapped atomically by configure_telemetry()."""

    provider: Any = field(default_factory=NoopProvider)


_registry = _Registry()


def configure_telemetry(provider: Any | None) -> Any:
    """Install *provider* process-wide (``None`` restores the no-op) and return it."""
    _registry.provider = NoopProvider() if provider is None else provider
    return _registry.provider


def get_telemetry() -> Any:
    return _registry.provider
