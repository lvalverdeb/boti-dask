from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import dask

from .session_settings import DEFAULT_CONNECTIVITY_TIMEOUT, DEFAULT_MIN_WORKERS

try:
    from dask.distributed import LocalCluster
except ImportError:
    LocalCluster = None

# Floor for the per-stage remaining time once the deadline has passed, so the
# last stage still gets a real (if tiny) timeout instead of zero or negative.
_MIN_REMAINING_SECONDS = 0.01

_RECOMMENDED_DASK_CONFIG = {
    "distributed.comm.timeouts.connect": "20s",
    "distributed.comm.timeouts.tcp": "120s",
    "distributed.worker.memory.target": 0.6,
    "distributed.worker.memory.spill": 0.7,
    "distributed.worker.memory.pause": 0.8,
    "distributed.scheduler.allowed-failures": 3,
    "distributed.deploy.lost-worker-timeout": "60s",
    "distributed.admin.large-graph-warning-threshold": "50MB",
    "dataframe.shuffle.method": "tasks",
}


def _stable_mapping_repr(value: Mapping[str, Any]) -> str:
    return repr(sorted((str(key), repr(item)) for key, item in value.items()))


class DaskSessionUnavailable(RuntimeError):
    """The scheduler could not be reached, had too few workers, or did not run a task
    within the connectivity deadline."""


@dataclass(frozen=True, slots=True)
class ConnectivityCheck:
    """What :class:`DaskSession` verifies before handing out a client.

    ``timeout`` bounds the whole check in seconds (scheduler ping, waiting for
    ``min_workers`` and one round-trip task); ``min_workers`` is the number of
    workers that must be present, at least one so the round trip can run.
    """

    timeout: float = DEFAULT_CONNECTIVITY_TIMEOUT
    min_workers: int = DEFAULT_MIN_WORKERS

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("ConnectivityCheck.timeout must be positive.")
        if self.min_workers < 1:
            raise ValueError("ConnectivityCheck.min_workers must be at least 1.")


def _probe_task() -> int:
    return 1


def _unavailable(
    stage: str, check: ConnectivityCheck, exc: BaseException
) -> DaskSessionUnavailable:
    return DaskSessionUnavailable(
        f"Dask cluster unavailable: {stage} failed within {check.timeout:g}s "
        f"(min_workers={check.min_workers}). Check the scheduler address and cluster "
        f"health before retrying. Cause: {type(exc).__name__}: {exc}"
    )


def _open_client(
    client_cls: Any, address: str, client_kwargs: Mapping[str, Any], check: ConnectivityCheck | None
) -> Any:
    """Connect to *address*; with a check, the connect itself honours the deadline."""
    kwargs = dict(client_kwargs)
    if check is not None:
        # Otherwise a dead address hangs for distributed's own default before
        # the verification even starts.
        kwargs.setdefault("timeout", check.timeout)
    try:
        return client_cls(address, **kwargs)
    except (OSError, TimeoutError) as exc:
        if check is None:
            raise
        raise DaskSessionUnavailable(
            f"Dask cluster unavailable: could not connect to {address} within "
            f"{check.timeout:g}s. Cause: {type(exc).__name__}: {exc}"
        ) from exc


def _verify_client_connection(client: Any, check: ConnectivityCheck) -> None:
    """Scheduler ping, worker-count wait and a round-trip task, all within ``check.timeout``."""
    deadline = time.perf_counter() + check.timeout
    remaining = lambda: max(deadline - time.perf_counter(), _MIN_REMAINING_SECONDS)  # noqa: E731
    try:
        client.scheduler_info()
    except Exception as exc:
        raise _unavailable("scheduler ping", check, exc) from exc
    try:
        client.wait_for_workers(n_workers=check.min_workers, timeout=remaining())
    except Exception as exc:
        raise _unavailable(f"waiting for {check.min_workers} worker(s)", check, exc) from exc
    try:
        client.submit(_probe_task, pure=False).result(timeout=remaining())
    except Exception as exc:
        raise _unavailable("round-trip task", check, exc) from exc


def _prepare_cluster_kwargs(
    cluster_factory: Any, cluster_kwargs: Mapping[str, Any]
) -> dict[str, Any]:
    resolved = dict(cluster_kwargs)
    if cluster_factory is LocalCluster and "dashboard_address" not in resolved:
        resolved["dashboard_address"] = ":0"
    return resolved


def recommended_dask_config(*, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    config = dict(_RECOMMENDED_DASK_CONFIG)
    if overrides:
        config.update(dict(overrides))
    return config


def apply_recommended_dask_config(**overrides: Any) -> Any:
    return dask.config.set(recommended_dask_config(overrides=overrides))
