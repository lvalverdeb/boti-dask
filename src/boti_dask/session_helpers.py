from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import dask

try:
    from dask.distributed import LocalCluster
except ImportError:
    LocalCluster = None

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


def _verify_client_connection(client: Any) -> None:
    try:
        client.scheduler_info()
    except Exception as exc:
        raise RuntimeError(
            "Failed to verify Dask client connectivity. "
            "Check the scheduler address and cluster health before retrying."
        ) from exc


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
