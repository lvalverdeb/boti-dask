from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from boti.core.settings import load_dotenv_values

_ENV_PREFIX_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*_?$")

# Connectivity-check defaults live here, the Dask-free module, so both the
# settings and ConnectivityCheck (in session_helpers) read the same numbers.
DEFAULT_CONNECTIVITY_TIMEOUT = 10.0
DEFAULT_MIN_WORKERS = 1
DEFAULT_WATCHDOG_FAILURES = 3


def _validate_env_prefix(prefix: str) -> str:
    normalized = prefix.strip()
    if not normalized or not _ENV_PREFIX_PATTERN.fullmatch(normalized):
        raise ValueError(
            "Environment prefixes must match [A-Za-z_][A-Za-z0-9_]* and may end with a single underscore."
        )
    return normalized


def _parse_env_bool(raw: str, *, field_name: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"Invalid boolean value for {field_name!r}: {raw!r}. Use one of true/false, yes/no, 1/0."
    )


def _parse_env_number(raw: str, *, field_name: str, kind: type[int] | type[float]) -> int | float:
    try:
        value = kind(raw.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid {kind.__name__} value for {field_name!r}: {raw!r}.") from exc
    if value <= 0:
        raise ValueError(f"{field_name!r} must be positive, got {raw!r}.")
    return value


# Cluster keyword arguments accepted from the environment. Dask cluster classes
# also take keys that load code at start-up (``preload``, ``worker_class``,
# ``security``, job-script prologues), and a dotenv file is not a place to
# decide what runs. Anything else goes through ``DaskSession(cluster_kwargs=...)``
# in code.
ENV_CLUSTER_KWARGS: frozenset[str] = frozenset(
    {
        # sizing
        "n_workers",
        "threads_per_worker",
        "processes",
        "memory_limit",
        "nanny",
        "cores",
        "memory",
        "queue",
        "walltime",
        "account",
        "project",
        "resources",
        # placement and naming
        "name",
        "namespace",
        "image",
        "interface",
        "host",
        "protocol",
        "dashboard_address",
        "scheduler_port",
        "local_directory",
        "log_directory",
    }
)


def _parse_env_cluster_kwargs(raw: str, *, field_name: str) -> dict[str, Any]:
    kwargs = _parse_env_json_mapping(raw, field_name=field_name)
    refused = sorted(key for key in kwargs if key not in ENV_CLUSTER_KWARGS)
    if refused:
        raise ValueError(
            f"{field_name!r} from the environment does not accept {refused!r}. Keys accepted "
            f"from the environment: {sorted(ENV_CLUSTER_KWARGS)}. Pass other cluster arguments "
            "as DaskSession(cluster_kwargs=...) in code."
        )
    return kwargs


def _parse_env_json_mapping(raw: str, *, field_name: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON value for {field_name!r}: {raw!r}. Provide a JSON object."
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid value for {field_name!r}: expected a JSON object.")
    return dict(parsed)


def _parse_env_list(raw: str | None) -> list[str]:
    """Comma-separated entries, blanks dropped (``{PREFIX}UPLOAD``)."""
    if raw is None:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _load_dotenv_values(env_file: str | Path | None) -> dict[str, str]:
    if env_file is None:
        return {}
    path = Path(env_file)
    if not path.exists():
        return {}
    # Delegate to boti's validated loader (rejects NUL bytes, control
    # characters, and malformed variable names) instead of hand-parsing.
    return load_dotenv_values(path)


@dataclass(slots=True)
class DaskSessionSettings:
    scheduler_address: str | None = None
    shared: bool = False
    shared_key: str | None = None
    verify_connectivity: bool = True
    connectivity_timeout: float = DEFAULT_CONNECTIVITY_TIMEOUT
    min_workers: int = DEFAULT_MIN_WORKERS
    cluster_kwargs: dict[str, Any] = field(default_factory=dict)
    client_kwargs: dict[str, Any] = field(default_factory=dict)
    # One of boti_dask.clusters.CLUSTER_KINDS; None keeps DaskSession's default
    # (LocalCluster). Ignored when scheduler_address is set.
    cluster_kind: str | None = None
    # A positive number of seconds turns on the watchdog (see boti_dask.watchdog).
    watchdog_interval: float | None = None
    watchdog_failures: int = DEFAULT_WATCHDOG_FAILURES
    # none | memory | otel (see boti_dask.telemetry); None leaves the process provider alone.
    telemetry: str | None = None
    # Paths or requirement strings shipped to workers on open (boti_dask.upload).
    upload: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.cluster_kind is not None:
            from .clusters import normalise_cluster_kind

            self.cluster_kind = normalise_cluster_kind(self.cluster_kind)

    @classmethod
    def from_env_prefix(
        cls,
        prefix: str,
        *,
        env_file: str | Path | None = None,
    ) -> DaskSessionSettings:
        normalized_prefix = _validate_env_prefix(prefix)
        merged = _load_dotenv_values(env_file)
        merged.update({k: v for k, v in os.environ.items() if isinstance(v, str)})

        scheduler_address = merged.get(f"{normalized_prefix}SCHEDULER_ADDRESS")
        shared_raw = merged.get(f"{normalized_prefix}SHARED")
        shared_key = merged.get(f"{normalized_prefix}SHARED_KEY")
        verify_raw = merged.get(f"{normalized_prefix}VERIFY_CONNECTIVITY")
        timeout_raw = merged.get(f"{normalized_prefix}CONNECTIVITY_TIMEOUT")
        min_workers_raw = merged.get(f"{normalized_prefix}MIN_WORKERS")
        cluster_kwargs_raw = merged.get(f"{normalized_prefix}CLUSTER_KWARGS")
        client_kwargs_raw = merged.get(f"{normalized_prefix}CLIENT_KWARGS")
        cluster_kind = merged.get(f"{normalized_prefix}CLUSTER_KIND")
        watchdog_interval_raw = merged.get(f"{normalized_prefix}WATCHDOG_INTERVAL")
        watchdog_failures_raw = merged.get(f"{normalized_prefix}WATCHDOG_FAILURES")
        telemetry = merged.get(f"{normalized_prefix}TELEMETRY")
        upload_raw = merged.get(f"{normalized_prefix}UPLOAD")

        return cls(
            scheduler_address=scheduler_address or None,
            shared=False
            if shared_raw is None
            else _parse_env_bool(shared_raw, field_name="shared"),
            shared_key=shared_key or None,
            verify_connectivity=True
            if verify_raw is None
            else _parse_env_bool(verify_raw, field_name="verify_connectivity"),
            connectivity_timeout=DEFAULT_CONNECTIVITY_TIMEOUT
            if timeout_raw is None
            else _parse_env_number(timeout_raw, field_name="connectivity_timeout", kind=float),
            min_workers=DEFAULT_MIN_WORKERS
            if min_workers_raw is None
            else int(_parse_env_number(min_workers_raw, field_name="min_workers", kind=int)),
            cluster_kwargs={}
            if cluster_kwargs_raw is None
            else _parse_env_cluster_kwargs(cluster_kwargs_raw, field_name="cluster_kwargs"),
            client_kwargs={}
            if client_kwargs_raw is None
            else _parse_env_json_mapping(client_kwargs_raw, field_name="client_kwargs"),
            cluster_kind=cluster_kind or None,
            watchdog_interval=None
            if watchdog_interval_raw is None
            else _parse_env_number(
                watchdog_interval_raw, field_name="watchdog_interval", kind=float
            ),
            watchdog_failures=DEFAULT_WATCHDOG_FAILURES
            if watchdog_failures_raw is None
            else int(
                _parse_env_number(watchdog_failures_raw, field_name="watchdog_failures", kind=int)
            ),
            telemetry=telemetry or None,
            upload=_parse_env_list(upload_raw),
        )

    def to_session_kwargs(self) -> dict[str, Any]:
        from .session_helpers import ConnectivityCheck

        payload: dict[str, Any] = {
            "shared": self.shared,
            "verify_connectivity": ConnectivityCheck(
                timeout=self.connectivity_timeout, min_workers=self.min_workers
            )
            if self.verify_connectivity
            else False,
            "cluster_kwargs": dict(self.cluster_kwargs),
            "client_kwargs": dict(self.client_kwargs),
        }
        if self.scheduler_address:
            payload["scheduler_address"] = self.scheduler_address
        if self.shared_key:
            payload["shared_key"] = self.shared_key
        if self.cluster_kind and not self.scheduler_address:
            from .clusters import cluster_factory

            payload["cluster_factory"] = cluster_factory(self.cluster_kind)
        if self.watchdog_interval is not None:
            from .watchdog import WatchdogConfig

            payload["watchdog"] = WatchdogConfig(
                interval=self.watchdog_interval, failures_before_rebind=self.watchdog_failures
            )
        if self.telemetry:
            from .telemetry import telemetry_provider

            payload["telemetry"] = telemetry_provider(self.telemetry)
        if self.upload:
            payload["upload"] = list(self.upload)
        return payload
