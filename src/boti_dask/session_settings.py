from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from boti.core.settings import load_dotenv_values

_ENV_PREFIX_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*_?$")


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
    verify_connectivity: bool = False
    cluster_kwargs: dict[str, Any] = field(default_factory=dict)
    client_kwargs: dict[str, Any] = field(default_factory=dict)

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
        cluster_kwargs_raw = merged.get(f"{normalized_prefix}CLUSTER_KWARGS")
        client_kwargs_raw = merged.get(f"{normalized_prefix}CLIENT_KWARGS")

        return cls(
            scheduler_address=scheduler_address or None,
            shared=False
            if shared_raw is None
            else _parse_env_bool(shared_raw, field_name="shared"),
            shared_key=shared_key or None,
            verify_connectivity=False
            if verify_raw is None
            else _parse_env_bool(verify_raw, field_name="verify_connectivity"),
            cluster_kwargs={}
            if cluster_kwargs_raw is None
            else _parse_env_json_mapping(cluster_kwargs_raw, field_name="cluster_kwargs"),
            client_kwargs={}
            if client_kwargs_raw is None
            else _parse_env_json_mapping(client_kwargs_raw, field_name="client_kwargs"),
        )

    def to_session_kwargs(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "shared": self.shared,
            "verify_connectivity": self.verify_connectivity,
            "cluster_kwargs": dict(self.cluster_kwargs),
            "client_kwargs": dict(self.client_kwargs),
        }
        if self.scheduler_address:
            payload["scheduler_address"] = self.scheduler_address
        if self.shared_key:
            payload["shared_key"] = self.shared_key
        return payload
