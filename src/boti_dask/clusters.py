"""Cluster backends selected by name: ``local``, ``kubernetes``, ``slurm`` and ``coiled``.

Each backend is a third-party cluster class imported only when asked for, so
this module costs nothing until :func:`cluster_factory` runs. A missing
backend package raises a :class:`ModuleNotFoundError` that names the boti-dask
extra to install, in the spirit of boti-data's ``dataframes_required``.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

__all__ = ["CLUSTER_KINDS", "cluster_factory", "make_cluster", "normalise_cluster_kind"]


@dataclass(frozen=True, slots=True)
class _Backend:
    module: str
    attribute: str
    extra: str | None


# Kind -> where its cluster class lives and which extra installs it. ``local``
# is distributed's own LocalCluster, always present.
_BACKENDS: dict[str, _Backend] = {
    "local": _Backend("dask.distributed", "LocalCluster", None),
    "kubernetes": _Backend("dask_kubernetes.operator", "KubeCluster", "k8s"),
    "slurm": _Backend("dask_jobqueue", "SLURMCluster", "slurm"),
    "coiled": _Backend("coiled", "Cluster", "coiled"),
}

CLUSTER_KINDS: tuple[str, ...] = tuple(_BACKENDS)


def normalise_cluster_kind(kind: str) -> str:
    """Lower-case and validate *kind*, raising ``ValueError`` listing the known kinds."""
    normalised = kind.strip().lower()
    if normalised not in _BACKENDS:
        raise ValueError(f"Unknown cluster kind {kind!r}. Known kinds: {', '.join(CLUSTER_KINDS)}.")
    return normalised


def _missing_backend(kind: str, backend: _Backend, exc: ModuleNotFoundError) -> ModuleNotFoundError:
    extra = backend.extra or "distributed"
    return ModuleNotFoundError(
        f"Cluster kind {kind!r} needs {backend.module.split('.', 1)[0]!r}, which is not "
        f"installed. Install boti-dask's {extra!r} extra: "
        f"\"pip install 'boti-dask[{extra}]'\" (or \"uv add 'boti-dask[{extra}]'\").",
        name=exc.name,
        path=exc.path,
    )


def cluster_factory(kind: str) -> Callable[..., Any]:
    """Return the cluster class for *kind*, importing its package on first use."""
    normalised = normalise_cluster_kind(kind)
    backend = _BACKENDS[normalised]
    try:
        module = importlib.import_module(backend.module)
    except ModuleNotFoundError as exc:
        raise _missing_backend(normalised, backend, exc) from exc
    return getattr(module, backend.attribute)


# The public convenience over cluster_factory(); the "underlying object" is the
# factory itself, exported right beside it.
# spaghetti-ignore[pass-through-method]
def make_cluster(kind: str, **cluster_kwargs: Any) -> Any:
    """Instantiate a cluster of *kind* with *cluster_kwargs*.

    The returned object is whatever the backend builds (``LocalCluster``,
    ``KubeCluster``, ``SLURMCluster`` or ``coiled.Cluster``); pass it to
    ``Client`` or let :class:`~boti_dask.DaskSession` do so through
    ``cluster_factory=cluster_factory(kind)``.
    """
    return cluster_factory(kind)(**cluster_kwargs)
