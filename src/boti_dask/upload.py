"""Ship code to the workers once: files, directories, pure-Python wheels and requirements.

:func:`sync_uploads` sends each payload through ``Client.upload_file`` (files,
directories zipped deterministically, wheels renamed so workers put them on
``sys.path``) or a ``PipInstall`` plugin (requirement strings), and records the
SHA-256 of every payload in the scheduler dataset ``boti-dask:uploads`` so a
later session against the same scheduler skips what is already there.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement

from ._internal import _log

__all__ = ["UPLOAD_DATASET", "Payload", "PayloadKind", "classify_payload", "sync_uploads"]

UPLOAD_DATASET = "boti-dask:uploads"
_module_log = logging.getLogger(__name__)

_LOADABLE_SUFFIXES = frozenset({".py", ".zip", ".egg", ".pyz"})
_SKIPPED_PARTS = frozenset({"__pycache__", ".git", ".pytest_cache", ".mypy_cache"})
_EPOCH = (1980, 1, 1, 0, 0, 0)  # zip's earliest timestamp, fixed for reproducible bytes
_PLUGIN_NAME_DIGEST_CHARS = 16
_LABEL_DIGEST_CHARS = 12


class PayloadKind(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"
    WHEEL = "wheel"
    REQUIREMENT = "requirement"


@dataclass(frozen=True, slots=True)
class Payload:
    """One thing to ship and the digest that identifies it on the scheduler."""

    kind: PayloadKind
    source: str
    digest: str

    @property
    def label(self) -> str:
        """What the scheduler dataset records: kind, base name and a short digest.

        The dataset is readable by every client of the scheduler, so a path
        payload is labelled by its base name alone, never its directory.
        """
        name = self.source if self.kind is PayloadKind.REQUIREMENT else Path(self.source).name
        return f"{self.kind.value}:{name}@{self.digest[:_LABEL_DIGEST_CHARS]}"

    @property
    def description(self) -> str:
        """Kind and full source, for the local log only."""
        return f"{self.kind.value}:{self.source}"


def _file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _directory_files(root: Path) -> Iterator[Path]:
    """Regular files under *root*, caches and ``.pyc`` left out.

    Symbolic links are skipped, file and directory alike, and every file must
    resolve inside the resolved root: a link in the tree would otherwise ship
    its target's contents to every worker.
    """
    base = root.resolve()
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.is_file() or path.suffix == ".pyc":
            continue
        if _SKIPPED_PARTS & set(path.relative_to(root).parts):
            continue
        if base in path.resolve().parents:
            yield path


def _resolved_directory(root: Path) -> Path:
    """*root* made absolute, so ``.`` or ``..`` still has a name to ship under.

    The filesystem root has no name at all: an archive built from it would
    hold entries with a leading slash that no worker can import.
    """
    resolved = root.expanduser().resolve()
    if not resolved.name:
        raise ValueError(
            f"Cannot upload {root}: the filesystem root has no directory name to ship under."
        )
    return resolved


def _zip_directory(root: Path, target: Path) -> None:
    """Zip *root* as ``root.name/...`` with sorted entries and fixed timestamps."""
    root = _resolved_directory(root)
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in _directory_files(root):
            info = zipfile.ZipInfo(f"{root.name}/{path.relative_to(root).as_posix()}", _EPOCH)
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())


def _looks_like_path(text: str) -> bool:
    if "://" in text or any(ch.isspace() for ch in text):
        return False  # a URL or an option line: the requirement validator names the problem
    return (
        "/" in text
        or os.sep in text
        or text.startswith((".", "~"))
        or Path(text).suffix in _LOADABLE_SUFFIXES | {".whl"}
    )


def _requirement_payload(text: str) -> Payload:
    """A PEP 508 requirement, and nothing pip would read as an option or a URL.

    PipInstall writes each entry as a line of a requirements file on every
    worker, where option lines (``-r``, ``--index-url``), direct URLs and
    ``${VAR}`` expansion are all honoured. Only a parseable name with
    optional extras, version specifiers and markers is accepted.
    """
    try:
        requirement = Requirement(text)
    except InvalidRequirement as exc:
        raise ValueError(
            f"Upload entry {text!r} is neither an existing path nor a requirement "
            f"such as 'package>=1.0' ({exc})."
        ) from exc
    if requirement.url is not None:
        raise ValueError(
            f"Upload entry {text!r} is a direct-URL requirement; ship the wheel file instead."
        )
    return Payload(PayloadKind.REQUIREMENT, text, hashlib.sha256(text.encode()).hexdigest())


def _directory_payload(path: Path) -> Payload:
    path = _resolved_directory(path)
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / f"{path.name}.zip"
        _zip_directory(path, target)
        return Payload(PayloadKind.DIRECTORY, str(path), _file_digest(target))


def _path_payload(path: Path) -> Payload:
    if path.is_dir():
        return _directory_payload(path)
    kind = PayloadKind.WHEEL if path.suffix == ".whl" else PayloadKind.FILE
    if kind is PayloadKind.FILE and path.suffix not in _LOADABLE_SUFFIXES:
        raise ValueError(
            f"Cannot upload {path}: workers load .py, .zip, .egg, .pyz and pure-Python "
            ".whl files; pass a requirement string for anything that needs pip."
        )
    return Payload(kind, str(path), _file_digest(path))


def classify_payload(item: str | Path) -> Payload:
    """Decide how *item* ships and compute its digest; the digest never touches the cluster.

    A path must exist: a missing one is an error rather than a package name,
    so a mistyped path never turns into a pip install on every worker.
    """
    path = Path(item).expanduser()
    if path.exists():
        return _path_payload(path)
    text = str(item).strip()
    if isinstance(item, Path) or _looks_like_path(text):
        raise FileNotFoundError(f"Upload path {item} does not exist.")
    if not text:
        raise ValueError("Upload payloads must be a path or a non-empty requirement string.")
    return _requirement_payload(text)


def _send_file_payload(client: Any, payload: Payload) -> None:
    path = Path(payload.source)
    if payload.kind is PayloadKind.FILE:
        client.upload_file(str(path))
        return
    with tempfile.TemporaryDirectory() as tmp:
        # Workers add .zip files to sys.path but ignore .whl, so a pure-Python
        # wheel (a zip of package directories) is shipped under a .zip name.
        target = Path(tmp) / f"{path.name}.zip"
        if payload.kind is PayloadKind.WHEEL:
            shutil.copyfile(path, target)
        else:
            _zip_directory(path, target)
        client.upload_file(str(target))


def _send_requirements(client: Any, payloads: list[Payload]) -> None:
    if not payloads:
        return
    from distributed.diagnostics.plugin import PipInstall

    packages = [payload.source for payload in payloads]
    client.register_plugin(
        PipInstall(packages=packages), name=f"boti-dask-pip-{_digest_of(packages)}"
    )


def _digest_of(parts: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:_PLUGIN_NAME_DIGEST_CHARS]


def _known_uploads(client: Any) -> dict[str, str]:
    try:
        known = client.get_dataset(UPLOAD_DATASET, default=None)
    except Exception:
        _module_log.debug("get_dataset(%s) failed", UPLOAD_DATASET, exc_info=True)
        return {}
    return dict(known) if isinstance(known, dict) else {}


def sync_uploads(
    client: Any, items: Iterable[str | Path], *, logger: Any | None = None
) -> list[Payload]:
    """Send every payload in *items* the scheduler has not seen; return those sent."""
    payloads = [classify_payload(item) for item in items]
    known = _known_uploads(client)
    pending = [payload for payload in payloads if payload.digest not in known]
    if not pending:
        _log(logger, "debug", f"All {len(payloads)} upload payload(s) already on the cluster.")
        return []
    requirements = [payload for payload in pending if payload.kind is PayloadKind.REQUIREMENT]
    for payload in pending:
        if payload.kind is not PayloadKind.REQUIREMENT:
            _send_file_payload(client, payload)
    _send_requirements(client, requirements)
    known.update({payload.digest: payload.label for payload in pending})
    client.publish_dataset(known, name=UPLOAD_DATASET, override=True)
    _log(
        logger,
        "info",
        f"Uploaded {len(pending)} payload(s) to the cluster: "
        + ", ".join(payload.description for payload in pending),
    )
    return pending
