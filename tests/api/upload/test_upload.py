"""Uploads reach the workers once per scheduler and are skipped afterwards."""

from __future__ import annotations

import importlib
import zipfile

import pytest

from boti_dask import DaskSession, DaskSessionSettings, dask_session, sync_uploads
from boti_dask import session as session_module
from boti_dask.upload import UPLOAD_DATASET, PayloadKind, classify_payload

_LOCAL = {"n_workers": 1, "threads_per_worker": 1, "processes": False, "dashboard_address": ":0"}


@pytest.fixture
def package_dir(tmp_path):
    pkg = tmp_path / "boti_upload_probe"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("def answer():\n    return 42\n")
    (pkg / "__pycache__").mkdir()
    (pkg / "__pycache__" / "junk.pyc").write_bytes(b"\x00")
    return pkg


def _import_probe_answer() -> int:
    import importlib

    module = importlib.import_module("boti_upload_probe")
    return module.answer()


def test_directory_digest_is_deterministic_and_skips_caches(package_dir, tmp_path):
    first = classify_payload(package_dir)
    (package_dir / "__pycache__" / "more.pyc").write_bytes(b"\x01")
    assert classify_payload(package_dir).digest == first.digest
    assert first.kind is PayloadKind.DIRECTORY
    (package_dir / "extra.py").write_text("x = 1\n")
    assert classify_payload(package_dir).digest != first.digest


def test_classify_files_wheels_and_requirements(tmp_path):
    script = tmp_path / "helper.py"
    script.write_text("y = 2\n")
    assert classify_payload(script).kind is PayloadKind.FILE
    wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as z:
        z.writestr("pkg/__init__.py", "")
    assert classify_payload(wheel).kind is PayloadKind.WHEEL
    assert classify_payload(" rich>=13 ").kind is PayloadKind.REQUIREMENT
    with pytest.raises(ValueError, match="requirement string"):
        classify_payload(tmp_path / "data.csv") if (tmp_path / "data.csv").write_text("a") else None
    with pytest.raises(ValueError, match="non-empty"):
        classify_payload("   ")


def test_uploaded_directory_is_importable_on_a_worker(package_dir):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("boti_upload_probe")
    with dask_session(cluster_kwargs=_LOCAL, upload=[package_dir]) as client:
        assert client.submit(_import_probe_answer, pure=False).result(timeout=30) == 42
        registry = client.get_dataset(UPLOAD_DATASET)
        assert list(registry.values()) == [f"directory:{package_dir}"]


class StubClient:
    """Scheduler-side state (datasets) is shared so two clients look like one scheduler."""

    def __init__(self, datasets: dict, address=None, **_kwargs) -> None:
        self.datasets = datasets
        self.status = "running"
        self.uploaded: list[str] = []
        self.plugins: list = []

    def upload_file(self, filename, load=True):
        self.uploaded.append(filename.rsplit("/", 1)[-1])

    def register_plugin(self, plugin, name=None, idempotent=None):
        self.plugins.append((name, plugin))

    def get_dataset(self, name, default=None):
        return self.datasets.get(name, default)

    def publish_dataset(self, value, *, name, override=False):
        self.datasets[name] = value

    def close(self) -> None:
        self.status = "closed"


def test_second_session_against_the_same_scheduler_does_not_resend(
    monkeypatch, package_dir, tmp_path
):
    scheduler_datasets: dict = {}
    clients: list[StubClient] = []

    def make_client(address, **kwargs):
        client = StubClient(scheduler_datasets, address, **kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr(session_module, "Client", make_client)
    script = tmp_path / "helper.py"
    script.write_text("z = 3\n")
    payloads = [package_dir, script, "rich>=13", "polars"]
    for _ in range(2):
        session = DaskSession(
            scheduler_address="tcp://stub:1", verify_connectivity=False, upload=payloads
        )
        session.open()
        session.close()
    assert clients[0].uploaded == ["boti_upload_probe.zip", "helper.py"]
    assert len(clients[0].plugins) == 1
    name, plugin = clients[0].plugins[0]
    assert type(plugin).__name__ == "PipInstall" and name.startswith("boti-dask-pip-")
    assert plugin._install_fn.packages == ["rich>=13", "polars"]  # distributed's own layout
    assert clients[1].uploaded == [] and clients[1].plugins == []
    assert len(scheduler_datasets[UPLOAD_DATASET]) == 4


def test_only_new_payloads_are_sent(monkeypatch, tmp_path):
    datasets: dict = {}
    client = StubClient(datasets)
    a = tmp_path / "a.py"
    a.write_text("a = 1\n")
    b = tmp_path / "b.py"
    b.write_text("b = 2\n")
    assert [p.source for p in sync_uploads(client, [a])] == [str(a)]
    assert [p.source for p in sync_uploads(client, [a, b])] == [str(b)]
    assert sync_uploads(client, [a, b]) == []
    assert client.uploaded == ["a.py", "b.py"]


def test_settings_read_upload_list(monkeypatch):
    monkeypatch.setenv("DASK_SESSION_UPLOAD", " ./src/mypkg, rich>=13 ,,")
    settings = DaskSessionSettings.from_env_prefix("DASK_SESSION_")
    assert settings.upload == ["./src/mypkg", "rich>=13"]
    assert settings.to_session_kwargs()["upload"] == ["./src/mypkg", "rich>=13"]
    assert "upload" not in DaskSessionSettings().to_session_kwargs()


def test_a_missing_path_is_an_error_not_a_requirement(tmp_path):
    """A mistyped path must never turn into a pip install on every worker."""
    with pytest.raises(FileNotFoundError, match="does not exist"):
        classify_payload(tmp_path / "etl_jobs")
    with pytest.raises(FileNotFoundError, match="does not exist"):
        classify_payload("./src/etl_jobs")
    with pytest.raises(FileNotFoundError, match="does not exist"):
        classify_payload("jobs.py")


@pytest.mark.parametrize(
    "entry",
    [
        "-r requirements.txt",
        "--index-url http://127.0.0.1/simple rich",
        "rich @ git+https://example.invalid/rich.git",
        "${PIP_EXTRA} rich",
        "rich; evil",
    ],
)
def test_requirement_strings_pip_would_read_as_options_or_urls_are_refused(entry):
    with pytest.raises(ValueError, match="requirement"):
        classify_payload(entry)


def test_plain_requirements_still_classify():
    for entry in ("rich", "rich>=13,<14", "rich[jupyter]>=13; python_version >= '3.10'"):
        assert classify_payload(entry).kind is PayloadKind.REQUIREMENT


def test_symlinks_inside_an_upload_directory_are_not_shipped(package_dir, tmp_path):
    secret = tmp_path / "outside.txt"
    secret.write_text("token=abc\n")
    (package_dir / "leak.py").symlink_to(secret)
    outside_dir = tmp_path / "outside_dir"
    outside_dir.mkdir()
    (outside_dir / "mod.py").write_text("x = 1\n")
    (package_dir / "linked").symlink_to(outside_dir, target_is_directory=True)
    (package_dir / "real.py").write_text("y = 2\n")

    archived: list[list[str]] = []

    class ArchiveReadingClient(StubClient):
        def upload_file(self, filename, load=True):
            with zipfile.ZipFile(filename) as archive:
                archived.append(sorted(archive.namelist()))

    sync_uploads(ArchiveReadingClient({}), [package_dir])
    assert archived == [["boti_upload_probe/__init__.py", "boti_upload_probe/real.py"]]
