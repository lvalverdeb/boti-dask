"""The package import surface stays Dask-free until a Dask name is touched."""

from __future__ import annotations

import json
import subprocess
import sys

import boti_dask

# pyarrow and numpy are deliberately absent: boti.core imports pyarrow.fs for
# its sandboxed filesystem, so any boti-backed name brings them in. The Dask
# chain is what boti_dask itself must not load.
_HEAVY = ("dask", "distributed", "pandas", "polars")

_PROBE = """
import json, sys, time
t0 = time.perf_counter()
import boti_dask
elapsed_ms = (time.perf_counter() - t0) * 1000
after_import = sorted(m for m in {heavy} if m in sys.modules)
boti_dask.DaskSessionSettings
boti_dask.describe_frame
boti_dask.inspect_graph
after_light_names = sorted(m for m in {heavy} if m in sys.modules)
boti_dask.DaskSession
after_session = "distributed" in sys.modules
print(json.dumps({{"elapsed_ms": elapsed_ms, "after_import": after_import,
                   "after_light_names": after_light_names, "after_session": after_session}}))
"""


def _probe() -> dict:
    out = subprocess.run(
        [sys.executable, "-I", "-c", _PROBE.format(heavy=_HEAVY)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_import_boti_dask_loads_no_engine_and_stays_fast():
    result = _probe()
    assert result["after_import"] == [], result
    assert result["after_light_names"] == [], result
    # Generous bound: the goal is under 50 ms warm; CI runners are slower.
    assert result["elapsed_ms"] < 250, result


def test_touching_a_dask_name_imports_distributed():
    assert _probe()["after_session"] is True


def test_every_exported_name_resolves_and_is_listed_by_dir():
    listed = set(dir(boti_dask))
    for name in boti_dask.__all__:
        assert name in listed
        assert getattr(boti_dask, name) is not None
    assert "STABLE" in listed and "deprecated" in listed


def test_unknown_name_raises_attribute_error():
    try:
        boti_dask.not_a_real_name
    except AttributeError as exc:
        assert "not_a_real_name" in str(exc)
    else:
        raise AssertionError("expected AttributeError")
