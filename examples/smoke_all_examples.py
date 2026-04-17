"""
Run all boti-dask example scripts as a lightweight smoke test.

Usage:
    uv run python examples/smoke_all_examples.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def discover_examples(examples_dir: Path) -> list[Path]:
    return sorted(
        p for p in examples_dir.glob("*.py") if p.name != "smoke_all_examples.py"
    )


def run_script(repo_root: Path, script: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(script)],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    examples_dir = repo_root / "examples"
    scripts = discover_examples(examples_dir)

    if not scripts:
        print("No example scripts found.")
        return 0

    failures: list[tuple[Path, subprocess.CompletedProcess[str]]] = []

    print(f"Running {len(scripts)} example scripts...")
    for script in scripts:
        rel = script.relative_to(repo_root)
        print(f"=== RUN {rel} ===")
        result = run_script(repo_root, script)
        if result.returncode == 0:
            print(f"PASS {rel}")
            continue

        print(f"FAIL {rel} (exit={result.returncode})")
        failures.append((script, result))

    print()
    print(f"Completed: {len(scripts) - len(failures)} passed, {len(failures)} failed")

    if not failures:
        return 0

    print("\nFailure details:")
    for script, result in failures:
        rel = script.relative_to(repo_root)
        print(f"\n--- {rel} ---")
        if result.stdout.strip():
            print("[stdout]")
            print(result.stdout.rstrip())
        if result.stderr.strip():
            print("[stderr]")
            print(result.stderr.rstrip())

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

