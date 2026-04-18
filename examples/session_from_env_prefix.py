"""Demonstrate loading Dask session defaults from prefixed environment variables."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from boti_dask import DaskSessionSettings, dask_session_from_env_prefix


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp_dir:
        env_file = Path(tmp_dir) / ".env"
        env_file.write_text(
            "\n".join(
                [
                    "BOTI_DASK_SCHEDULER_ADDRESS=tcp://scheduler:8786",
                    "BOTI_DASK_SHARED=true",
                    "BOTI_DASK_SHARED_KEY=example-shared-session",
                    "BOTI_DASK_VERIFY_CONNECTIVITY=false",
                    'BOTI_DASK_CLUSTER_KWARGS={"n_workers":1,"threads_per_worker":1}',
                    'BOTI_DASK_CLIENT_KWARGS={"set_as_default":false}',
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        # Environment variables override dotenv values when keys overlap.
        os.environ["BOTI_DASK_SHARED"] = "false"

        settings = DaskSessionSettings.from_env_prefix("BOTI_DASK_", env_file=env_file)
        session = dask_session_from_env_prefix(
            "BOTI_DASK_",
            env_file=env_file,
            verify_connectivity=True,
        )

        print("Loaded settings:", settings)
        print("Session kwargs:", session)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

