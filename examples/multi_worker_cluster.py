"""
Multi-worker cluster: launches 4 workers (2 threads each), distributes a
DataFrame across partitions, and runs grouped aggregations in parallel.
"""

from __future__ import annotations

import dask.dataframe as dd
import pandas as pd

from boti_dask import (
    apply_recommended_dask_config,
    dask_is_empty,
    dask_session,
    describe_client,
    safe_compute,
    safe_persist,
    safe_wait,
)


def main() -> dict[str, object]:
    with apply_recommended_dask_config():
        with dask_session(
            verify_connectivity=True,
            cluster_kwargs={
                "n_workers": 4,
                "threads_per_worker": 2,
                "processes": False,
                "dashboard_address": ":0",
            },
        ) as client:
            info = describe_client(client)
            print(f"Scheduler: {info['scheduler']}")
            print(f"Workers: {info['workers']}, total threads: {info['threads']}")

            size = 9_999
            frame = dd.from_pandas(
                pd.DataFrame(
                    {
                        "id": range(size),
                        "value": range(size),
                        "category": ["X", "Y", "Z"] * (size // 3),
                    }
                ),
                npartitions=8,
            )

            persisted = safe_persist(frame, dask_client=client)
            safe_wait(persisted, dask_client=client, timeout=30)

            # Distributed computations spread across the 4 workers
            total = safe_compute(persisted["value"].sum(), dask_client=client)
            grouped = safe_compute(
                persisted.groupby("category")["value"].sum(),
                dask_client=client,
            )

            empty = dask_is_empty(persisted, dask_client=client)
            print(f"Sum of 'value': {int(total)}")
            for cat, val in grouped.items():
                print(f"  {cat}: {int(val)}")
            print(f"Empty: {empty}")

            del persisted

    return {
        "workers": info["workers"],
        "threads": info["threads"],
        "total": int(total),
        "grouped": {k: int(v) for k, v in grouped.items()},
        "empty": empty,
    }


if __name__ == "__main__":
    main()
