"""
Async distributed pipeline: persist, wait, head, compute, gather, and
unique-value extraction — all through async-safe wrappers.
"""

from __future__ import annotations

import asyncio

import dask
import dask.dataframe as dd
import pandas as pd

from boti_dask import (
    UniqueValuesExtractor,
    async_safe_compute,
    async_safe_gather,
    async_safe_head,
    async_safe_persist,
    async_safe_wait,
    dask_session,
)

CLUSTER_KWARGS = {
    "n_workers": 2,
    "threads_per_worker": 1,
    "processes": False,
    "dashboard_address": ":0",
}


async def _pipeline() -> dict[str, object]:
    frame = dd.from_pandas(
        pd.DataFrame(
            {"id": range(100), "group": ["A", "B"] * 50}
        ),
        npartitions=4,
    )

    with dask_session(cluster_kwargs=CLUSTER_KWARGS) as client:
        # 1. Persist the frame across workers
        persisted = await async_safe_persist(frame, dask_client=client)
        await async_safe_wait(persisted, dask_client=client, timeout=30)

        # 2. Multiple async operations
        delayed_ops = [dask.delayed(lambda v=v: v * 10)() for v in (1, 2, 3)]

        preview = await async_safe_head(persisted, n=5, dask_client=client)
        gathered = await async_safe_gather(delayed_ops, dask_client=client)
        total = await async_safe_compute(
            persisted["id"].sum(), dask_client=client
        )

        # 3. Concurrent unique-value extraction
        extractor = UniqueValuesExtractor(dask_client=client)
        uniques = await extractor.extract_unique_values(
            persisted, "group", "id", limit=5
        )

        del persisted

    return {
        "preview_ids": preview["id"].tolist(),
        "gathered": gathered,
        "total": int(total),
        "unique_groups": uniques["group"],
    }


def main() -> dict[str, object]:
    result = asyncio.run(_pipeline())
    for key, value in result.items():
        print(f"{key}: {value}")
    return result


if __name__ == "__main__":
    main()
