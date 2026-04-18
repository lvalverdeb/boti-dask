"""
Example showing sync, async, and distributed resilience helpers.
"""

from __future__ import annotations

import asyncio

import dask
import dask.dataframe as dd
import pandas as pd

from boti_dask import (
    UniqueValuesExtractor,
    apply_recommended_dask_config,
    async_safe_compute,
    async_safe_gather,
    async_safe_head,
    async_safe_persist,
    async_safe_wait,
    dask_is_empty,
    dask_is_probably_empty,
    dask_session,
    describe_client,
    inspect_graph,
    safe_compute,
    safe_gather,
    safe_head,
    safe_persist,
    safe_wait,
)


async def _run_async_example(*, client, frame) -> tuple[int, list[int], list[int]]:
    delayed_value = dask.delayed(lambda: 6 * 7)()
    persisted = await async_safe_persist(delayed_value, dask_client=client)
    await async_safe_wait(persisted, dask_client=client, timeout=5)
    gathered = await async_safe_gather(
        [dask.delayed(lambda value=value: value + 1)() for value in (1, 2)],
        dask_client=client,
    )
    preview = await async_safe_head(frame, n=2, dask_client=client)
    return (
        await async_safe_compute(delayed_value, dask_client=client),
        gathered,
        preview["id"].tolist(),
    )


def run_example() -> dict[str, object]:
    frame = dd.from_pandas(
        pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "status": ["active", "inactive", "active", "active"],
            }
        ),
        npartitions=2,
    )
    graph_metrics = inspect_graph(frame)

    with apply_recommended_dask_config():
        with dask_session(
            verify_connectivity=True,
            shared=True,
            shared_key="data-facade-dask-resilience",
            cluster_kwargs={
                "n_workers": 1,
                "threads_per_worker": 1,
                "processes": False,
                "dashboard_address": ":0",
            }
        ) as client:
            with dask_session(
                verify_connectivity=True,
                shared=True,
                shared_key="data-facade-dask-resilience",
                cluster_kwargs={
                    "n_workers": 1,
                    "threads_per_worker": 1,
                    "processes": False,
                    "dashboard_address": ":0",
                },
            ) as reused_client:
                client_summary = describe_client(client)
                shared_client_reused = client is reused_client
                persisted_frame = safe_persist(frame, dask_client=client)
                safe_wait(persisted_frame, dask_client=client, timeout=5)
                persisted_partitions = persisted_frame.npartitions
                sync_total = int(safe_compute(frame["id"].sum(), dask_client=client))
                sync_preview = safe_head(persisted_frame, n=2, dask_client=client)
                sync_gather = safe_gather(
                    [dask.delayed(lambda value=value: value * 10)() for value in (1, 2)],
                    dask_client=client,
                )
                async_total, async_gather, async_preview_ids = asyncio.run(
                    _run_async_example(client=client, frame=persisted_frame)
                )
                probably_empty = dask_is_probably_empty(persisted_frame)
                is_empty = dask_is_empty(persisted_frame, dask_client=client)
                unique_values = asyncio.run(
                    UniqueValuesExtractor(dask_client=client).extract_unique_values(
                        persisted_frame,
                        "status",
                        "id",
                        limit=10,
                    )
                )
                del persisted_frame

    return {
        "client": client_summary,
        "graph_metrics": graph_metrics,
        "sync_total": sync_total,
        "async_total": async_total,
        "sync_preview_ids": sync_preview["id"].tolist(),
        "sync_gather": sync_gather,
        "async_gather": async_gather,
        "async_preview_ids": async_preview_ids,
        "partitions": persisted_partitions,
        "probably_empty": probably_empty,
        "is_empty": is_empty,
        "unique_values": unique_values,
        "shared_client_reused": shared_client_reused,
    }


def main() -> dict[str, object]:
    result = run_example()
    print("Resilience graph metrics:")
    print(result["graph_metrics"])
    print("\nDistributed session client:")
    print(result["client"])
    print(f"Shared session reused: {result['shared_client_reused']}")
    print(f"\nSync resilient total: {result['sync_total']}")
    print(f"Sync preview ids: {result['sync_preview_ids']}")
    print(f"Sync gathered values: {result['sync_gather']}")
    print(f"Async resilient total: {result['async_total']}")
    print(f"Async preview ids: {result['async_preview_ids']}")
    print(f"Async gathered values: {result['async_gather']}")
    print(f"Persisted partitions: {result['partitions']}")
    print(f"Probably empty: {result['probably_empty']}")
    print(f"Is empty: {result['is_empty']}")
    print(f"Unique statuses: {result['unique_values']['status']}")
    return result


if __name__ == "__main__":
    main()
