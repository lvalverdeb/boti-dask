"""
Orphaned graph scenarios: demonstrates what happens when a Dask collection
outlives the session that created it, plus shared and external-client patterns
that avoid the orphan problem.
"""

from __future__ import annotations

import warnings

import dask
import dask.dataframe as dd
import pandas as pd

from boti_dask import (
    dask_session,
    safe_compute,
    safe_head,
    safe_persist,
    safe_wait,
)

CLUSTER_KWARGS = {
    "n_workers": 1,
    "threads_per_worker": 1,
    "processes": False,
    "dashboard_address": ":0",
}


def main() -> dict[str, object]:
    frame = dd.from_pandas(
        pd.DataFrame({"id": [1, 2, 3]}), npartitions=2
    )
    results: dict[str, object] = {}

    # -- Scenario 1: Orphaned graph -------------------------------------------
    print("=== Scenario 1: Orphaned graph from managed session ===")
    with warnings.catch_warnings(record=True) as caught:
        with dask_session(cluster_kwargs=CLUSTER_KWARGS) as client:
            persisted = safe_persist(frame, dask_client=client)
            safe_wait(persisted, dask_client=client, timeout=5)

        warning_messages = [str(w.message) for w in caught]
        for msg in warning_messages:
            print(f"  [warning] {msg}")

    results["scenario1_warning_count"] = len(warning_messages)

    try:
        safe_head(persisted)
        results["scenario1_raised"] = False
    except RuntimeError as e:
        print(f"  [error] {e}")
        results["scenario1_raised"] = True
        results["scenario1_error"] = str(e)

    # -- Scenario 2: External client survives session close --------------------
    print()
    print("=== Scenario 2: External client survives session close ===")
    from dask.distributed import Client, LocalCluster

    with LocalCluster(
        n_workers=1, threads_per_worker=1, processes=False, dashboard_address=":0"
    ) as cluster:
        external = Client(cluster)
        with dask_session(client=external) as session_client:
            delayed = dask.delayed(lambda: 42)()
            result = safe_compute(delayed, dask_client=session_client)
            print(f"  Computed: {result}")

        print(f"  External client status after session: {external.status}")
        results["scenario2_client_alive"] = external.status == "running"
        results["scenario2_result"] = result
        external.close()

    # -- Scenario 3: Shared session with staggered release --------------------
    print()
    print("=== Scenario 3: Shared session with staggered release ===")
    with dask_session(
        cluster_kwargs=CLUSTER_KWARGS,
        shared=True,
        shared_key="staggered-demo",
    ) as outer:
        outer_result = safe_compute(
            dask.delayed(lambda: "outer")(), dask_client=outer
        )
        with dask_session(
            shared=True,
            shared_key="staggered-demo",
        ) as inner:
            inner_result = safe_compute(
                dask.delayed(lambda: "inner")(), dask_client=inner
            )
            print(f"  [inner] {inner_result}, alive: {inner.status == 'running'}")
        # outer still holds a ref, so the client is still alive
        results["scenario3_client_alive_at_inner"] = inner.status == "running"
        print(f"  [outer] {outer_result}, alive: {outer.status == 'running'}")

    results["scenario3_outer"] = outer_result
    results["scenario3_inner"] = inner_result

    # -- Scenario 4: Local fallback when no client is available ----------------
    print()
    print("=== Scenario 4: Local fallback (no Dask DataFrame) ===")
    delayed = dask.delayed(lambda: 99)()
    local_result = safe_compute(delayed)
    print(f"  Local fallback result: {local_result}")
    results["scenario4_local_result"] = local_result

    print()
    print("[done] all orphaned graph scenarios completed")
    return results


if __name__ == "__main__":
    main()
