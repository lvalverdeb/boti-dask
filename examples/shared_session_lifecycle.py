"""
Shared session lifecycle: multiple contexts reuse the same Dask client
via reference counting in SessionPool.  The client stays alive until the
last context exits.
"""

from __future__ import annotations

from boti_dask import dask_session, describe_client

CLUSTER_KWARGS = {
    "n_workers": 2,
    "threads_per_worker": 1,
    "processes": False,
    "dashboard_address": ":0",
}
SHARED_KEY = "shared-lifecycle-demo"


def main() -> dict[str, object]:
    outer_before: dict[str, object] = {}
    inner: dict[str, object] = {}
    outer_after: dict[str, object] = {}
    session_status: str = ""

    with dask_session(
        cluster_kwargs=CLUSTER_KWARGS,
        shared=True,
        shared_key=SHARED_KEY,
    ) as outer_client:
        outer_before = describe_client(outer_client)
        print(f"[outer] workers: {outer_before['workers']}")

        with dask_session(
            shared=True,
            shared_key=SHARED_KEY,
        ) as inner_client:
            inner_info = describe_client(inner_client)
            inner = {
                "same_client": outer_client is inner_client,
                "dashboard": inner_info["dashboard"],
            }
            print(f"[inner] same client? {inner['same_client']}")

        # After inner context exits, the shared session is still alive
        # because outer still holds a reference.
        outer_after = describe_client(outer_client)
        print(f"[outer] still connected: {outer_after['workers']} workers")

    # After outer context exits, ref_count reaches zero and the client is closed.
    session_status = outer_client.status
    print(f"[done] client status: {session_status}")

    return {
        "outer_before_workers": outer_before["workers"],
        "same_client": inner["same_client"],
        "outer_after_workers": outer_after["workers"],
        "session_status": session_status,
    }


if __name__ == "__main__":
    main()
