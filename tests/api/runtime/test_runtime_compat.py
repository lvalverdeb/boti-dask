from __future__ import annotations

import dask
import dask.dataframe as dd
import pandas as pd
import pytest

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


class CaptureLogger:
    def __init__(self) -> None:
        self.debugs: list[str] = []
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def debug(self, message: str, *_args, **_kwargs) -> None:
        self.debugs.append(message)

    def info(self, message: str, *_args, **_kwargs) -> None:
        self.infos.append(message)

    def warning(self, message: str, *_args, **_kwargs) -> None:
        self.warnings.append(message)

    def error(self, message: str, *_args, **_kwargs) -> None:
        self.errors.append(message)


def test_dask_session_connects_to_scheduler_address_and_logs():
    distributed = pytest.importorskip("dask.distributed")
    Client = distributed.Client
    LocalCluster = distributed.LocalCluster
    logger = CaptureLogger()

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster, Client(cluster):
        with dask_session(scheduler_address=cluster.scheduler_address, logger=logger) as client:
            summary = describe_client(client)

    assert summary["workers"] == 1
    assert any("Connected Dask client" in message for message in logger.infos)


def test_dask_session_can_create_managed_local_cluster():
    pytest.importorskip("dask.distributed")

    with dask_session(cluster_kwargs={"n_workers": 1, "threads_per_worker": 1, "processes": False, "dashboard_address": ":0"}) as client:
        summary = describe_client(client)

    assert summary["workers"] == 1


def test_inspect_graph_reports_dask_metrics_and_logs():
    logger = CaptureLogger()
    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    metrics = inspect_graph(frame, logger=logger)

    assert metrics["is_dask"] is True
    assert metrics["npartitions"] == 2
    assert metrics["task_count"] is not None
    assert any("Dask graph inspection metrics=" in message for message in logger.infos)


def test_safe_compute_uses_local_threads_for_non_dataframe_objects():
    value = dask.delayed(lambda: 40 + 2)()

    computed = safe_compute(value)

    assert computed == 42


def test_safe_compute_rejects_local_fallback_for_dask_dataframes():
    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    with pytest.raises(RuntimeError, match="Local fallback forbidden"):
        safe_compute(frame)


def test_safe_compute_dry_run_skips_execution_and_logs():
    logger = CaptureLogger()
    delayed_value = dask.delayed(lambda: (_ for _ in ()).throw(AssertionError("should not execute")))()

    result = safe_compute(delayed_value, logger=logger, dry_run=True)

    assert result is None
    assert any("dry run requested" in message for message in logger.infos)


def test_safe_compute_and_wait_work_with_distributed_client():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    delayed_value = dask.delayed(lambda: 21 * 2)()

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        with dask_session(scheduler_address=cluster.scheduler_address) as client:
            persisted = safe_persist(delayed_value, dask_client=client)
            waited = safe_wait(persisted, dask_client=client, timeout=5)
            computed = safe_compute(delayed_value, dask_client=client)

    assert waited is persisted
    assert computed == 42


def test_safe_head_returns_dataframe_preview_with_distributed_client():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3], "status": ["a", "b", "c"]}), npartitions=2)

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        with dask_session(scheduler_address=cluster.scheduler_address) as client:
            preview = safe_head(frame, n=2, dask_client=client)

    assert preview["id"].tolist() == [1, 2]


def test_safe_head_translates_orphaned_graph_errors():
    pytest.importorskip("dask.distributed")

    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    with pytest.warns(RuntimeWarning):
        with dask_session(
            cluster_kwargs={
                "n_workers": 1,
                "threads_per_worker": 1,
                "processes": False,
                "dashboard_address": ":0",
            }
        ) as client:
            persisted = safe_persist(frame, dask_client=client)
            safe_wait(persisted, dask_client=client, timeout=5)

    with pytest.raises(RuntimeError, match="orphaned from its original client/worker state"):
        safe_head(persisted)


def test_safe_gather_computes_local_delayed_structures():
    delayed_values = [dask.delayed(lambda value=value: value * 2)() for value in (2, 3)]

    gathered = safe_gather(delayed_values)

    assert gathered == [4, 6]


def test_managed_session_warns_before_persisted_dataframe_becomes_invalid():
    pytest.importorskip("dask.distributed")

    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    with pytest.warns(RuntimeWarning) as recorded:
        with dask_session(
            cluster_kwargs={
                "n_workers": 1,
                "threads_per_worker": 1,
                "processes": False,
                "dashboard_address": ":0",
            }
        ) as client:
            persisted = safe_persist(frame, dask_client=client)
            safe_wait(persisted, dask_client=client, timeout=5)

    assert "Closing managed Dask session with 1 live persisted collection" in str(recorded[0].message)
    assert "head()/compute() calls may fail with Missing dependency" in str(recorded[0].message)

    with pytest.raises(ValueError, match="Missing dependency"):
        persisted.head()


@pytest.mark.asyncio
async def test_async_safe_compute_and_wait_work_with_sync_client():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    delayed_value = dask.delayed(lambda: 21 * 2)()

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        with dask_session(scheduler_address=cluster.scheduler_address) as client:
            persisted = await async_safe_persist(delayed_value, dask_client=client)
            waited = await async_safe_wait(persisted, dask_client=client, timeout=5)
            computed = await async_safe_compute(delayed_value, dask_client=client)

    assert waited is persisted
    assert computed == 42


@pytest.mark.asyncio
async def test_async_safe_head_and_gather_work_with_distributed_client():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster

    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)
    delayed_values = [dask.delayed(lambda value=value: value + 1)() for value in (1, 2)]

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        with dask_session(scheduler_address=cluster.scheduler_address) as client:
            preview = await async_safe_head(frame, n=2, dask_client=client)
            gathered = await async_safe_gather(delayed_values, dask_client=client)

    assert preview["id"].tolist() == [1, 2]
    assert gathered == [2, 3]


@pytest.mark.asyncio
async def test_async_safe_compute_rejects_local_fallback_for_dask_dataframes():
    frame = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    with pytest.raises(RuntimeError, match="Local fallback forbidden"):
        await async_safe_compute(frame)


def test_dask_empty_helpers_cover_empty_and_non_empty_frames():
    empty = dd.from_pandas(pd.DataFrame({"id": pd.Series([], dtype="Int64")}), npartitions=1)
    non_empty = dd.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    assert dask_is_probably_empty(empty) is False
    assert dask_is_empty(empty) is True
    assert dask_is_empty(non_empty) is False


@pytest.mark.asyncio
async def test_unique_values_extractor_returns_values_and_handles_missing_columns():
    logger = CaptureLogger()
    frame = dd.from_pandas(
        pd.DataFrame({"status": ["active", "inactive", "active"], "id": [1, 2, 3]}),
        npartitions=2,
    )

    result = await UniqueValuesExtractor(logger=logger).extract_unique_values(
        frame,
        "status",
        "missing",
        limit=10,
    )

    assert set(result["status"]) == {"active", "inactive"}
    assert result["missing"] == []
    assert any("Column 'missing' not found" in message for message in logger.warnings)


def test_apply_recommended_dask_config_sets_shuffle_tasks():
    with apply_recommended_dask_config():
        assert dask.config.get("dataframe.shuffle.method") == "tasks"


def test_dask_session_can_verify_connectivity_and_reuse_shared_client():
    distributed = pytest.importorskip("dask.distributed")
    LocalCluster = distributed.LocalCluster
    logger = CaptureLogger()

    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=":0",
    ) as cluster:
        with dask_session(
            scheduler_address=cluster.scheduler_address,
            verify_connectivity=True,
            shared=True,
            shared_key="test-shared-session",
            logger=logger,
        ) as client_one:
            with dask_session(
                scheduler_address=cluster.scheduler_address,
                verify_connectivity=True,
                shared=True,
                shared_key="test-shared-session",
                logger=logger,
            ) as client_two:
                assert client_one is client_two
                assert describe_client(client_two)["workers"] == 1

            assert describe_client(client_one)["workers"] == 1

    assert any("Verified Dask client connectivity" in message for message in logger.infos)
    assert any("Reusing shared Dask session" in message for message in logger.infos)
