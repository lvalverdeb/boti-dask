# Session API

`boti_dask.session` provides explicit Dask client/session lifecycle helpers.

## Primary symbols

- `DaskSession`
- `dask_session(...)`
- `recommended_dask_config(...)`
- `apply_recommended_dask_config(...)`
- `describe_client(...)`
- `current_client_summary()`

## Typical usage

```python
from boti_dask import dask_session, describe_client

with dask_session(
    verify_connectivity=True,
    cluster_kwargs={"n_workers": 1, "threads_per_worker": 1, "processes": False, "dashboard_address": None},
) as client:
    print(describe_client(client))
```

## Notes

- `shared=True` + `shared_key=...` enables cross-context shared session reuse.
- Session close emits a runtime warning when live persisted collections are tracked.

