# Primitives API

`boti_dask.primitives` gives dependents cluster-wide coordination without importing
`distributed` themselves.

## Symbols

- `cluster_semaphore(name, max_leases=1, *, client=None)`
- `cluster_lock(name, *, client=None)`
- `cluster_queue(name, *, maxsize=0, client=None)`

## Behaviour

- With a running client (passed in, or discovered through `distributed.get_client()`, which
  also works inside a task on a worker) the `distributed` primitive of that name is returned,
  shared by name across the whole cluster.
- Without one, a `threading.BoundedSemaphore`, `threading.Lock` or `queue.Queue` of the same
  name is returned, shared by name across the process. Callers write one code path.
- Names are prefixed `boti-dask:` on the scheduler so they never collide with user primitives.
- Primitives obtained through a client are tracked by the session pool and closed when that
  client is closed by `DaskSession`.

## Typical usage

Bound the number of database connections a distributed load may open, cluster-wide:

```python
from boti_dask import cluster_semaphore

def fetch_partition(spec):
    with cluster_semaphore("db:orders", max_leases=4):   # at most 4 across all workers
        return run_query(spec)
```

The same call made outside a cluster bounds the process instead, so local runs and tests
need no special casing.
