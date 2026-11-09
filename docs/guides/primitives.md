# Primitives API

`boti_dask.primitives` gives dependents cluster-wide coordination without importing
`distributed` themselves.

## Symbols

- `cluster_semaphore(name, max_leases=1, *, client=None, namespace=None, strict=True)`
- `cluster_lock(name, *, client=None, namespace=None)`
- `cluster_queue(name, *, maxsize=0, client=None, namespace=None)`

## Behaviour

- With a running client (passed in, or discovered through `distributed.get_client()`, which
  also works inside a task on a worker) the `distributed` primitive of that name is returned,
  shared by name across the whole cluster.
- Without one, a `threading.BoundedSemaphore`, `threading.Lock` or `queue.Queue` of the same
  name is returned, shared by name across the process. Callers write one code path.
- A client passed in must be running; a closed one raises `RuntimeError` rather than quietly
  handing back a process-local primitive that gives no cluster-wide exclusion.
- A semaphore that already exists under the name with a different limit raises `ValueError`;
  `strict=False` adopts the existing limit and logs a warning.
- Names are prefixed `boti-dask:` on the scheduler so they never collide with user primitives.
  Pass `namespace=session.namespace` (the `shared_key`, else a digest unique to the session) and
  the prefix becomes `boti-dask:<namespace>:` so two sessions on one scheduler stay apart.
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
