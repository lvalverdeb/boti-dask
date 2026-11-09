# Clusters API

`boti_dask.clusters` picks a cluster backend by name so deployment choice lives in
configuration rather than in code.

| Kind         | Cluster class                        | Install extra        |
|--------------|--------------------------------------|----------------------|
| `local`      | `distributed.LocalCluster`           | none                 |
| `kubernetes` | `dask_kubernetes.operator.KubeCluster` | `boti-dask[k8s]`   |
| `slurm`      | `dask_jobqueue.SLURMCluster`         | `boti-dask[slurm]`   |
| `coiled`     | `coiled.Cluster`                     | `boti-dask[coiled]`  |

## Symbols

- `CLUSTER_KINDS`: the tuple above.
- `cluster_factory(kind)`: the cluster class, imported on first use. A missing package raises
  `ModuleNotFoundError` naming the extra to install; an unknown kind raises `ValueError`.
- `make_cluster(kind, **cluster_kwargs)`: builds the cluster.

## From the environment

`DaskSessionSettings.cluster_kind` (env `{PREFIX}CLUSTER_KIND`) selects the backend; the
existing `{PREFIX}CLUSTER_KWARGS` JSON is passed to it. A `{PREFIX}SCHEDULER_ADDRESS` takes
precedence, since connecting to a scheduler builds no cluster.

Only sizing, placement and naming keys are accepted from the environment
(`boti_dask.session_settings.ENV_CLUSTER_KWARGS`: `n_workers`, `threads_per_worker`,
`memory_limit`, `cores`, `memory`, `queue`, `walltime`, `name`, `namespace`, `image` and
the like). Keys that load code at start-up, such as `preload`, `worker_class`, `security`
or a job-script prologue, are refused with an error: pass them as
`DaskSession(cluster_kwargs=...)` in code, where they are reviewed with the rest of it.

```bash
export ETL_DASK_CLUSTER_KIND=slurm
export ETL_DASK_CLUSTER_KWARGS='{"queue": "batch", "cores": 8, "memory": "32GB"}'
```

```python
from boti_dask import dask_session_from_env_prefix

with dask_session_from_env_prefix("ETL_DASK_") as client:
    ...
```

In code, `DaskSession(cluster_factory=cluster_factory("kubernetes"), cluster_kwargs={...})`
does the same. Each backend's own keyword arguments are documented by that project; boti-dask
passes them through unchanged, except that `local` defaults `dashboard_address` to `":0"`.
