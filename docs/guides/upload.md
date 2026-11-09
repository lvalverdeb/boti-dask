# Upload API

`boti_dask.upload` ships code to the workers once per scheduler. Give `DaskSession` (or
`dask_session`) an `upload` list and every payload is sent on open and again after a rebind;
payloads the scheduler has already seen are skipped.

```python
from boti_dask import dask_session

with dask_session(
    scheduler_address="tcp://scheduler:8786",
    upload=["./src/etl_jobs", "./dist/etl_jobs-1.4.0-py3-none-any.whl", "rich>=13"],
) as client:
    ...
```

From the environment: `{PREFIX}UPLOAD` as a comma-separated list
(`./src/etl_jobs,rich>=13`) or a JSON list of strings (`["pkg[a,b]>=1,<2", "./src/etl_jobs"]`).
In the comma-separated form a comma inside brackets or before a version operator stays
inside its entry, so `pkg[a,b]>=1,<2` is one requirement, not four.

A path entry must exist; a missing one is an error rather than a package name, so a
mistyped path never becomes a pip install on every worker. Requirement entries must be
plain PEP 508 strings (`rich>=13`, `pkg[extra]; python_version >= "3.11"`): option
lines, `${VAR}` expansion and direct-URL requirements are refused.

| Payload                                | How it ships                                                                                 |
|----------------------------------------|----------------------------------------------------------------------------------------------|
| `.py`, `.zip`, `.egg`, `.pyz` file     | `Client.upload_file`; workers import or add it to `sys.path`.                                |
| Directory                              | Zipped deterministically (sorted entries, fixed timestamps, caches skipped) as `<name>.zip`, then `upload_file`. The resolved directory name becomes the importable package, so `.` ships under the current directory's name; the filesystem root is refused. |
| Pure-Python `.whl`                     | Copied under a `.zip` name so workers add it to `sys.path`. Wheels with compiled extensions need a requirement string instead. |
| Anything else (a requirement string)   | One `PipInstall` worker plugin per sync; pip runs on each worker.                            |

## Deduplication

Each payload gets a SHA-256 digest (of the file bytes, the deterministic zip, or the requirement
text). Digests of everything sent are stored in the scheduler dataset `boti-dask:uploads`, so a
second session against the same scheduler only sends what changed. A fresh cluster has no
dataset and receives everything. Every client of the scheduler can read the dataset, so each
entry records only the payload kind, its base name and a short digest
(`directory:etl_jobs@3f1a…`), never a local directory; the session log still names the full
source.

`sync_uploads(client, items, logger=None)` does the same for an existing client and returns
the payloads it sent.
