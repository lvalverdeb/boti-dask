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

From the environment: `{PREFIX}UPLOAD` as a comma-separated list.

A path entry must exist; a missing one is an error rather than a package name, so a
mistyped path never becomes a pip install on every worker. Requirement entries must be
plain PEP 508 strings (`rich>=13`, `pkg[extra]; python_version >= "3.11"`): option
lines, `${VAR}` expansion and direct-URL requirements are refused.

| Payload                                | How it ships                                                                                 |
|----------------------------------------|----------------------------------------------------------------------------------------------|
| `.py`, `.zip`, `.egg`, `.pyz` file     | `Client.upload_file`; workers import or add it to `sys.path`.                                |
| Directory                              | Zipped deterministically (sorted entries, fixed timestamps, caches skipped) as `<name>.zip`, then `upload_file`. The directory name becomes the importable package. |
| Pure-Python `.whl`                     | Copied under a `.zip` name so workers add it to `sys.path`. Wheels with compiled extensions need a requirement string instead. |
| Anything else (a requirement string)   | One `PipInstall` worker plugin per sync; pip runs on each worker.                            |

## Deduplication

Each payload gets a SHA-256 digest (of the file bytes, the deterministic zip, or the requirement
text). Digests of everything sent are stored in the scheduler dataset `boti-dask:uploads`, so a
second session against the same scheduler only sends what changed. A fresh cluster has no
dataset and receives everything.

`sync_uploads(client, items, logger=None)` does the same for an existing client and returns
the payloads it sent.
