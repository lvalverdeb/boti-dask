# Benchmarks

`benchmarks/test_benchmarks.py` measures, with pytest-benchmark:

- `compute`: `safe_compute` against a bare `client.compute(...).result()` on a one-task graph.
  The difference is the wrapper's overhead (graph inspection, client resolution, the telemetry
  span). Target: under 5 % on the minimum time.
- `session`: acquiring and releasing a second holder of a shared session.
- `diagnostics`: `describe_frame` on a one-million-row pandas frame.

```bash
make bench            # run and compare against benchmarks/baseline/local
make bench-baseline   # overwrite the local baseline with this machine's numbers
```

Comparison fails when a minimum time regresses by more than 20 % (`--benchmark-compare-fail=min:20%`).
Minimum rather than mean: a scheduler round trip has a standard deviation larger than its mean,
so the mean is noise while the minimum is stable.

Baselines live per environment: `benchmarks/baseline/local/` for a developer machine and
`benchmarks/baseline/ci/` for GitHub's `ubuntu-latest` runners. The benchmark workflow compares
against `ci/` when it holds a baseline and otherwise only publishes its results as an artifact;
seed `ci/0001_baseline.json` from that artifact to turn the comparison on.
