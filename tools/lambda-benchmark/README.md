# s3-unspool Lambda Benchmark

This folder contains the optional Lambda benchmark harness for `s3-unspool`.
It is repository tooling, not a published package.

Canonical documentation now lives under `docs/`:

- [Run Lambda Benchmarks](../../docs/how-to/run-lambda-benchmarks.md)
- [Lambda Benchmark Harness](../../docs/reference/lambda-benchmark-harness.md)
- [Benchmark Snapshots](../../docs/reference/benchmark-snapshots.md)

Local validation commands:

```sh
cargo +1.95.0 test -p s3-unspool-lambda
uv run --project tools/lambda-benchmark python -m py_compile \
  tools/lambda-benchmark/src/s3_unspool_benchmark/run.py
```
