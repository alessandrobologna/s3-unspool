# Fixture Generator

The local Python package under `tools/fixturegen` generates deterministic
benchmark fixtures for `s3-unspool`.

The generator creates repository-shaped directory trees with a mix of:

- Structured Markdown, code, config, and log files
- Binary-like assets such as images, fonts, archives, and precompressed blobs
- Mixed files such as bundles, source maps, wasm-like payloads, and packed data

Every generated fixture has a JSON manifest with file paths, content classes,
profiles, sizes, and SHA-256 digests.

The mutation tool copies an existing fixture and rewrites a deterministic subset
of files while preserving paths, sizes, classes, and profiles.

## Console Scripts

| Script | Purpose |
| --- | --- |
| `s3-unspool-generate-fixture` | Generate a deterministic base fixture directory. |
| `s3-unspool-mutate-fixture` | Copy and mutate a deterministic subset of files. |

## Local Install

```sh
uv tool install --editable tools/fixturegen
```

Then invoke the console scripts directly:

```sh
s3-unspool-generate-fixture ./tmp/fixture --files 1000 --total-size 512MiB --seed 42 --clean
s3-unspool-mutate-fixture ./tmp/fixture ./tmp/fixture-10pct --change-ratio 0.10 --seed 2 --clean
```

## Test

```sh
uv run --project tools/fixturegen python -m unittest discover -s tools/fixturegen/tests
```

## See Also

- [Generate Benchmark Fixtures](../how-to/generate-fixtures.md)
- [Run Lambda Benchmarks](../how-to/run-lambda-benchmarks.md)
