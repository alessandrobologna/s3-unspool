# s3-unspool Fixture Tools

This local Python package generates deterministic benchmark fixtures for
`s3-unspool`.

Canonical documentation now lives under `docs/`:

- [Generate Benchmark Fixtures](../../docs/how-to/generate-fixtures.md)
- [Fixture Generator](../../docs/reference/fixture-generator.md)
- [Run Lambda Benchmarks](../../docs/how-to/run-lambda-benchmarks.md)

Local validation command:

```sh
uv run --project tools/fixturegen python -m unittest discover -s tools/fixturegen/tests
```
