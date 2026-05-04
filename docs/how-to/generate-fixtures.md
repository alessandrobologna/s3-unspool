# Generate Benchmark Fixtures

Use the fixture generator when you need deterministic mixed-content directories
for local smoke tests or Lambda benchmarks.

## Generate a Base Fixture

```sh
uv run --project tools/fixturegen s3-unspool-generate-fixture ./tmp/fixture \
  --files 1000 \
  --total-size 512MiB \
  --seed 42 \
  --clean
```

By default, the manifest is written alongside the output directory:

```text
./tmp/fixture.manifest.json
```

## Generate an Update Fixture

```sh
uv run --project tools/fixturegen s3-unspool-mutate-fixture ./tmp/fixture ./tmp/fixture-10pct \
  --change-ratio 0.10 \
  --seed 2 \
  --clean
```

The mutation tool rewrites a deterministic subset of files while preserving
paths, sizes, content classes, and profiles.

## Zip and Extract Fixtures

```sh
s3-unspool zip ./tmp/fixture s3://my-bucket/fixtures/fixture.zip
s3-unspool unzip s3://my-bucket/fixtures/fixture.zip s3://my-bucket/fixture-out/

s3-unspool zip ./tmp/fixture-10pct s3://my-bucket/fixtures/fixture-10pct.zip
s3-unspool unzip s3://my-bucket/fixtures/fixture-10pct.zip s3://my-bucket/fixture-out/
```

## See Also

- [Fixture Generator](../reference/fixture-generator.md)
- [Run Lambda Benchmarks](run-lambda-benchmarks.md)
