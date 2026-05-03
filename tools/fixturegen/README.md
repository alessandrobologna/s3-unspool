# s3-unspool Fixture Tools

This local Python package generates deterministic benchmark fixtures for
`s3-unspool`.

The generator creates repository-shaped directory trees with a mix of:

- structured Markdown, code, config, and log files
- binary-like assets such as images, fonts, archives, and precompressed blobs
- mixed files such as bundles, source maps, wasm-like payloads, and packed data

Every generated fixture has a JSON manifest with file paths, content classes,
profiles, sizes, and SHA-256 digests. The mutation tool copies an existing
fixture and rewrites a deterministic subset of files while preserving paths,
sizes, classes, and profiles.

## Run With uv

Generate a base fixture:

```sh
uv run --project tools/fixturegen s3-unspool-generate-fixture ./tmp/fixture \
  --files 1000 \
  --total-size 512MiB \
  --seed 42 \
  --clean
```

The manifest is written next to the output directory by default:

```text
./tmp/fixture.manifest.json
```

Generate an update fixture with about 10 percent of files changed:

```sh
uv run --project tools/fixturegen s3-unspool-mutate-fixture ./tmp/fixture ./tmp/fixture-10pct \
  --change-ratio 0.10 \
  --seed 2 \
  --clean
```

Zip and unzip the fixtures with the Rust CLI:

```sh
s3-unspool zip ./tmp/fixture s3://my-bucket/fixtures/fixture.zip
s3-unspool unzip s3://my-bucket/fixtures/fixture.zip s3://my-bucket/fixture-out/

s3-unspool zip ./tmp/fixture-10pct s3://my-bucket/fixtures/fixture-10pct.zip
s3-unspool unzip s3://my-bucket/fixtures/fixture-10pct.zip s3://my-bucket/fixture-out/
```

## Install Locally

For repeated use, install the tool from this checkout:

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
