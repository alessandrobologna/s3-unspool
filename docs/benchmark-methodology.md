# Benchmark Methodology

Reproducibility notes for the `s3-unspool` Lambda extraction benchmark. Current
measurements live in [Benchmark Results](benchmark-results.md), and extraction
internals are described in [Architecture](architecture.md).

Fixtures were generated on 2026-04-26 with deterministic seeds, a 40% compressible,
40% incompressible, 20% mixed file-class split, and 5% mutated update variants.
The target sizes below are uncompressed payload sizes. ZIP sizes are measured from
the streamed `s3-unspool upload` reports and verified with `aws s3 ls`.

## Fixture Objects

All fixture ZIPs are in:

```text
s3://<benchmark-bucket>/benchmarks/fixtures/2026-04-26/
```

| Fixture | Variant | Files | Uncompressed | ZIP bytes | ZIP size | Changed files | Changed bytes | S3 object |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| small | base | 512 | 10 MiB | 4,412,119 | 4.21 MiB | 0 | 0 B | `small-base.zip` |
| small | mutated-5pct | 512 | 10 MiB | 4,412,593 | 4.21 MiB | 26 | 541,919 B | `small-mutated-5pct.zip` |
| medium | base | 8,192 | 500 MiB | 244,990,127 | 233.65 MiB | 0 | 0 B | `medium-base.zip` |
| medium | mutated-5pct | 8,192 | 500 MiB | 245,002,959 | 233.66 MiB | 410 | 27,377,892 B | `medium-mutated-5pct.zip` |
| large | base | 49,152 | 3 GiB | 1,465,304,598 | 1.37 GiB | 0 | 0 B | `large-base.zip` |
| large | mutated-5pct | 49,152 | 3 GiB | 1,465,383,405 | 1.37 GiB | 2,458 | 154,887,422 B | `large-mutated-5pct.zip` |

## Fixture Generation

Local fixture root:

```text
/tmp/s3-unspool-benchmarks/fixtures
```

Upload reports:

```text
/tmp/s3-unspool-benchmarks/reports
```

Generation commands:

```sh
scripts/generate-fixture.py /tmp/s3-unspool-benchmarks/fixtures/small/base \
  --files 512 \
  --total-size 10MiB \
  --seed 42601 \
  --compressible-ratio 0.4 \
  --incompressible-ratio 0.4 \
  --clean \
  --manifest /tmp/s3-unspool-benchmarks/fixtures/small/base.manifest.json

scripts/generate-fixture.py /tmp/s3-unspool-benchmarks/fixtures/medium/base \
  --files 8192 \
  --total-size 500MiB \
  --seed 42602 \
  --compressible-ratio 0.4 \
  --incompressible-ratio 0.4 \
  --clean \
  --manifest /tmp/s3-unspool-benchmarks/fixtures/medium/base.manifest.json

scripts/generate-fixture.py /tmp/s3-unspool-benchmarks/fixtures/large/base \
  --files 49152 \
  --total-size 3GiB \
  --seed 42603 \
  --compressible-ratio 0.4 \
  --incompressible-ratio 0.4 \
  --clean \
  --manifest /tmp/s3-unspool-benchmarks/fixtures/large/base.manifest.json
```

Mutation commands:

```sh
scripts/mutate-fixture.py \
  /tmp/s3-unspool-benchmarks/fixtures/small/base \
  /tmp/s3-unspool-benchmarks/fixtures/small/mutated-5pct \
  --manifest /tmp/s3-unspool-benchmarks/fixtures/small/base.manifest.json \
  --output-manifest /tmp/s3-unspool-benchmarks/fixtures/small/mutated-5pct.manifest.json \
  --change-ratio 0.05 \
  --seed 52601 \
  --copy-mode hardlink \
  --clean

scripts/mutate-fixture.py \
  /tmp/s3-unspool-benchmarks/fixtures/medium/base \
  /tmp/s3-unspool-benchmarks/fixtures/medium/mutated-5pct \
  --manifest /tmp/s3-unspool-benchmarks/fixtures/medium/base.manifest.json \
  --output-manifest /tmp/s3-unspool-benchmarks/fixtures/medium/mutated-5pct.manifest.json \
  --change-ratio 0.05 \
  --seed 52602 \
  --copy-mode hardlink \
  --clean

scripts/mutate-fixture.py \
  /tmp/s3-unspool-benchmarks/fixtures/large/base \
  /tmp/s3-unspool-benchmarks/fixtures/large/mutated-5pct \
  --manifest /tmp/s3-unspool-benchmarks/fixtures/large/base.manifest.json \
  --output-manifest /tmp/s3-unspool-benchmarks/fixtures/large/mutated-5pct.manifest.json \
  --change-ratio 0.05 \
  --seed 52603 \
  --copy-mode hardlink \
  --clean
```

Upload commands:

```sh
BENCHMARK_BUCKET="your-benchmark-bucket"
BUCKET="$BENCHMARK_BUCKET"
PREFIX=benchmarks/fixtures/2026-04-26

./target/debug/s3-unspool --quiet --color never upload \
  --report=/tmp/s3-unspool-benchmarks/reports/small-base-upload.json \
  /tmp/s3-unspool-benchmarks/fixtures/small/base \
  "s3://$BUCKET/$PREFIX/small-base.zip"

./target/debug/s3-unspool --quiet --color never upload \
  --report=/tmp/s3-unspool-benchmarks/reports/small-mutated-5pct-upload.json \
  /tmp/s3-unspool-benchmarks/fixtures/small/mutated-5pct \
  "s3://$BUCKET/$PREFIX/small-mutated-5pct.zip"

./target/debug/s3-unspool --quiet --color never upload \
  --report=/tmp/s3-unspool-benchmarks/reports/medium-base-upload.json \
  /tmp/s3-unspool-benchmarks/fixtures/medium/base \
  "s3://$BUCKET/$PREFIX/medium-base.zip"

./target/debug/s3-unspool --quiet --color never upload \
  --report=/tmp/s3-unspool-benchmarks/reports/medium-mutated-5pct-upload.json \
  /tmp/s3-unspool-benchmarks/fixtures/medium/mutated-5pct \
  "s3://$BUCKET/$PREFIX/medium-mutated-5pct.zip"

./target/debug/s3-unspool --quiet --color never upload \
  --report=/tmp/s3-unspool-benchmarks/reports/large-base-upload.json \
  /tmp/s3-unspool-benchmarks/fixtures/large/base \
  "s3://$BUCKET/$PREFIX/large-base.zip"

./target/debug/s3-unspool --quiet --color never upload \
  --report=/tmp/s3-unspool-benchmarks/reports/large-mutated-5pct-upload.json \
  /tmp/s3-unspool-benchmarks/fixtures/large/mutated-5pct \
  "s3://$BUCKET/$PREFIX/large-mutated-5pct.zip"
```

## Lambda Test Plan

Benchmark memory sizes:

| Memory | Expected default workers | Notes |
| ---: | ---: | --- |
| 256 MB | 6 | Low-memory Lambda profile with square-root worker scaling. |
| 1024 MB | 11 | Mid-memory profile with more workers and cache. |
| 2048 MB | 16 | High-memory Lambda profile with the default worker cap. |

For each memory size and fixture size:

1. Set Lambda memory and wait for the update to complete.
2. Empty a dedicated destination prefix for that memory and fixture.
3. Invoke the base ZIP once to measure a cold/full extract into an empty prefix.
4. Invoke the mutated 5% ZIP against the same destination prefix with the catalog enabled.
5. Restore the same base destination state, then invoke the mutated 5% ZIP with
   `"ignoreCatalog": true` to measure fallback extract-and-hash behavior.
6. Record Lambda CloudWatch `REPORT` duration and max memory, plus response diagnostics.

Timing source: Lambda duration and max memory should come from the Lambda
CloudWatch Logs `REPORT` line, not local AWS CLI wall time.

## Results

Measured results are kept in [Benchmark Results](benchmark-results.md). This
document is only the reproducibility recipe: fixture construction, upload
commands, Lambda setup, and the benchmark harness invocation.

## Run Commands

The preferred harness is `scripts/benchmark.py`. It uses PEP 723 metadata, so it
can be run directly with `uv`:

```sh
BENCHMARK_BUCKET="your-benchmark-bucket"

uv run scripts/benchmark.py \
  --stack-name s3-unspool \
  --bucket "$BENCHMARK_BUCKET" \
  --samples 5 \
  --max-workers 1
```

The published results use five measured samples per configuration, but the
harness now runs live samples serially by default. S3 charges Tier-1 request
fees per PUT/POST/LIST request, so parallel samples multiply cost without
changing the per-sample algorithm after throttling has been ruled out. To run
parallel samples intentionally, pass both `--max-workers N` and
`--allow-parallel-s3-costs`.

The harness:

- updates Lambda memory sequentially for `256`, `1024`, and `2048` MB;
- runs fixture/configuration samples serially by default within each memory
  size;
- gives every measured sample its own destination prefix;
- seeds update samples with the base ZIP before measuring the mutated ZIP;
- captures Lambda `REPORT` duration and max memory from invoke log tails;
- writes per-sample JSON, aggregate JSON, Markdown tables, and SVG bar charts
  under `benchmark-results/<run-id>/`;
- updates [Benchmark Results](benchmark-results.md) unless `--no-results-md`
  is passed.

Manual commands are still useful for one-off debugging.

Get deployed function name:

```sh
STACK=s3-unspool
FUNCTION=$(aws cloudformation describe-stacks \
  --stack-name "$STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`FunctionName`].OutputValue' \
  --output text)
```

Set memory:

```sh
aws lambda update-function-configuration \
  --function-name "$FUNCTION" \
  --memory-size 256

aws lambda wait function-updated --function-name "$FUNCTION"
```

The benchmark Lambda template scopes access to fixture reads under
`benchmarks/fixtures/` and destination reads/writes/deletes under
`benchmarks/extract/` by default. Destination `s3:GetObject` is required for
changed-file `PutObject` calls that use `If-Match`; without it, S3 rejects the
conditional overwrite with `AccessDenied`. The benchmark harness also refuses to
clean up prefixes outside `benchmarks/extract/`, and each sample deletes only
its own run-specific destination prefix.

Invoke one run:

```sh
BENCHMARK_BUCKET="your-benchmark-bucket"
BUCKET="$BENCHMARK_BUCKET"
FIXTURE_PREFIX=benchmarks/fixtures/2026-04-26
DESTINATION_PREFIX=benchmarks/extract/2026-04-26
MEMORY=256
FIXTURE=small
SOURCE_ZIP=small-base.zip
RUN=full
IGNORE_CATALOG=false
CATALOG=enabled

aws s3 rm \
  "s3://$BUCKET/$DESTINATION_PREFIX/$MEMORY/$FIXTURE/" \
  --recursive

aws lambda invoke \
  --cli-binary-format raw-in-base64-out \
  --function-name "$FUNCTION" \
  --payload "{\"source\":\"s3://$BUCKET/$FIXTURE_PREFIX/$SOURCE_ZIP\",\"destinationPrefix\":\"s3://$BUCKET/$DESTINATION_PREFIX/$MEMORY/$FIXTURE/\",\"diagnostics\":true,\"ignoreCatalog\":$IGNORE_CATALOG}" \
  "/tmp/s3-unspool-benchmarks/reports/lambda-$MEMORY-$FIXTURE-$RUN-$CATALOG.json"
```

For 5% update runs, keep the destination prefix populated by the corresponding
base run and set `SOURCE_ZIP` to `<fixture>-mutated-5pct.zip`. Run once with
`IGNORE_CATALOG=false`, then restore the base destination state and run again
with `IGNORE_CATALOG=true`.
