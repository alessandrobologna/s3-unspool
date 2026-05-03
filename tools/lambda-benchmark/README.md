# s3-unspool Lambda Benchmark

This folder contains the optional Lambda benchmark harness for `s3-unspool`.
It is repository tooling, not a published package.

Use this guide when you need to deploy the harness, invoke it directly, or run
the automated benchmark matrix that feeds the checked-in benchmark snapshots.
For the published results, see [Benchmark Snapshots](../../docs/benchmark.md).

The harness includes:

- a SAM template that deploys one direct-invoke Lambda function
- a `s3-unspool-lambda` Rust crate built with Cargo Lambda
- an example invoke event
- a local `uv` package for automated benchmark runs and chart generation

## Build and Deploy

Validate and build the SAM app from the repository root:

```sh
sam validate --lint --template-file tools/lambda-benchmark/template.yaml
PATH="$HOME/.cargo/bin:$PATH" RUSTUP_TOOLCHAIN=1.95.0 \
  sam build --beta-features --template-file tools/lambda-benchmark/template.yaml
```

Deploy the built template:

```sh
sam deploy --guided --template-file .aws-sam/build/template.yaml
```

The SAM template deploys:

- one direct-invoke Lambda function built with Cargo Lambda
- one test S3 bucket with a one-day object lifecycle rule for benchmark cleanup
- a Lambda role that can list, read, write, and optionally delete objects in
  that test bucket
- optional benchmark-bucket access scoped to `BenchmarkFixturePrefix` for
  fixture reads and `BenchmarkDestinationPrefix` for benchmark reads, writes,
  and optional deletes

## Direct Invoke

Use direct invokes for smoke tests and one-off payload checks.

Find the generated bucket and function:

```sh
STACK=s3-unspool

BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`TestBucketName`].OutputValue' \
  --output text)

FUNCTION=$(aws cloudformation describe-stacks \
  --stack-name "$STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`FunctionName`].OutputValue' \
  --output text)
```

Upload a ZIP and invoke the Lambda:

```sh
aws s3 cp site.zip "s3://$BUCKET/source/site.zip"

aws lambda invoke \
  --cli-binary-format raw-in-base64-out \
  --function-name "$FUNCTION" \
  --payload "{\"source\":\"s3://$BUCKET/source/site.zip\",\"destinationPrefix\":\"s3://$BUCKET/www/\",\"diagnostics\":true}" \
  /tmp/s3-unspool-response.json
```

Payload fields:

```json
{
  "source": "s3://bucket/source/site.zip",
  "destinationPrefix": "s3://bucket/www/",
  "deleteExtra": false,
  "diagnostics": false,
  "ignoreCatalog": false,
  "includeOperations": false,
  "includePatterns": [],
  "excludePatterns": []
}
```

When invoking against the benchmark bucket, keep the source under the configured
fixture prefix and the destination under the configured destination prefix. The
template scopes benchmark-bucket object permissions to those prefixes, including
destination `s3:GetObject` for conditional overwrites.

`concurrency` is optional. When it is omitted, the Lambda picks a default from
the configured memory size: `4` workers at `128` MB, `6` at `256` MB, `8` at
`512` MB, `11` at `1024` MB, and `16` at `2048` MB and above.

Set `"ignoreCatalog": true` to force extraction to ignore the embedded MD5
catalog and measure the fallback extract-and-hash path. The payload also accepts
`"ignoreEmbeddedCatalog": true`.

Set `"includePatterns"` or `"excludePatterns"` to restore only selected ZIP
entries from the archive. Selected Lambda extracts use the same source-range
planning as full extracts, and they reject `"deleteExtra": true` for the same
reason as the CLI: unselected destination objects are outside the restore scope.

Lambda responses omit per-object `operations` by default so large benchmark
invokes stay below the synchronous invoke response limit. Set
`"includeOperations": true` only when you need the full per-object report.

## Automated Benchmarks

The benchmark runner is a local `uv` package. It invokes the deployed Lambda
function, collects CloudWatch `REPORT` durations, and writes local benchmark
artifacts under `benchmark-results/<run-id>` by default.

```sh
uv run --project tools/lambda-benchmark s3-unspool-benchmark \
  --stack-name s3-unspool \
  --bucket "$BUCKET" \
  --fixture-prefix benchmarks/fixtures/2026-04-29 \
  --destination-prefix benchmarks/extract/2026-04-29 \
  --fixtures streaming \
  --runs full,update-catalog,update-ignore \
  --memories 128,256,512 \
  --samples 3
```

For a command-only smoke check that does not invoke Lambda:

```sh
uv run --project tools/lambda-benchmark s3-unspool-benchmark \
  --dry-run \
  --function-name dry-run-function \
  --bucket example-bucket \
  --fixtures streaming \
  --runs full \
  --memories 128 \
  --samples 1 \
  --no-results-md \
  --region us-east-1
```

## Test

```sh
cargo +1.95.0 test -p s3-unspool-lambda
uv run --project tools/lambda-benchmark python -m py_compile \
  tools/lambda-benchmark/src/s3_unspool_benchmark/run.py
```
