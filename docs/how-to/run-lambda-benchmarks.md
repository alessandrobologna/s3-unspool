# Run Lambda Benchmarks

Use the Lambda benchmark harness when you want to measure `s3-unspool` inside
AWS Lambda with controlled fixture data.

## Before You Begin

You need:

- AWS credentials for the account and region you will benchmark in
- AWS SAM CLI
- Rust, Cargo, and the Rust toolchain used by this repository
- Cargo Lambda support available to the SAM beta build
- `uv` for the Python benchmark runner and fixture tools
- permission to create and use the benchmark stack's S3 bucket

Benchmark runs can create Lambda invocations, S3 objects, CloudWatch logs, and
SAM deployment artifacts. Use an isolated stack or prefix, and clean up test
objects when you are done.

## Build and Deploy the Harness

Validate and build from the repository root:

```sh
sam validate --lint --template-file tools/lambda-benchmark/template.yaml
PATH="$HOME/.cargo/bin:$PATH" RUSTUP_TOOLCHAIN=1.95.0 \
  sam build --beta-features --template-file tools/lambda-benchmark/template.yaml
```

Deploy the built template:

```sh
sam deploy --guided --template-file .aws-sam/build/template.yaml
```

Deploy or refresh the stack before each benchmark session so the function
configuration, memory settings, IAM policy, and test bucket match the current
checkout.

## Find the Bucket

```sh
STACK=s3-unspool

BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`TestBucketName`].OutputValue' \
  --output text)
```

## Run the Benchmark Matrix

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

Generated benchmark artifacts default to `benchmark-results/<run-id>` under the
current working directory.

Timing in the benchmark snapshot docs comes from Lambda CloudWatch `REPORT`
duration lines. It does not include local CLI wall time, fixture upload time, or
CloudFormation deployment time.

## Smoke Check the Runner

For a smoke check that builds the command line without invoking Lambda:

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

## See Also

- [Lambda Benchmark Harness](../reference/lambda-benchmark-harness.md)
- [Benchmark Snapshots](../reference/benchmark-snapshots.md)
- [Generate Benchmark Fixtures](generate-fixtures.md)
