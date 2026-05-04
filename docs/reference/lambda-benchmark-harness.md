# Lambda Benchmark Harness

The optional Lambda benchmark harness lives under `tools/lambda-benchmark`. It
is repository tooling, not a published package.

The harness includes:

- a SAM template that deploys one direct-invoke Lambda function
- a `s3-unspool-lambda` Rust crate built with Cargo Lambda
- an example invoke event
- a local `uv` package for automated benchmark runs and chart generation

## Deployed Resources

The SAM template deploys:

- one direct-invoke Lambda function built with Cargo Lambda
- one test S3 bucket with a one-day object lifecycle rule for benchmark cleanup
- a Lambda role that can list, read, write, and optionally delete objects in
  that test bucket
- optional benchmark-bucket read access scoped to `BenchmarkFixturePrefix`
- optional benchmark-bucket read, write, and delete access scoped to
  `BenchmarkDestinationPrefix`

## Payload Fields

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
planning as full extracts, and they reject `"deleteExtra": true` because
unselected destination objects are outside the restore scope.

Lambda responses omit per-object `operations` by default so large benchmark
invokes stay below the synchronous invoke response limit. Set
`"includeOperations": true` only when you need the full per-object report.

## See Also

- [Run Lambda Benchmarks](../how-to/run-lambda-benchmarks.md)
- [Benchmark Snapshots](benchmark-snapshots.md)
