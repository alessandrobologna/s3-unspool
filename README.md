# s3-unspool

`s3-unspool` is a Rust crate for fast, streaming extraction of large ZIP
archives from S3 into S3 prefixes.

It is built for deployment archives and low-scratch environments: the source ZIP
is read with ranged S3 `GetObject` requests, the destination prefix is listed
once, unchanged files can be skipped with an embedded MD5 catalog, and missing or
changed files are written with S3 conditional `PutObject` requests.

The crate also includes an upload helper that streams a local directory into a
ZIP object in S3 and embeds the catalog used by later incremental extracts.

For high-concurrency extraction, the destination S3 client should relax or
disable AWS SDK upload stalled-stream protection. `s3-unspool` can legitimately
pause a destination request body while waiting for planned source ranges. Keep
download stalled-stream protection enabled for source reads; the repository CLI
and Lambda example configure this split.

## Why Use It

- Streams extraction from S3 to S3 without downloading the ZIP or extracted files
  to local storage.
- Handles large archives with bounded memory and single-part destination writes.
- Optimizes wall time with concurrent entry extraction and a catalog-driven
  source scheduler that fetches only the ZIP byte ranges needed for the run.
- Skips unchanged files quickly when the ZIP was produced by `s3-unspool`.
- Avoids per-object `HeadObject` calls; destination ETags come from the initial
  `ListObjectsV2` pass.
- Protects changed destinations with conditional writes instead of overwriting
  objects whose ETag changed during the run.

## Install

```sh
cargo add s3-unspool
```

## Quick Start

Create an AWS S3 client with the normal AWS SDK configuration. The two core
library operations are extracting an existing S3 ZIP into an S3 prefix and
uploading a local directory as a cataloged ZIP for fast future extracts.

### Extract an S3 ZIP to a Destination Prefix

Use `sync_zip_to_s3` when the ZIP already exists in S3. The archive is read with
ranged `GetObject` requests, destination objects are listed once, and missing or
changed entries are streamed directly into the destination prefix.

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use s3_unspool::{S3Object, S3Prefix, SyncOptions, sync_zip_to_s3};

#[tokio::main]
async fn main() -> s3_unspool::Result<()> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let mut extract = SyncOptions::new(
        S3Object::parse("s3://my-bucket/releases/site.zip")?,
        S3Prefix::parse("s3://my-bucket/www/")?,
    );
    extract.delete_extra = true;
    extract.collect_diagnostics = true;

    let report = sync_zip_to_s3(&client, extract).await?;
    println!("changed files: {}", report.summary.uploaded_changed);

    Ok(())
}
```

`SyncOptions::ignore_embedded_catalog` is `false` by default. Set it to `true`
to benchmark or force the fallback extract-and-hash comparison path against the
same ZIP object.

### Upload a Directory as a Cataloged ZIP

Use `upload_directory_zip_to_s3` when you want `s3-unspool` to create the source
ZIP. Uploads stream the ZIP into S3 with multipart upload and include the
embedded catalog by default.

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use s3_unspool::{S3Object, UploadOptions, upload_directory_zip_to_s3};

#[tokio::main]
async fn main() -> s3_unspool::Result<()> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let upload = UploadOptions::new(
        "./site",
        S3Object::parse("s3://my-bucket/releases/site.zip")?,
    );
    let report = upload_directory_zip_to_s3(&client, upload).await?;

    println!(
        "uploaded {} files into {} bytes",
        report.files, report.zip_bytes
    );

    Ok(())
}
```

`UploadOptions::include_catalog` is `true` by default. Set it to `false` only
when you need a plain ZIP without the update-skip catalog.

## Extraction Model

The extract flow is:

1. Read the ZIP central directory, and embedded catalog metadata unless it is
   ignored, from S3 with ranged `GetObject` requests.
2. List the destination prefix once with `ListObjectsV2`.
3. Compare ZIP entries with destination keys and listed ETags.
4. Skip unchanged entries directly when catalog MD5s are available; otherwise
   stream existing entries through the ZIP decoder to hash them.
5. Upload missing or changed destination objects with conditional `PutObject`.
6. Optionally delete destination objects that are not present in the ZIP.

Destination checks do not use per-file `HeadObject` calls. The destination ETag
from `ListObjectsV2` is the comparison point.

Conditional writes protect against overwriting newer destination data:

- Missing keys are uploaded with `If-None-Match: *`.
- Changed keys are uploaded with `If-Match: <listed destination ETag>`.
- If the condition fails, the object is reported as a conflict and extraction
  continues.

Library callers that prefer all-or-nothing behavior can set
`SyncOptions::fail_on_conditional_conflict = true`. In that mode, the first
observed conditional conflict returns an error and the run stops before
deleting extra destination objects.

## Required S3 Permissions

Extraction needs these object-level permissions:

- Source ZIP object: `s3:GetObject`.
- Destination prefix: `s3:PutObject`.
- Destination prefix, for changed-object overwrites: `s3:GetObject`.
- Destination prefix, only when deleting extras: `s3:DeleteObject`.

The destination `s3:GetObject` permission is required even though
`s3-unspool` does not issue per-file destination `HeadObject` requests or read
destination object bodies. S3 authorizes `PutObject` requests with
`If-Match: <etag>` against object-read permission; without destination
`s3:GetObject`, changed files are rejected with `AccessDenied`.

## Fast Updates

ZIPs created by `s3-unspool` include an embedded catalog at:

```text
.s3-unspool/catalog.v1.json
```

The catalog records each file path and MD5 digest. During extraction,
`s3-unspool` can compare those digests directly with destination ETags and skip
unchanged entries before decompressing them.

External ZIP files are still supported. If the embedded catalog is missing,
`s3-unspool` falls back to streaming entries and hashing them while extracting.
Use `SyncOptions::ignore_embedded_catalog = true`, CLI `--ignore-catalog`, or
Lambda payload `"ignoreCatalog": true` to force that fallback path.

The embedded catalog file is reserved. It is never extracted to the destination,
and upload sources cannot contain a file at that path.

## Command-Line Testing

The repository includes a CLI for trying the same upload and extract flows from
a terminal. Build it from a checkout:

```sh
cargo build --release -p s3-unspool-cli --bin s3-unspool
```

During development, commands can be run through Cargo:

```sh
cargo run -p s3-unspool-cli -- \
  upload ./site s3://my-bucket/releases/site.zip

cargo run -p s3-unspool-cli -- \
  extract s3://my-bucket/releases/site.zip s3://my-bucket/www/
```

The built binary is `./target/release/s3-unspool`:

```sh
./target/release/s3-unspool extract \
  s3://my-bucket/releases/site.zip \
  s3://my-bucket/www/
```

Useful extract options:

- `--delete-extra`: delete destination objects under the prefix that are not in
  the ZIP.
- `--concurrency <N>`: maximum number of ZIP entries processed at once. The CLI
  default is `64`.
- `--report`: add a formatted operation report to the CLI transcript.
- `--report=PATH`: write the JSON operation report to a file.
- `--diagnostics`: add source scheduler, ranged `GetObject`, block cache, and
  destination `PutObject` retry counters to the JSON report.
- `--ignore-catalog`: ignore `.s3-unspool/catalog.v1.json` and compare existing
  destination objects by extracting and hashing each ZIP entry.

Global CLI options:

- `--quiet`: suppress human-readable status output.
- `--color auto|always|never`: control semantic color output.

## CLI Output And Reports

Interactive upload and extract commands show a single-line spinner with elapsed
time and progress where available:

```text
• Uploading 00:03 [█████▍            ] 30% 18 MiB/512 MiB file 42/1000
```

The spinner is written to stderr, clears itself before the final summary, and is
disabled by `--quiet` or non-interactive output.

Use bare `--report` to expand the final human-readable transcript:

```sh
s3-unspool extract \
  --diagnostics \
  --report \
  s3://my-bucket/releases/site.zip \
  s3://my-bucket/www/
```

Use `--report=PATH` when you want JSON for automation:

```sh
s3-unspool extract \
  --report=report.json \
  s3://my-bucket/releases/site.zip \
  s3://my-bucket/www/
```

Formatted upload reports contain the source directory, destination ZIP object,
file count, uncompressed bytes, uploaded ZIP bytes, wall time, and upload speed
in MiB/s.

Extract reports contain:

- `summary`: totals for uploaded, skipped, conflicted, deleted, and errored
  objects.
- `operations`: one record per relevant object.
- `diagnostics`: optional source scheduler, block cache, and failed/retried
  `PutObject` counters when diagnostics are enabled.

Example extract summary:

```json
{
  "zip_files": 1000,
  "destination_objects": 1000,
  "uploaded_new": 0,
  "uploaded_changed": 100,
  "skipped_unchanged": 900,
  "conditional_conflicts": 0,
  "deleted_extra": 0,
  "errors": 0
}
```

## Performance And Architecture

Extraction starts by reading the ZIP central directory and listing the
destination prefix. Entries that match the embedded MD5 catalog are skipped
before any source file data is fetched. The remaining entries are converted into
a source-ordered block plan, with nearby byte spans coalesced so workers can
share ranged `GetObject` responses.

The library defaults are conservative:

- `SyncOptions::concurrency`: `64`
- `SyncOptions::put_concurrency`: `16`
- `SyncOptions::source_block_size`: 8 MiB
- `SyncOptions::source_block_merge_gap`: 256 KiB
- `SyncOptions::source_get_concurrency`: `4`
- `SyncOptions::source_window_capacity`: 64 MiB
- `SyncOptions::put_retry_policy`: 6 attempts, 250 ms base retry delay, 5 s
  max retry delay, 1 s base `SlowDown` delay, 30 s max `SlowDown` delay, full
  jitter

The Lambda harness uses different defaults because Lambda memory often buys CPU.
It scales entry workers with a square-root curve (`round(4 *
sqrt(memory_mb / 128))`, clamped to `4..=16`) and reuses otherwise idle memory
for the source block window. The budget reserves a fixed 64 MiB baseline, 12 MiB
per worker, 2 KiB per ZIP file, and currently in-flight source blocks before
assigning remaining memory to the block window. When that computed window exceeds
512 MiB, the Lambda leaves an additional 384 MiB unused as measured RSS
headroom for allocator, catalog, SDK, and upload buffers, then caps the adaptive
window at 512 MiB. Lambda also caps concurrent destination PUTs at
`min(entry_workers, max(source_get_concurrency, 2), 8)` so S3 `SlowDown` backoff
can control write pressure without changing the invoke payload shape.

See [Architecture](docs/architecture.md) for the extraction flow, source
scheduler behavior, and diagnostics glossary.

Benchmark documentation is split into [methodology](docs/benchmark-methodology.md)
and [results](docs/benchmark-results.md). The methodology doc is the
reproducible recipe; the results doc is refreshed by `scripts/benchmark.py`.

## Benchmarking With Lambda

The included SAM template deploys:

- One direct-invoke Lambda function built with Cargo Lambda.
- One test S3 bucket.
- A Lambda role that can list, read, write, and delete objects in that test
  bucket.
- Optional benchmark-bucket access scoped to `BenchmarkFixturePrefix` for
  fixture reads and `BenchmarkDestinationPrefix` for benchmark reads, writes,
  and deletes.

Validate and build:

```sh
sam validate --lint
PATH="$HOME/.cargo/bin:$PATH" RUSTUP_TOOLCHAIN=1.95.0 sam build --beta-features
```

Deploy:

```sh
sam deploy --guided
```

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
  "includeOperations": false
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

Lambda responses omit per-object `operations` by default so large benchmark
invokes stay below the synchronous invoke response limit. Set
`"includeOperations": true` only when you need the full per-object report.

## Fixture Tools

Create deterministic local test data:

```sh
scripts/generate-fixture.py ./tmp/fixture \
  --files 1000 \
  --total-size 512MiB \
  --seed 42 \
  --clean
```

The generator creates nested directories with a mix of compressible,
incompressible, and mixed-content files. It writes a manifest next to the output
directory by default:

```text
./tmp/fixture.manifest.json
```

Create an update fixture with about 10 percent of files changed:

```sh
scripts/mutate-fixture.py ./tmp/fixture ./tmp/fixture-10pct \
  --change-ratio 0.10 \
  --seed 2 \
  --clean
```

Upload and extract the fixtures:

```sh
s3-unspool upload ./tmp/fixture s3://my-bucket/fixtures/fixture.zip
s3-unspool extract s3://my-bucket/fixtures/fixture.zip s3://my-bucket/fixture-out/

s3-unspool upload ./tmp/fixture-10pct s3://my-bucket/fixtures/fixture-10pct.zip
s3-unspool extract s3://my-bucket/fixtures/fixture-10pct.zip s3://my-bucket/fixture-out/
```

Use these scripts to compare full deploys, no-op deploys, and update deploys
with a known mix of file sizes and compressibility.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `crates/s3-unspool` | Published Rust library crate |
| `crates/s3-unspool-cli` | Repository CLI for testing and reports |
| `lambda/s3-unspool-lambda` | SAM/Cargo Lambda benchmark harness |
| `scripts/` | Fixture generation and benchmark helpers |
| `docs/` | Architecture and benchmark documentation |

The CLI and Lambda packages are repository tools. The published crate is
`s3-unspool`.

## Versioning

Pre-release versions, when available, use standard Cargo SemVer pre-release
identifiers such as `0.1.0-alpha.1`, `0.1.0-beta.1`, or `0.1.0-rc.1`.
Consumers opt into a pre-release explicitly:

```sh
cargo add s3-unspool@0.1.0-alpha.1
```

## Assumptions And Limits

- The crate is built for Rust 1.95 and edition 2024.
- ZIP extraction supports Stored and Deflate entries.
- Upload sources must be local directories.
- Upload includes regular files recursively.
- Symbolic links and other special files are rejected.
- Upload paths must be UTF-8 and cannot contain backslashes.
- ZIP entry paths must be relative UTF-8 paths with no absolute roots, `..`,
  empty components, Windows drive prefixes, or backslashes.
- Upload sources cannot contain `.s3-unspool/catalog.v1.json`.
- Source ZIP uploads use S3 multipart upload so the archive can be streamed once
  without precomputing its final compressed size.
- Destination objects are assumed to be written by this tool or by equivalent
  single-part `PutObject` writes.
- Destination ETags are assumed to be MD5 hashes of object content. SSE-C and
  multipart destination objects are out of scope for ETag comparison.
- Destination writes use single `PutObject` requests, not multipart upload.
  Objects larger than the S3 single-PUT limit are rejected or fail.
- IAM policies for conditional overwrites must allow `s3:GetObject` on
  destination objects as well as `s3:PutObject`.
- Source reads are pinned to the source object ETag observed at the start of the
  run. If the source ZIP changes mid-run, extraction fails or reports errors
  instead of mixing old and new source bytes.

## Live S3 Test

The live S3 test is skipped unless `S3_UNSPOOL_LIVE_BUCKET` is set:

```sh
S3_UNSPOOL_LIVE_BUCKET=your-test-bucket \
  cargo test -p s3-unspool --test live_s3 -- --nocapture
```

The test creates a temporary prefix, exercises upload, skip, overwrite, and
delete behavior, verifies destination object contents, and deletes the temporary
objects at the end of the run.

## License

Licensed under the MIT license. See [LICENSE](LICENSE).
