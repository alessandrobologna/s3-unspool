# s3-unspool

`s3-unspool` is a Rust crate for fast, streaming extraction of large ZIP
archives from S3 into S3 prefixes.

It is designed for large archives and low scratch-space environments: the source
ZIP is read with ranged S3 `GetObject` calls, and extracted files can be
streamed directly into S3 `PutObject` requests or written to a local directory.
The crate also includes zip helpers that stream a local directory or existing S3
prefix into a local or S3 ZIP and embed the catalog used by later incremental
extracts.

## Install

```sh
cargo add s3-unspool
```

## Examples

### Extract an S3 ZIP to a Destination Prefix

Use `sync_zip_to_s3` when the ZIP already exists in S3. The source archive is
read with ranged S3 requests, and missing or changed entries are streamed
directly into the destination prefix.

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

    let report = sync_zip_to_s3(&client, extract).await?;
    println!("changed files: {}", report.summary.uploaded_changed);

    Ok(())
}
```

### Upload a Directory as a Cataloged ZIP

Use `upload_directory_zip_to_s3` when you want the crate to produce the source
ZIP and embed the catalog used by fast future extracts. Empty local directories
are written as ZIP directory entries.

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

    println!("uploaded {} files", report.files);

    Ok(())
}
```

### Upload an S3 Prefix as a Cataloged ZIP

Use `zip_s3_prefix_to_s3` when the source files already live in S3 and should be
snapshotted into a ZIP object without local object storage. Use
`zip_s3_prefix_to_file` when the ZIP destination should be a local file instead.

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use s3_unspool::{S3Object, S3Prefix, S3PrefixUploadOptions, zip_s3_prefix_to_s3};

#[tokio::main]
async fn main() -> s3_unspool::Result<()> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let upload = S3PrefixUploadOptions::new(
        S3Prefix::parse("s3://my-bucket/www/")?,
        S3Object::parse("s3://my-bucket/releases/site.zip")?,
    );
    let report = zip_s3_prefix_to_s3(&client, upload).await?;

    println!("uploaded {} files", report.files);

    Ok(())
}
```

## Behavior

- Reads source ZIP data with ranged S3 `GetObject` requests.
- Lists the destination prefix once with `ListObjectsV2`.
- Uses listed destination ETags instead of per-object `HeadObject` calls.
- Uploads missing files with `If-None-Match: *`.
- Uploads changed files with `If-Match: <listed destination ETag>`.
- Optionally deletes destination objects that are not present in the ZIP.
- Supports Stored and Deflate ZIP entries.
- Uploads generated source ZIPs with S3 multipart upload.
- Uploads existing S3 prefixes into generated ZIPs without local object storage.
- Preserves ZIP directory entries and zero-byte S3 folder marker objects.
- Emits optional upload progress events through upload option progress handlers.
- Can ignore the embedded catalog with `SyncOptions::ignore_embedded_catalog`
  when you need to force the fallback extract-and-hash comparison path.
- Can fail fast on destination write races with
  `SyncOptions::fail_on_conditional_conflict`.
- Keeps source ZIP blocks in a bounded memory window and reuses cached blocks
  across destination `PutObject` retries when possible.
- Exposes `SyncOptions::put_concurrency` and
  `SyncOptions::put_retry_policy` for destination write backoff, including shared
  throttling for S3 `SlowDown`.

## Required S3 Permissions

Extraction needs:

| Scope | Permission | Why |
| --- | --- | --- |
| Source ZIP object | `s3:GetObject` | Read ZIP metadata and ranged source bytes. |
| Destination bucket | `s3:ListBucket` | List destination keys and ETags once. |
| Destination prefix | `s3:PutObject` | Write missing and changed objects. |
| Destination prefix | `s3:GetObject` | Authorize conditional overwrites with `If-Match`. |
| Destination prefix | `s3:DeleteObject` | Only needed when `delete_extra` is enabled. |

S3-prefix upload additionally needs `s3:ListBucket` for the source bucket,
`s3:GetObject` for the source prefix, and multipart-upload write permissions for
the destination ZIP object.

The destination `s3:GetObject` permission is required even though
`s3-unspool` does not issue per-file destination `HeadObject` requests or read
destination object bodies. S3 authorizes `PutObject` requests with
`If-Match: <etag>` against object-read permission; without destination
`s3:GetObject`, changed files are rejected with `AccessDenied`.

## Advanced Usage

Use `sync_zip_to_s3_with_clients` when source ranged reads and destination
streaming writes should use different S3 client configuration. This is useful
for high-concurrency extraction, where the destination client may disable AWS
SDK upload stalled-stream protection while the source client keeps download
protection enabled.

Use `inspect_s3_zip` to read source ZIP size and file count before choosing
memory settings. `adaptive_source_get_concurrency` and
`adaptive_source_window_capacity` can then derive scheduler settings for
memory-bounded runtimes such as Lambda.

For high-concurrency extracts, configure the destination S3 client so AWS SDK
upload stalled-stream protection is relaxed or disabled. The source scheduler can
legitimately pause a destination body while waiting for planned ZIP bytes. Keep
download stalled-stream protection enabled for source reads.

ZIPs created by `upload_directory_zip_to_s3` include an embedded catalog at
`.s3-unspool/catalog.v1.json`. The catalog stores each file path and MD5 digest
so later extracts can skip unchanged files before decompressing them.

Directory markers are preserved explicitly. ZIP directory entries such as
`assets/empty/` extract to zero-byte S3 objects with trailing-slash keys, and
empty local directories or zero-byte S3 keys ending in `/` upload as ZIP
directory entries. Nonzero S3 objects ending in `/` are rejected as ambiguous.

## Assumptions

- Destination objects are written with single-part `PutObject`.
- Destination ETags are MD5 hashes of object content.
- Multipart destination objects and SSE-C destination ETags are out of scope for
  comparison.
- Destination writes use single `PutObject` requests, not multipart upload.
- S3-prefix upload rejects nonzero objects whose keys end in `/`.

The CLI and Lambda harness live in the repository workspace, but they are not
included in the published `s3-unspool` crate.
