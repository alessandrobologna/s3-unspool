//! Streaming ZIP upload and extraction for Amazon S3.
//!
//! `s3-unspool` zips local directories and existing S3 prefixes into local or
//! S3 ZIP files, and unzips local or S3 ZIP files into local directories or S3
//! prefixes.
//!
//! Local directory and S3-prefix zip operations generate the ZIP once. S3 ZIP
//! destinations are streamed with multipart upload, and local ZIP destinations
//! are written through a temporary sibling file before being renamed into place.
//! Empty local directories and zero-byte S3 marker objects are preserved as ZIP
//! directory entries.
//!
//! Extraction compares ZIP entries with destination objects listed by
//! `ListObjectsV2`. Missing objects are uploaded with `If-None-Match: *`, and
//! changed objects are uploaded with `If-Match` against the listed destination
//! ETag so newer destination data is not overwritten accidentally.
//! Conditional write conflicts are recorded and skipped by default; set
//! [`SyncOptions::fail_on_conflict`] to return an error on the first observed
//! conflict.
//!
//! Conditional overwrites require `s3:GetObject` permission on destination
//! objects as well as `s3:PutObject`. `s3-unspool` does not issue per-file
//! destination `HeadObject` requests or read destination object bodies, but S3
//! authorizes `If-Match` writes against object-read permission.
//!
//! Destination `PutObject` bodies are fed by a source-range scheduler, so a body
//! can pause while waiting for planned ZIP bytes. For high-concurrency extracts,
//! consider relaxing or disabling AWS SDK upload stalled-stream protection on
//! the destination client. Keep download stalled-stream protection enabled for
//! source reads. The repository CLI and Lambda example configure this split.
//! Library users can call [`sync_zip_to_s3_with_clients`] when source reads and
//! destination writes need separate S3 client configuration.
//!
//! [`inspect_s3_zip`] reads source ZIP size and file count without downloading
//! the archive. It is useful before choosing memory-sensitive scheduler options
//! with [`AdaptiveSourceWindow`] or
//! [`SyncOptions::with_source_window_memory_budget_mb`].
//!
//! ZIPs created with [`upload_directory_zip_to_s3`], [`zip_directory_to_file`],
//! [`zip_s3_prefix_to_s3`], or [`zip_s3_prefix_to_file`] include an embedded catalog at
//! [`EMBEDDED_CATALOG_PATH`] by default. The catalog stores each file path and
//! MD5 digest so later extracts can skip unchanged files before decompressing
//! them. Set [`SyncOptions::force_hash_comparison`] when you need to measure
//! or force the fallback extract-and-hash path.
//!
//! Set [`SyncOptions::with_selection`] or the equivalent local unzip option
//! when only a subset of ZIP paths should be restored. Selection patterns use
//! gitignore-style syntax and are applied before source range planning, so
//! ranged `GetObject` requests are planned only for selected entries that still
//! need source bytes. Exclude-only selections restore every non-excluded ZIP
//! path. Selected extracts reject [`SyncOptions::delete_extra_objects`] because
//! unselected destination objects are outside the restore scope.
//!
//! ZIP directory entries round-trip as zero-byte S3 marker objects whose keys
//! end in `/`. Local upload preserves empty directories as ZIP directory
//! entries, and S3-prefix upload preserves zero-byte S3 marker objects as ZIP
//! directory entries while rejecting nonzero trailing-slash S3 objects as
//! ambiguous.
//!
//! # Extract an S3 ZIP to a Destination Prefix
//!
//! ```no_run
//! use aws_config::BehaviorVersion;
//! use aws_sdk_s3::Client;
//! use s3_unspool::{S3Object, S3Prefix, SyncOptions, sync_zip_to_s3};
//!
//! # async fn run() -> s3_unspool::Result<()> {
//! let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//! let client = Client::new(&config);
//!
//! let extract = SyncOptions::new(
//!     S3Object::parse("s3://my-bucket/releases/site.zip")?,
//!     S3Prefix::parse("s3://my-bucket/www/")?,
//! )
//! .delete_extra_objects();
//!
//! let report = sync_zip_to_s3(&client, extract).await?;
//! println!("uploaded changed files: {}", report.summary.uploaded_changed);
//! # Ok(())
//! # }
//! ```
//!
//! # Extract Selected Entries from an S3 ZIP
//!
//! ```no_run
//! use aws_config::BehaviorVersion;
//! use aws_sdk_s3::Client;
//! use s3_unspool::{S3Object, S3Prefix, SyncOptions, UnzipSelection, sync_zip_to_s3};
//!
//! # async fn run() -> s3_unspool::Result<()> {
//! let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//! let client = Client::new(&config);
//!
//! let extract = SyncOptions::new(
//!     S3Object::parse("s3://my-bucket/releases/site.zip")?,
//!     S3Prefix::parse("s3://my-bucket/www/")?,
//! )
//! .with_selection(
//!     UnzipSelection::new()
//!         .include("index.md")
//!         .include("docs/**/*.md")
//!         .exclude("docs/drafts/**"),
//! );
//!
//! let report = sync_zip_to_s3(&client, extract).await?;
//! println!("processed entries: {}", report.summary.zip_files);
//! # Ok(())
//! # }
//! ```
//!
//! # Upload a Directory as a Cataloged ZIP
//!
//! ```no_run
//! use aws_config::BehaviorVersion;
//! use aws_sdk_s3::Client;
//! use s3_unspool::{S3Object, UploadOptions, upload_directory_zip_to_s3};
//!
//! # async fn run() -> s3_unspool::Result<()> {
//! let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//! let client = Client::new(&config);
//!
//! let upload = UploadOptions::new(
//!     "./site",
//!     S3Object::parse("s3://my-bucket/releases/site.zip")?,
//! );
//! let report = upload_directory_zip_to_s3(&client, upload).await?;
//! println!("uploaded files: {}", report.files);
//! # Ok(())
//! # }
//! ```
//!
//! # Upload an S3 Prefix as a Cataloged ZIP
//!
//! ```no_run
//! use aws_config::BehaviorVersion;
//! use aws_sdk_s3::Client;
//! use s3_unspool::{S3Object, S3Prefix, S3PrefixUploadOptions, zip_s3_prefix_to_s3};
//!
//! # async fn run() -> s3_unspool::Result<()> {
//! let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//! let client = Client::new(&config);
//!
//! let upload = S3PrefixUploadOptions::new(
//!     S3Prefix::parse("s3://my-bucket/www/")?,
//!     S3Object::parse("s3://my-bucket/releases/site.zip")?,
//! );
//! let report = zip_s3_prefix_to_s3(&client, upload).await?;
//! println!("uploaded files: {}", report.files);
//! # Ok(())
//! # }
//! ```
//!
//! # Assumptions
//!
//! The crate assumes destination objects use single-part S3 ETags that match the
//! MD5 digest of the object body. Multipart destination objects and SSE-C
//! destination ETags are intentionally out of scope for comparison.

#![deny(missing_docs)]

mod catalog;
mod constants;
mod entry_reader;
mod error;
mod extract;
mod inspect;
mod options;
mod range;
mod report;
mod s3_uri;
mod source;
mod upload;
mod zip_manifest;

pub use constants::EMBEDDED_CATALOG_PATH;
pub use error::{Error, Result};
pub use extract::{
    dry_run_sync_zip_to_s3, dry_run_sync_zip_to_s3_with_clients, dry_run_unzip_file_to_local,
    dry_run_unzip_file_to_s3, dry_run_unzip_s3_zip_to_local, sync_zip_to_s3,
    sync_zip_to_s3_with_clients, unzip_file_to_local, unzip_file_to_s3, unzip_s3_zip_to_local,
};
pub use inspect::{S3ZipInfo, inspect_s3_zip};
pub use options::{
    AdaptiveSourceWindow, ComparisonMode, ConflictPolicy, DestinationCleanup, LocalUnzipOptions,
    LocalZipOptions, LocalZipSyncOptions, PutRetryPolicy, RetryJitter, S3PrefixLocalZipOptions,
    S3PrefixUploadOptions, S3ZipLocalUnzipOptions, SyncOptions, UnzipSelection, UploadOptions,
    UploadProgress, UploadProgressHandler, ZipCompression, adaptive_source_get_concurrency,
};
pub use report::{
    DryRunDiagnostics, DryRunObjectReport, DryRunOperationStatus, LocalUnzipDiagnostics,
    LocalUnzipReport, LocalZipReport, LocalZipToS3Report, ObjectReport, OperationStatus,
    PutDiagnostics, PutRetryDiagnostics, S3PrefixUploadReport, SourceDiagnostics, SyncDiagnostics,
    SyncReport, SyncSummary, UnzipDryRunReport, UnzipDryRunSummary, UploadReport, ZipDryRunReport,
};
pub use s3_uri::{S3Object, S3Prefix};
pub use upload::{
    dry_run_upload_directory_zip_to_s3, dry_run_zip_directory_to_file,
    dry_run_zip_s3_prefix_to_file, dry_run_zip_s3_prefix_to_s3, upload_directory_zip_to_s3,
    zip_directory_to_file, zip_s3_prefix_to_file, zip_s3_prefix_to_s3,
};

#[cfg(test)]
mod tests;
