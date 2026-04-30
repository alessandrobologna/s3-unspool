//! Streaming ZIP upload and extraction for Amazon S3.
//!
//! `s3-unspool` uploads local directories as ZIP objects in S3 and extracts S3
//! ZIP objects into destination prefixes without writing the archive or
//! extracted files to local storage.
//!
//! Local directory uploads generate the ZIP once and send it with S3 multipart
//! upload, so the archive does not need to be sized or written locally before
//! upload.
//!
//! Extraction compares ZIP entries with destination objects listed by
//! `ListObjectsV2`. Missing objects are uploaded with `If-None-Match: *`, and
//! changed objects are uploaded with `If-Match` against the listed destination
//! ETag so newer destination data is not overwritten accidentally.
//! Conditional write conflicts are recorded and skipped by default; set
//! [`SyncOptions::fail_on_conditional_conflict`] to return an error on the first
//! observed conflict.
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
//!
//! ZIPs created with [`upload_directory_zip_to_s3`] include an embedded catalog
//! at [`EMBEDDED_CATALOG_PATH`] by default. The catalog stores each file path and
//! MD5 digest so later extracts can skip unchanged files before decompressing
//! them. Set [`SyncOptions::ignore_embedded_catalog`] when you need to measure or
//! force the fallback extract-and-hash path.
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
//! let mut extract = SyncOptions::new(
//!     S3Object::parse("s3://my-bucket/releases/site.zip")?,
//!     S3Prefix::parse("s3://my-bucket/www/")?,
//! );
//! extract.delete_extra = true;
//!
//! let report = sync_zip_to_s3(&client, extract).await?;
//! println!("uploaded changed files: {}", report.summary.uploaded_changed);
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
pub use extract::{sync_zip_to_s3, sync_zip_to_s3_with_clients};
pub use inspect::{S3ZipInfo, inspect_s3_zip};
pub use options::{
    PutRetryPolicy, RetryJitter, SyncOptions, UploadOptions, UploadProgress, UploadProgressHandler,
    adaptive_source_get_concurrency, adaptive_source_window_capacity,
};
pub use report::{
    ObjectReport, OperationStatus, PutDiagnostics, PutRetryDiagnostics, SourceDiagnostics,
    SyncDiagnostics, SyncReport, SyncSummary, UploadReport,
};
pub use s3_uri::{S3Object, S3Prefix};
pub use upload::upload_directory_zip_to_s3;

#[cfg(test)]
mod tests;
