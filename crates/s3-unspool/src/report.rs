use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::options::RetryJitter;
use crate::s3_uri::{S3Object, S3Prefix};

/// Summary returned by [`crate::upload_directory_zip_to_s3`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadReport {
    /// Local directory that was uploaded.
    pub source_dir: String,
    /// Destination ZIP object.
    pub destination: S3Object,
    /// Number of regular files included in the ZIP.
    pub files: usize,
    /// Number of preserved directory entries included in the ZIP.
    #[serde(default)]
    pub directories: usize,
    /// Total uncompressed payload bytes.
    pub uncompressed_bytes: u64,
    /// Total uploaded ZIP object bytes.
    pub zip_bytes: u64,
}

/// Summary returned by [`crate::zip_s3_prefix_to_s3`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3PrefixUploadReport {
    /// Source prefix that was uploaded.
    pub source: S3Prefix,
    /// Destination ZIP object.
    pub destination: S3Object,
    /// Number of regular source objects included as ZIP file entries.
    pub files: usize,
    /// Number of zero-byte trailing-slash source objects included as ZIP directories.
    pub directories: usize,
    /// Total number of ZIP entries written, excluding the embedded catalog.
    pub entries: usize,
    /// Total uncompressed payload bytes across regular file entries.
    pub uncompressed_bytes: u64,
    /// Total uploaded ZIP object bytes.
    pub zip_bytes: u64,
}

/// Summary returned by local ZIP creation helpers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalZipReport {
    /// Source tree that was zipped.
    pub source: String,
    /// Destination ZIP file path.
    pub destination_zip: String,
    /// Number of regular file entries included in the ZIP.
    pub files: usize,
    /// Number of preserved directory entries included in the ZIP.
    pub directories: usize,
    /// Total number of ZIP entries written, excluding the embedded catalog.
    pub entries: usize,
    /// Total uncompressed payload bytes across regular file entries.
    pub uncompressed_bytes: u64,
    /// Size of the generated ZIP file.
    pub zip_bytes: u64,
}

/// Aggregate counters for an extract run.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SyncSummary {
    /// Number of source ZIP entries found, excluding the embedded catalog.
    pub zip_files: usize,
    /// Number of destination objects listed before extraction.
    pub destination_objects: usize,
    /// Number of missing destination objects uploaded.
    pub uploaded_new: usize,
    /// Number of changed destination objects uploaded.
    pub uploaded_changed: usize,
    /// Number of unchanged destination objects skipped.
    pub skipped_unchanged: usize,
    /// Number of conditional write conflicts.
    pub conditional_conflicts: usize,
    /// Number of extra destination objects deleted.
    pub deleted_extra: usize,
    /// Number of per-object errors.
    pub errors: usize,
}

/// Full report returned by [`crate::sync_zip_to_s3`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncReport {
    /// Source ZIP object.
    pub source: S3Object,
    /// Destination prefix.
    pub destination: S3Prefix,
    /// Aggregate extract counters.
    pub summary: SyncSummary,
    /// Optional source scheduler and destination `PutObject` diagnostics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<SyncDiagnostics>,
    /// Per-object operation records.
    pub operations: Vec<ObjectReport>,
}

impl SyncReport {
    /// Returns `true` when one or more object operations failed.
    pub fn has_errors(&self) -> bool {
        self.summary.errors > 0
    }
}

/// Full report returned when extracting a local ZIP into an S3 prefix.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalZipToS3Report {
    /// Source ZIP file path.
    pub source_zip: String,
    /// Destination prefix.
    pub destination: S3Prefix,
    /// Aggregate extract counters.
    pub summary: SyncSummary,
    /// Per-entry operation records.
    pub operations: Vec<ObjectReport>,
}

impl LocalZipToS3Report {
    /// Returns `true` when one or more entry operations failed.
    pub fn has_errors(&self) -> bool {
        self.summary.errors > 0
    }
}

/// Full report returned when extracting a ZIP into a local directory.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalUnzipReport {
    /// Source ZIP object URI or local file path.
    pub source_zip: String,
    /// Destination local directory.
    pub destination_dir: String,
    /// Aggregate extract counters.
    pub summary: SyncSummary,
    /// Optional source scheduler diagnostics for S3 ZIP sources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<LocalUnzipDiagnostics>,
    /// Per-entry operation records.
    pub operations: Vec<ObjectReport>,
}

impl LocalUnzipReport {
    /// Returns `true` when one or more entry operations failed.
    pub fn has_errors(&self) -> bool {
        self.summary.errors > 0
    }
}

/// Effective extract settings and aggregate diagnostics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncDiagnostics {
    /// Effective entry concurrency.
    pub concurrency: usize,
    /// Effective destination `PutObject` concurrency.
    pub put_concurrency: usize,
    /// Effective destination `PutObject` retry policy.
    pub put_retry: PutRetryDiagnostics,
    /// Effective source block size in bytes.
    pub source_block_size: usize,
    /// Effective source block merge gap in bytes.
    pub source_block_merge_gap: usize,
    /// Effective source ranged `GetObject` concurrency.
    pub source_get_concurrency: usize,
    /// Effective source block window capacity in bytes.
    pub source_window_capacity: usize,
    /// Aggregate source scheduler counters.
    pub source: SourceDiagnostics,
    /// Aggregate destination `PutObject` counters.
    pub put: PutDiagnostics,
}

/// Effective local unzip settings and aggregate source diagnostics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalUnzipDiagnostics {
    /// Effective entry concurrency.
    pub concurrency: usize,
    /// Effective source block size in bytes.
    pub source_block_size: usize,
    /// Effective source block merge gap in bytes.
    pub source_block_merge_gap: usize,
    /// Effective source ranged `GetObject` concurrency.
    pub source_get_concurrency: usize,
    /// Effective source block window capacity in bytes.
    pub source_window_capacity: usize,
    /// Aggregate source scheduler counters.
    pub source: SourceDiagnostics,
}

/// Source scheduler and ranged `GetObject` counters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceDiagnostics {
    /// Source ZIP object size in bytes.
    pub source_zip_bytes: u64,
    /// Number of ZIP entries included in source plans.
    pub planned_entries: u64,
    /// Number of source blocks included in source plans.
    pub planned_blocks: u64,
    /// Number of source blocks fetched successfully.
    pub fetched_blocks: u64,
    /// Total ranged `GetObject` attempts, including retries.
    pub source_get_attempts: u64,
    /// Total ranged `GetObject` retries.
    pub source_get_retries: u64,
    /// Ranged `GetObject` request errors.
    pub source_get_request_errors: u64,
    /// Ranged `GetObject` response body errors.
    pub source_get_body_errors: u64,
    /// Ranged `GetObject` responses that ended before the requested bytes were read.
    pub source_get_short_body_errors: u64,
    /// Source block fetches that failed after all retry attempts.
    pub source_get_errors: u64,
    /// Sum of planned source block sizes.
    pub planned_source_bytes: u64,
    /// Sum of fetched source block sizes.
    pub fetched_source_bytes: u64,
    /// Unique source bytes covered by fetched ranges.
    pub unique_source_bytes: u64,
    /// Ratio of fetched source bytes to unique fetched source bytes.
    pub source_amplification: f64,
    /// Number of block read requests served from ready blocks.
    pub block_hits: u64,
    /// Number of block read requests that waited for scheduled data.
    pub block_waits: u64,
    /// Number of ready source blocks released from the resident window after all
    /// planned claims consumed them.
    pub block_releases: u64,
    /// Number of reader cache misses. This should remain zero for the planned
    /// source scheduler.
    pub block_misses: u64,
    /// Number of explicit replay fetches for blocks that had already been
    /// released from the resident window.
    pub block_refetches: u64,
    /// Highest number of concurrent ranged `GetObject` requests.
    pub active_gets_high_water: u64,
}

/// Effective destination `PutObject` retry settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutRetryDiagnostics {
    /// Maximum application-level `PutObject` attempts per object.
    pub max_attempts: usize,
    /// Base delay for retryable non-throttling failures, in milliseconds.
    pub base_delay_ms: u64,
    /// Maximum delay for retryable non-throttling failures, in milliseconds.
    pub max_delay_ms: u64,
    /// Base delay for throttling failures such as S3 `SlowDown`, in milliseconds.
    pub slowdown_base_delay_ms: u64,
    /// Maximum delay for throttling failures such as S3 `SlowDown`, in milliseconds.
    pub slowdown_max_delay_ms: u64,
    /// Jitter mode applied to computed retry delays.
    pub jitter: RetryJitter,
}

/// Destination `PutObject` failure counters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PutDiagnostics {
    /// Number of failed `PutObject` attempts, including retryable attempts that
    /// later succeeded.
    pub failed_attempts: u64,
    /// Failed `PutObject` attempts grouped by AWS error code or SDK failure kind.
    pub failures_by_error_code: BTreeMap<String, u64>,
    /// Application-level retry attempts scheduled after failed `PutObject` attempts.
    pub retry_attempts: u64,
    /// Failed `PutObject` attempts classified as throttling.
    pub throttled_attempts: u64,
    /// Number of waits on the shared destination PUT throttle.
    pub throttle_waits: u64,
    /// Total milliseconds spent waiting on the shared destination PUT throttle.
    pub throttle_wait_millis: u64,
}

/// Status for a single destination object operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// The destination key was absent and was uploaded.
    UploadedNew,
    /// The destination key existed and was overwritten.
    UploadedChanged,
    /// The destination key existed and already matched the source entry.
    SkippedUnchanged,
    /// A conditional write failed because the destination changed after listing.
    ConditionalConflict,
    /// The destination key was extra and was deleted.
    DeletedExtra,
    /// The object operation failed.
    Error,
}

/// Per-object operation result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectReport {
    /// Operation status.
    pub status: OperationStatus,
    /// Destination object key or local path.
    pub key: String,
    /// Source ZIP path when the operation corresponds to a ZIP entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zip_path: Option<String>,
    /// Source entry size in bytes when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Source MD5 digest when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    /// Destination ETag observed during the initial listing when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_etag: Option<String>,
    /// Error or conflict message when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
pub(crate) fn summarize(report: &mut SyncReport) {
    for operation in &report.operations {
        summarize_operation(&mut report.summary, operation);
    }
}

pub(crate) fn summarize_operation(summary: &mut SyncSummary, operation: &ObjectReport) {
    match operation.status {
        OperationStatus::UploadedNew => summary.uploaded_new += 1,
        OperationStatus::UploadedChanged => summary.uploaded_changed += 1,
        OperationStatus::SkippedUnchanged => summary.skipped_unchanged += 1,
        OperationStatus::ConditionalConflict => summary.conditional_conflicts += 1,
        OperationStatus::DeletedExtra => summary.deleted_extra += 1,
        OperationStatus::Error => summary.errors += 1,
    }
}
