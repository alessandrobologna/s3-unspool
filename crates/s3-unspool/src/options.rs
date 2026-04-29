use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants::*;
use crate::s3_uri::{S3Object, S3Prefix};

/// Options for extracting a ZIP object from S3 into an S3 prefix.
#[derive(Clone, Debug)]
pub struct SyncOptions {
    /// Source ZIP object.
    pub source: S3Object,
    /// Destination prefix that receives ZIP entries.
    pub destination: S3Prefix,
    /// Delete destination objects under the prefix that are not present in the ZIP.
    pub delete_extra: bool,
    /// Collect source scheduler diagnostics in the returned report.
    pub collect_diagnostics: bool,
    /// Ignore the embedded update catalog even when the ZIP contains one.
    pub ignore_embedded_catalog: bool,
    /// Return an error on the first conditional write conflict.
    ///
    /// When this is `false`, conditional conflicts are recorded in the report
    /// and extraction continues.
    pub fail_on_conditional_conflict: bool,
    /// Collect one operation record per processed object in the returned report.
    pub collect_operations: bool,
    /// Maximum number of ZIP entries processed concurrently.
    pub concurrency: usize,
    /// Maximum number of destination `PutObject` requests in flight.
    pub put_concurrency: usize,
    /// Retry and backoff policy for destination `PutObject` attempts.
    pub put_retry_policy: PutRetryPolicy,
    /// Maximum size for planned source ZIP blocks.
    pub source_block_size: usize,
    /// Maximum gap that can be read while coalescing adjacent source spans.
    pub source_block_merge_gap: usize,
    /// Maximum number of ranged source `GetObject` requests in flight.
    pub source_get_concurrency: usize,
    /// Maximum bytes held by the planned source block window.
    pub source_window_capacity: usize,
    /// Buffer size used when streaming entry bodies to S3.
    pub body_chunk_size: usize,
    /// Capacity of the in-memory pipe between decompression and S3 upload.
    pub pipe_capacity: usize,
}

impl SyncOptions {
    /// Creates extract options for a source ZIP object and destination prefix.
    pub fn new(source: S3Object, destination: S3Prefix) -> Self {
        Self {
            source,
            destination,
            delete_extra: false,
            collect_diagnostics: false,
            ignore_embedded_catalog: false,
            fail_on_conditional_conflict: false,
            collect_operations: true,
            concurrency: DEFAULT_CONCURRENCY,
            put_concurrency: DEFAULT_PUT_CONCURRENCY,
            put_retry_policy: PutRetryPolicy::default(),
            source_block_size: DEFAULT_SOURCE_BLOCK_SIZE,
            source_block_merge_gap: DEFAULT_SOURCE_BLOCK_MERGE_GAP,
            source_get_concurrency: DEFAULT_SOURCE_GET_CONCURRENCY,
            source_window_capacity: DEFAULT_SOURCE_WINDOW_CAPACITY,
            body_chunk_size: DEFAULT_BODY_CHUNK_SIZE,
            pipe_capacity: DEFAULT_PIPE_CAPACITY,
        }
    }
}

/// Retry and backoff policy for destination `PutObject` attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutRetryPolicy {
    /// Maximum number of application-level `PutObject` attempts per object.
    pub max_attempts: usize,
    /// Base delay for retryable non-throttling failures.
    pub base_delay: Duration,
    /// Maximum delay for retryable non-throttling failures.
    pub max_delay: Duration,
    /// Base delay for throttling failures such as S3 `SlowDown`.
    pub slowdown_base_delay: Duration,
    /// Maximum delay for throttling failures such as S3 `SlowDown`.
    pub slowdown_max_delay: Duration,
    /// Jitter mode applied to computed retry delays.
    pub jitter: RetryJitter,
}

impl Default for PutRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: PUT_OBJECT_MAX_ATTEMPTS,
            base_delay: Duration::from_millis(PUT_OBJECT_RETRY_BASE_DELAY_MS),
            max_delay: Duration::from_millis(PUT_OBJECT_RETRY_MAX_DELAY_MS),
            slowdown_base_delay: Duration::from_millis(PUT_OBJECT_SLOWDOWN_RETRY_BASE_DELAY_MS),
            slowdown_max_delay: Duration::from_millis(PUT_OBJECT_SLOWDOWN_RETRY_MAX_DELAY_MS),
            jitter: RetryJitter::Full,
        }
    }
}

/// Jitter mode used for application-level destination `PutObject` retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryJitter {
    /// Use full jitter, selecting a random delay from zero to the computed cap.
    Full,
    /// Use deterministic exponential delays without jitter.
    None,
}

/// Computes adaptive source `GetObject` concurrency for a fixed memory envelope.
///
/// The policy scales source reads in the same direction as Lambda CPU: one
/// source request per 256 MiB of configured memory, capped at eight.
pub fn adaptive_source_get_concurrency(available_memory_mb: u64) -> usize {
    let slots = available_memory_mb / ADAPTIVE_SOURCE_GET_MEMORY_STEP_MB;
    usize::try_from(slots)
        .unwrap_or(usize::MAX)
        .clamp(1, ADAPTIVE_SOURCE_MAX_GET_CONCURRENCY)
}

/// Computes an adaptive source block window capacity.
///
/// The returned capacity reserves a fixed runtime baseline, a fixed amount per
/// extraction worker, and a fixed amount per ZIP file entry before assigning
/// otherwise idle memory to the source ZIP block window.
pub fn adaptive_source_window_capacity(
    available_memory_mb: u64,
    source_zip_bytes: u64,
    concurrency: usize,
    zip_file_count: usize,
    source_block_size: usize,
    source_get_concurrency: usize,
) -> usize {
    let Some(available_memory_bytes) = available_memory_mb.checked_mul(1024 * 1024) else {
        return usize::try_from(source_zip_bytes).unwrap_or(usize::MAX);
    };
    let concurrency = u64::try_from(concurrency.max(1)).unwrap_or(u64::MAX);
    let zip_file_count = u64::try_from(zip_file_count).unwrap_or(u64::MAX);
    let worker_budget = concurrency.saturating_mul(ADAPTIVE_CACHE_WORKER_OVERHEAD);
    let file_budget = zip_file_count.saturating_mul(ADAPTIVE_CACHE_FILE_OVERHEAD);
    let in_flight_budget = u64::try_from(source_get_concurrency.max(1))
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(source_block_size).unwrap_or(u64::MAX));
    let reserved = ADAPTIVE_CACHE_BASE_OVERHEAD
        .saturating_add(worker_budget)
        .saturating_add(file_budget)
        .saturating_add(in_flight_budget);
    let capacity = available_memory_bytes
        .saturating_sub(reserved)
        .min(source_zip_bytes);
    let capacity = if capacity > ADAPTIVE_CACHE_LARGE_THRESHOLD {
        capacity.saturating_sub(ADAPTIVE_CACHE_LARGE_RSS_SLACK)
    } else {
        capacity
    }
    .min(ADAPTIVE_CACHE_MAX_WINDOW_CAPACITY);

    let minimum_block_capacity = u64::try_from(source_block_size)
        .unwrap_or(u64::MAX)
        .min(source_zip_bytes);
    let capacity = if capacity < minimum_block_capacity {
        0
    } else {
        capacity
    };

    usize::try_from(capacity).unwrap_or(usize::MAX)
}

/// Options for zipping a local directory and uploading it as an S3 object.
#[derive(Clone)]
pub struct UploadOptions {
    /// Local directory whose regular files should be included recursively.
    pub source_dir: PathBuf,
    /// Destination ZIP object.
    pub destination: S3Object,
    /// Include the embedded update catalog at [`crate::EMBEDDED_CATALOG_PATH`].
    pub include_catalog: bool,
    /// Buffer size used when streaming the ZIP body to S3.
    pub body_chunk_size: usize,
    /// Capacity of the in-memory pipe between ZIP production and S3 upload.
    pub pipe_capacity: usize,
    /// Optional progress callback invoked during upload preparation and ZIP streaming.
    pub progress: Option<UploadProgressHandler>,
}

impl UploadOptions {
    /// Creates upload options for a local source directory and destination object.
    pub fn new(source_dir: impl Into<PathBuf>, destination: S3Object) -> Self {
        Self {
            source_dir: source_dir.into(),
            destination,
            include_catalog: true,
            body_chunk_size: DEFAULT_BODY_CHUNK_SIZE,
            pipe_capacity: DEFAULT_PIPE_CAPACITY,
            progress: None,
        }
    }
}

impl std::fmt::Debug for UploadOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UploadOptions")
            .field("source_dir", &self.source_dir)
            .field("destination", &self.destination)
            .field("include_catalog", &self.include_catalog)
            .field("body_chunk_size", &self.body_chunk_size)
            .field("pipe_capacity", &self.pipe_capacity)
            .field(
                "progress",
                &self.progress.as_ref().map(|_| "UploadProgressHandler"),
            )
            .finish()
    }
}

/// Upload progress callback wrapper.
///
/// The callback is invoked synchronously from the upload task whenever progress
/// state changes. Keep the callback lightweight; hand work off to another task
/// if it needs to perform I/O.
#[derive(Clone)]
pub struct UploadProgressHandler {
    callback: Arc<dyn Fn(UploadProgress) + Send + Sync + 'static>,
}

impl UploadProgressHandler {
    /// Creates an upload progress handler from a callback.
    pub fn new(callback: impl Fn(UploadProgress) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    pub(crate) fn emit(&self, progress: UploadProgress) {
        (self.callback)(progress);
    }
}

impl std::fmt::Debug for UploadProgressHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("UploadProgressHandler")
    }
}

/// Progress event emitted while preparing and streaming an upload ZIP.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UploadProgress {
    /// The source directory has been scanned and the total file count is known.
    Planned {
        /// Total number of files that will be included in the ZIP.
        total_files: usize,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
    },
    /// A file has started streaming into the ZIP writer.
    FileStarted {
        /// One-based index of the file currently being streamed.
        current_file: usize,
        /// Total number of files that will be included in the ZIP.
        total_files: usize,
        /// Number of files that have finished streaming into the ZIP.
        processed_files: usize,
        /// Uncompressed bytes that have finished streaming into the ZIP.
        processed_bytes: u64,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
        /// ZIP path of the file that just started.
        path: String,
    },
    /// A file is still streaming and byte progress has advanced.
    FileProgress {
        /// One-based index of the file currently being streamed.
        current_file: usize,
        /// Total number of files that will be included in the ZIP.
        total_files: usize,
        /// Number of files that have finished streaming into the ZIP.
        processed_files: usize,
        /// Uncompressed bytes that have streamed into the ZIP producer so far.
        processed_bytes: u64,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
        /// ZIP path of the file currently being streamed.
        path: String,
    },
    /// One file has finished streaming into the ZIP writer.
    FileFinished {
        /// Number of files that have finished streaming into the ZIP.
        processed_files: usize,
        /// Total number of files that will be included in the ZIP.
        total_files: usize,
        /// Uncompressed bytes that have finished streaming into the ZIP.
        processed_bytes: u64,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
        /// ZIP path of the file that just finished.
        path: String,
    },
    /// ZIP production has finished writing into the upload pipe.
    ///
    /// S3 multipart upload completion may still be in progress when this event
    /// is emitted.
    Finished {
        /// Total number of files included in the ZIP.
        total_files: usize,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
    },
}
