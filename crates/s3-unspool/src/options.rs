use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_zip::Compression;
use serde::{Deserialize, Serialize};

use crate::constants::*;
use crate::s3_uri::{S3Object, S3Prefix};

/// Compression method used for regular file entries when creating ZIP archives.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ZipCompression {
    /// Use Deflate, the default ZIP compression method supported by common tools.
    #[default]
    Deflate,
    /// Use Zstandard method 93 for regular file entries.
    #[cfg(feature = "zstd")]
    Zstd,
}

impl ZipCompression {
    pub(crate) fn to_async_zip(self) -> Compression {
        match self {
            ZipCompression::Deflate => Compression::Deflate,
            #[cfg(feature = "zstd")]
            ZipCompression::Zstd => Compression::Zstd,
        }
    }

    /// Returns a stable lowercase name for display or configuration.
    pub fn as_str(self) -> &'static str {
        match self {
            ZipCompression::Deflate => "deflate",
            #[cfg(feature = "zstd")]
            ZipCompression::Zstd => "zstd",
        }
    }
}

/// ZIP entry selection patterns for unzip APIs.
///
/// When no patterns are configured, unzip operations process every supported ZIP
/// entry. Patterns are matched against normalized ZIP paths, not local
/// filesystem paths or destination S3 keys. Use
/// [`UnzipSelection::new`]/[`UnzipSelection::include`] for builder-style
/// configuration, or pass an array such as `["docs/**", "!docs/drafts/**"]`
/// to `with_selection`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnzipSelection {
    patterns: Vec<String>,
}

impl UnzipSelection {
    /// Creates an empty selection that extracts every supported ZIP entry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a selection from ordered include/exclude patterns.
    ///
    /// Patterns use gitignore-style matching. Later patterns override earlier
    /// patterns, and patterns prefixed with `!` exclude matching ZIP paths.
    /// If only exclude patterns are configured, every non-excluded ZIP path is
    /// selected.
    pub fn patterns(patterns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            patterns: patterns.into_iter().map(Into::into).collect(),
        }
    }

    /// Adds an include pattern.
    ///
    /// Leading `!` and `#` characters are treated as literal path characters
    /// in the builder API. Use [`Self::patterns`] for raw selection lines.
    pub fn include(mut self, pattern: impl Into<String>) -> Self {
        self.patterns
            .push(escape_leading_gitignore_marker(pattern.into()));
        self
    }

    /// Adds an exclude pattern.
    ///
    /// Leading `!` and `#` characters are treated as literal path characters
    /// in the builder API. Use [`Self::patterns`] for raw selection lines.
    pub fn exclude(mut self, pattern: impl Into<String>) -> Self {
        self.patterns.push(format!(
            "!{}",
            escape_leading_gitignore_marker(pattern.into())
        ));
        self
    }

    /// Returns true when no selection patterns have been configured.
    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Returns the ordered selection patterns.
    pub fn as_patterns(&self) -> &[String] {
        &self.patterns
    }
}

impl<const N: usize> From<[&str; N]> for UnzipSelection {
    fn from(patterns: [&str; N]) -> Self {
        Self::patterns(patterns)
    }
}

impl From<Vec<String>> for UnzipSelection {
    fn from(patterns: Vec<String>) -> Self {
        Self { patterns }
    }
}

fn escape_leading_gitignore_marker(pattern: String) -> String {
    if pattern.starts_with('!') || pattern.starts_with('#') {
        format!("\\{pattern}")
    } else {
        pattern
    }
}

/// How an unzip-to-S3 operation treats destination objects that are not in the ZIP.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DestinationCleanup {
    /// Keep destination objects that do not correspond to selected ZIP entries.
    #[default]
    KeepExtra,
    /// Delete destination objects under the destination prefix that are not in the ZIP.
    DeleteExtra,
}

impl DestinationCleanup {
    pub(crate) fn deletes_extra(self) -> bool {
        matches!(self, Self::DeleteExtra)
    }
}

/// How unzip operations compare ZIP entries with existing destination objects.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ComparisonMode {
    /// Use the embedded catalog when present, then fall back to entry hashing.
    #[default]
    CatalogThenHash,
    /// Ignore any embedded catalog and hash ZIP entries for comparison.
    HashEntries,
}

impl ComparisonMode {
    pub(crate) fn ignores_embedded_catalog(self) -> bool {
        matches!(self, Self::HashEntries)
    }
}

/// How unzip-to-S3 operations handle destination conditional write conflicts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConflictPolicy {
    /// Record conditional conflicts in the report and continue processing.
    #[default]
    ReportAndContinue,
    /// Return an error after the first conditional conflict is observed.
    FailFast,
}

impl ConflictPolicy {
    pub(crate) fn fails_fast(self) -> bool {
        matches!(self, Self::FailFast)
    }
}

/// Options for extracting a ZIP object from S3 into an S3 prefix.
#[derive(Clone, Debug)]
pub struct SyncOptions {
    /// Source ZIP object.
    pub(crate) source: S3Object,
    /// Destination prefix that receives ZIP entries.
    pub(crate) destination: S3Prefix,
    /// How destination objects outside the ZIP are handled.
    ///
    /// Deleting extra objects requires a non-empty destination prefix so a
    /// bucket root is never swept accidentally.
    pub(crate) cleanup: DestinationCleanup,
    /// ZIP entry selection. Empty selection extracts every supported entry.
    ///
    /// Selection cannot be combined with [`DestinationCleanup::DeleteExtra`].
    pub(crate) selection: UnzipSelection,
    /// Collect source scheduler diagnostics in the returned report.
    pub(crate) collect_diagnostics: bool,
    /// Comparison policy for embedded catalogs and entry hashing.
    pub(crate) comparison: ComparisonMode,
    /// Conditional write conflict handling policy.
    pub(crate) conflict_policy: ConflictPolicy,
    /// Collect one operation record per processed object in the returned report.
    pub(crate) collect_operations: bool,
    /// Maximum number of ZIP entries processed concurrently.
    ///
    /// Must be greater than zero.
    pub(crate) concurrency: usize,
    /// Maximum number of destination `PutObject` requests in flight.
    ///
    /// Must be greater than zero.
    pub(crate) put_concurrency: usize,
    /// Retry and backoff policy for destination `PutObject` attempts.
    pub(crate) put_retry_policy: PutRetryPolicy,
    /// Maximum size for planned source ZIP blocks.
    ///
    /// Must be greater than zero.
    pub(crate) source_block_size: usize,
    /// Maximum gap that can be read while coalescing adjacent source spans.
    pub(crate) source_block_merge_gap: usize,
    /// Maximum number of ranged source `GetObject` requests in flight.
    ///
    /// Must be greater than zero.
    pub(crate) source_get_concurrency: usize,
    /// Maximum bytes held by the planned source block window.
    ///
    /// When nonzero, this must be large enough to hold the effective source
    /// block size after clamping that block size to the source ZIP size.
    pub(crate) source_window_capacity: usize,
    /// Available memory budget, in MiB, used to derive the source block window.
    ///
    /// When set, extraction computes [`Self::source_window_capacity`] after the
    /// ZIP manifest is loaded, using the real source ZIP size and file count.
    /// This is useful for memory-bounded runtimes that want to assign otherwise
    /// idle memory to source block buffering while reserving space for catalog
    /// metadata and worker overhead.
    pub(crate) source_window_memory_budget_mb: Option<u64>,
    /// Buffer size used when streaming entry bodies to S3.
    ///
    /// Must be greater than zero and no larger than 16 MiB.
    pub(crate) body_chunk_size: usize,
    /// Capacity of the in-memory pipe between decompression and S3 upload.
    ///
    /// Must be greater than zero and no larger than 64 MiB.
    pub(crate) pipe_capacity: usize,
}

impl SyncOptions {
    /// Creates extract options for a source ZIP object and destination prefix.
    pub fn new(source: S3Object, destination: S3Prefix) -> Self {
        Self {
            source,
            destination,
            cleanup: DestinationCleanup::default(),
            selection: UnzipSelection::default(),
            collect_diagnostics: false,
            comparison: ComparisonMode::default(),
            conflict_policy: ConflictPolicy::default(),
            collect_operations: true,
            concurrency: DEFAULT_CONCURRENCY,
            put_concurrency: DEFAULT_PUT_CONCURRENCY,
            put_retry_policy: PutRetryPolicy::default(),
            source_block_size: DEFAULT_SOURCE_BLOCK_SIZE,
            source_block_merge_gap: DEFAULT_SOURCE_BLOCK_MERGE_GAP,
            source_get_concurrency: DEFAULT_SOURCE_GET_CONCURRENCY,
            source_window_capacity: DEFAULT_SOURCE_WINDOW_CAPACITY,
            source_window_memory_budget_mb: None,
            body_chunk_size: DEFAULT_BODY_CHUNK_SIZE,
            pipe_capacity: DEFAULT_PIPE_CAPACITY,
        }
    }

    /// Returns the source ZIP object.
    pub fn source(&self) -> &S3Object {
        &self.source
    }

    /// Returns the destination prefix.
    pub fn destination(&self) -> &S3Prefix {
        &self.destination
    }

    /// Returns the destination cleanup policy.
    pub fn cleanup(&self) -> DestinationCleanup {
        self.cleanup
    }

    /// Returns the ZIP entry selection patterns.
    pub fn selection(&self) -> &UnzipSelection {
        &self.selection
    }

    /// Returns whether source scheduler diagnostics are collected.
    pub fn collects_diagnostics(&self) -> bool {
        self.collect_diagnostics
    }

    /// Returns the ZIP entry comparison policy.
    pub fn comparison_mode(&self) -> ComparisonMode {
        self.comparison
    }

    /// Returns the conditional write conflict handling policy.
    pub fn conflict_policy(&self) -> ConflictPolicy {
        self.conflict_policy
    }

    /// Returns whether per-object operation records are collected.
    pub fn collects_operations(&self) -> bool {
        self.collect_operations
    }

    /// Returns the maximum number of ZIP entries processed concurrently.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Returns the maximum number of destination `PutObject` requests in flight.
    pub fn put_concurrency(&self) -> usize {
        self.put_concurrency
    }

    /// Returns the retry and backoff policy for destination `PutObject` attempts.
    pub fn put_retry_policy(&self) -> &PutRetryPolicy {
        &self.put_retry_policy
    }

    /// Returns the maximum size for planned source ZIP blocks.
    pub fn source_block_size(&self) -> usize {
        self.source_block_size
    }

    /// Returns the maximum gap that can be read while coalescing adjacent source spans.
    pub fn source_block_merge_gap(&self) -> usize {
        self.source_block_merge_gap
    }

    /// Returns the maximum number of ranged source `GetObject` requests in flight.
    pub fn source_get_concurrency(&self) -> usize {
        self.source_get_concurrency
    }

    /// Returns the configured source block window capacity.
    ///
    /// When [`Self::with_source_window_memory_budget_mb`] is used, extraction
    /// derives the effective post-manifest value at runtime and reports it in
    /// [`crate::SyncDiagnostics::source_window_capacity`] when diagnostics are collected.
    pub fn source_window_capacity(&self) -> usize {
        self.source_window_capacity
    }

    /// Returns the available memory budget, in MiB, used to derive the source block window.
    pub fn source_window_memory_budget_mb(&self) -> Option<u64> {
        self.source_window_memory_budget_mb
    }

    /// Returns the buffer size used when streaming entry bodies to S3.
    pub fn body_chunk_size(&self) -> usize {
        self.body_chunk_size
    }

    /// Returns the in-memory pipe capacity between decompression and S3 upload.
    pub fn pipe_capacity(&self) -> usize {
        self.pipe_capacity
    }

    /// Sets ZIP entry selection patterns.
    pub fn with_selection(mut self, selection: impl Into<UnzipSelection>) -> Self {
        self.selection = selection.into();
        self
    }

    /// Deletes destination objects under the prefix that are not present in the ZIP.
    ///
    /// This requires a non-empty destination prefix and cannot be combined with
    /// a non-empty selection.
    pub fn delete_extra_objects(mut self) -> Self {
        self.cleanup = DestinationCleanup::DeleteExtra;
        self
    }

    /// Sets the destination cleanup policy.
    pub fn with_cleanup(mut self, cleanup: DestinationCleanup) -> Self {
        self.cleanup = cleanup;
        self
    }

    /// Collects source scheduler diagnostics in the returned report.
    pub fn collect_diagnostics(mut self) -> Self {
        self.collect_diagnostics = true;
        self
    }

    /// Ignores any embedded catalog and hashes ZIP entries for comparison.
    pub fn force_hash_comparison(mut self) -> Self {
        self.comparison = ComparisonMode::HashEntries;
        self
    }

    /// Sets the ZIP entry comparison policy.
    pub fn with_comparison_mode(mut self, comparison: ComparisonMode) -> Self {
        self.comparison = comparison;
        self
    }

    /// Returns an error after the first conditional write conflict is observed.
    pub fn fail_on_conflict(mut self) -> Self {
        self.conflict_policy = ConflictPolicy::FailFast;
        self
    }

    /// Sets the conditional write conflict handling policy.
    pub fn with_conflict_policy(mut self, conflict_policy: ConflictPolicy) -> Self {
        self.conflict_policy = conflict_policy;
        self
    }

    /// Omits per-object operation records from the returned report.
    pub fn without_operations(mut self) -> Self {
        self.collect_operations = false;
        self
    }

    /// Sets the maximum number of ZIP entries processed concurrently.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the maximum number of destination `PutObject` requests in flight.
    pub fn with_put_concurrency(mut self, put_concurrency: usize) -> Self {
        self.put_concurrency = put_concurrency;
        self
    }

    /// Sets the retry and backoff policy for destination `PutObject` attempts.
    pub fn with_put_retry_policy(mut self, put_retry_policy: PutRetryPolicy) -> Self {
        self.put_retry_policy = put_retry_policy;
        self
    }

    /// Sets the maximum size for planned source ZIP blocks.
    pub fn with_source_block_size(mut self, source_block_size: usize) -> Self {
        self.source_block_size = source_block_size;
        self
    }

    /// Sets the maximum gap that can be read while coalescing adjacent source spans.
    pub fn with_source_block_merge_gap(mut self, source_block_merge_gap: usize) -> Self {
        self.source_block_merge_gap = source_block_merge_gap;
        self
    }

    /// Sets the maximum number of ranged source `GetObject` requests in flight.
    pub fn with_source_get_concurrency(mut self, source_get_concurrency: usize) -> Self {
        self.source_get_concurrency = source_get_concurrency;
        self
    }

    /// Sets the maximum bytes held by the planned source block window.
    pub fn with_source_window_capacity(mut self, source_window_capacity: usize) -> Self {
        self.source_window_capacity = source_window_capacity;
        self
    }

    /// Sets the available memory budget, in MiB, used to derive the source block window.
    pub fn with_source_window_memory_budget_mb(
        mut self,
        source_window_memory_budget_mb: u64,
    ) -> Self {
        self.source_window_memory_budget_mb = Some(source_window_memory_budget_mb);
        self
    }

    /// Sets the buffer size used when streaming entry bodies to S3.
    pub fn with_body_chunk_size(mut self, body_chunk_size: usize) -> Self {
        self.body_chunk_size = body_chunk_size;
        self
    }

    /// Sets the in-memory pipe capacity between decompression and S3 upload.
    pub fn with_pipe_capacity(mut self, pipe_capacity: usize) -> Self {
        self.pipe_capacity = pipe_capacity;
        self
    }
}

/// Retry and backoff policy for destination `PutObject` attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutRetryPolicy {
    /// Maximum number of application-level `PutObject` attempts per object.
    ///
    /// Must be greater than zero.
    pub(crate) max_attempts: usize,
    /// Base delay for retryable non-throttling failures.
    pub(crate) base_delay: Duration,
    /// Maximum delay for retryable non-throttling failures.
    ///
    /// Must be greater than or equal to [`Self::base_delay`].
    pub(crate) max_delay: Duration,
    /// Base delay for throttling failures such as S3 `SlowDown`.
    pub(crate) slowdown_base_delay: Duration,
    /// Maximum delay for throttling failures such as S3 `SlowDown`.
    ///
    /// Must be greater than or equal to [`Self::slowdown_base_delay`].
    pub(crate) slowdown_max_delay: Duration,
    /// Jitter mode applied to computed retry delays.
    pub(crate) jitter: RetryJitter,
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

impl PutRetryPolicy {
    /// Returns the maximum number of application-level `PutObject` attempts.
    pub fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    /// Returns the base delay for retryable non-throttling failures.
    pub fn base_delay(&self) -> Duration {
        self.base_delay
    }

    /// Returns the maximum delay for retryable non-throttling failures.
    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// Returns the base delay for throttling failures such as S3 `SlowDown`.
    pub fn slowdown_base_delay(&self) -> Duration {
        self.slowdown_base_delay
    }

    /// Returns the maximum delay for throttling failures such as S3 `SlowDown`.
    pub fn slowdown_max_delay(&self) -> Duration {
        self.slowdown_max_delay
    }

    /// Returns the jitter mode applied to computed retry delays.
    pub fn jitter(&self) -> RetryJitter {
        self.jitter
    }

    /// Sets the maximum number of application-level `PutObject` attempts.
    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the base delay for retryable non-throttling failures.
    pub fn with_base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Sets the maximum delay for retryable non-throttling failures.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Sets the base delay for throttling failures such as S3 `SlowDown`.
    pub fn with_slowdown_base_delay(mut self, slowdown_base_delay: Duration) -> Self {
        self.slowdown_base_delay = slowdown_base_delay;
        self
    }

    /// Sets the maximum delay for throttling failures such as S3 `SlowDown`.
    pub fn with_slowdown_max_delay(mut self, slowdown_max_delay: Duration) -> Self {
        self.slowdown_max_delay = slowdown_max_delay;
        self
    }

    /// Sets the jitter mode applied to computed retry delays.
    pub fn with_jitter(mut self, jitter: RetryJitter) -> Self {
        self.jitter = jitter;
        self
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

/// Inputs for deriving an adaptive source block window capacity.
///
/// The capacity calculation reserves a fixed runtime baseline, a fixed amount
/// per extraction worker, and a fixed amount per ZIP file entry before assigning
/// otherwise idle memory to the source ZIP block window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveSourceWindow {
    /// Available runtime memory, in MiB.
    pub(crate) available_memory_mb: u64,
    /// Size of the source ZIP object, in bytes.
    pub(crate) source_zip_bytes: u64,
    /// Number of regular file entries in the ZIP.
    pub(crate) zip_file_count: usize,
    /// Maximum number of ZIP entries processed concurrently.
    pub(crate) concurrency: usize,
    /// Maximum size for planned source ZIP blocks.
    pub(crate) source_block_size: usize,
    /// Maximum number of ranged source `GetObject` requests in flight.
    pub(crate) source_get_concurrency: usize,
}

impl AdaptiveSourceWindow {
    /// Creates adaptive source window inputs with the crate defaults for scheduler knobs.
    pub fn new(available_memory_mb: u64, source_zip_bytes: u64, zip_file_count: usize) -> Self {
        Self {
            available_memory_mb,
            source_zip_bytes,
            zip_file_count,
            concurrency: DEFAULT_CONCURRENCY,
            source_block_size: DEFAULT_SOURCE_BLOCK_SIZE,
            source_get_concurrency: DEFAULT_SOURCE_GET_CONCURRENCY,
        }
    }

    /// Sets the maximum number of ZIP entries processed concurrently.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the maximum size for planned source ZIP blocks.
    pub fn with_source_block_size(mut self, source_block_size: usize) -> Self {
        self.source_block_size = source_block_size;
        self
    }

    /// Sets the maximum number of ranged source `GetObject` requests in flight.
    pub fn with_source_get_concurrency(mut self, source_get_concurrency: usize) -> Self {
        self.source_get_concurrency = source_get_concurrency;
        self
    }

    /// Computes the adaptive source block window capacity.
    pub fn capacity(self) -> usize {
        let Some(available_memory_bytes) = self.available_memory_mb.checked_mul(1024 * 1024) else {
            return usize::try_from(self.source_zip_bytes).unwrap_or(usize::MAX);
        };
        let concurrency = u64::try_from(self.concurrency.max(1)).unwrap_or(u64::MAX);
        let zip_file_count = u64::try_from(self.zip_file_count).unwrap_or(u64::MAX);
        let worker_budget = concurrency.saturating_mul(ADAPTIVE_CACHE_WORKER_OVERHEAD);
        let file_budget = zip_file_count.saturating_mul(ADAPTIVE_CACHE_FILE_OVERHEAD);
        let in_flight_budget = u64::try_from(self.source_get_concurrency.max(1))
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(self.source_block_size).unwrap_or(u64::MAX));
        let reserved = ADAPTIVE_CACHE_BASE_OVERHEAD
            .saturating_add(worker_budget)
            .saturating_add(file_budget)
            .saturating_add(in_flight_budget);
        let capacity = available_memory_bytes
            .saturating_sub(reserved)
            .min(self.source_zip_bytes);
        let capacity = if capacity > ADAPTIVE_CACHE_LARGE_THRESHOLD {
            capacity.saturating_sub(ADAPTIVE_CACHE_LARGE_RSS_SLACK)
        } else {
            capacity
        }
        .min(ADAPTIVE_CACHE_MAX_WINDOW_CAPACITY);

        let minimum_block_capacity = u64::try_from(self.source_block_size.max(1))
            .unwrap_or(u64::MAX)
            .min(self.source_zip_bytes);
        let capacity = capacity.max(minimum_block_capacity);

        usize::try_from(capacity).unwrap_or(usize::MAX)
    }
}

/// Options for zipping a local directory and uploading it as an S3 object.
#[derive(Clone)]
pub struct UploadOptions {
    /// Local directory whose regular files and empty directories should be included recursively.
    pub(crate) source_dir: PathBuf,
    /// Destination ZIP object.
    pub(crate) destination: S3Object,
    /// Include the embedded update catalog at [`crate::EMBEDDED_CATALOG_PATH`].
    pub(crate) include_catalog: bool,
    /// Compression method for regular file entries.
    pub(crate) compression: ZipCompression,
    /// Buffer size used when streaming the ZIP body to S3.
    ///
    /// Must be greater than zero and no larger than 16 MiB.
    pub(crate) body_chunk_size: usize,
    /// Capacity of the in-memory pipe between ZIP production and S3 upload.
    ///
    /// Must be greater than zero and no larger than 64 MiB.
    pub(crate) pipe_capacity: usize,
    /// Optional progress callback invoked during upload preparation and ZIP streaming.
    pub(crate) progress: Option<UploadProgressHandler>,
}

/// Options for zipping a local directory to a local ZIP file.
#[derive(Clone)]
pub struct LocalZipOptions {
    /// Local directory whose regular files and empty directories should be included recursively.
    pub(crate) source_dir: PathBuf,
    /// Destination ZIP file path.
    pub(crate) destination_zip: PathBuf,
    /// Include the embedded update catalog at [`crate::EMBEDDED_CATALOG_PATH`].
    pub(crate) include_catalog: bool,
    /// Compression method for regular file entries.
    pub(crate) compression: ZipCompression,
    /// Optional progress callback invoked during upload preparation and ZIP streaming.
    pub(crate) progress: Option<UploadProgressHandler>,
}

/// Options for zipping an S3 prefix and uploading it as an S3 ZIP object.
#[derive(Clone)]
pub struct S3PrefixUploadOptions {
    /// Source prefix whose objects should be included recursively.
    pub(crate) source: S3Prefix,
    /// Destination ZIP object.
    pub(crate) destination: S3Object,
    /// Include the embedded update catalog at [`crate::EMBEDDED_CATALOG_PATH`].
    pub(crate) include_catalog: bool,
    /// Compression method for regular file entries.
    pub(crate) compression: ZipCompression,
    /// Buffer size used when streaming the ZIP body to S3.
    ///
    /// Must be greater than zero and no larger than 16 MiB.
    pub(crate) body_chunk_size: usize,
    /// Capacity of the in-memory pipe between ZIP production and S3 upload.
    ///
    /// Must be greater than zero and no larger than 64 MiB.
    pub(crate) pipe_capacity: usize,
    /// Optional progress callback invoked during source listing and ZIP streaming.
    pub(crate) progress: Option<UploadProgressHandler>,
}

/// Options for zipping an S3 prefix to a local ZIP file.
#[derive(Clone)]
pub struct S3PrefixLocalZipOptions {
    /// Source prefix whose objects should be included recursively.
    pub(crate) source: S3Prefix,
    /// Destination ZIP file path.
    pub(crate) destination_zip: PathBuf,
    /// Include the embedded update catalog at [`crate::EMBEDDED_CATALOG_PATH`].
    pub(crate) include_catalog: bool,
    /// Compression method for regular file entries.
    pub(crate) compression: ZipCompression,
    /// Optional progress callback invoked during source listing and ZIP streaming.
    pub(crate) progress: Option<UploadProgressHandler>,
}

/// Options for extracting a local ZIP file into an S3 prefix.
#[derive(Clone)]
pub struct LocalZipSyncOptions {
    /// Source ZIP file path.
    pub(crate) source_zip: PathBuf,
    /// Destination prefix that receives ZIP entries.
    pub(crate) destination: S3Prefix,
    /// How destination objects outside the ZIP are handled.
    ///
    /// Deleting extra objects requires a non-empty destination prefix so a
    /// bucket root is never treated as a sync deletion scope.
    pub(crate) cleanup: DestinationCleanup,
    /// ZIP entry selection. Empty selection extracts every supported entry.
    ///
    /// Selection cannot be combined with [`DestinationCleanup::DeleteExtra`].
    pub(crate) selection: UnzipSelection,
    /// Comparison policy for embedded catalogs and entry hashing.
    pub(crate) comparison: ComparisonMode,
    /// Collect one operation record per processed object in the returned report.
    pub(crate) collect_operations: bool,
    /// Maximum number of ZIP entries processed concurrently.
    pub(crate) concurrency: usize,
    /// Buffer size used when streaming entry bodies to S3.
    pub(crate) body_chunk_size: usize,
    /// Capacity of the in-memory pipe between decompression and S3 upload.
    pub(crate) pipe_capacity: usize,
}

/// Options for extracting an S3 ZIP object into a local directory.
#[derive(Clone)]
pub struct S3ZipLocalUnzipOptions {
    /// Source ZIP object.
    pub(crate) source: S3Object,
    /// Destination local directory.
    pub(crate) destination_dir: PathBuf,
    /// ZIP entry selection. Empty selection extracts every supported entry.
    pub(crate) selection: UnzipSelection,
    /// Collect source scheduler diagnostics in the returned report.
    pub(crate) collect_diagnostics: bool,
    /// Comparison policy for embedded catalogs and entry hashing.
    pub(crate) comparison: ComparisonMode,
    /// Collect one operation record per processed entry in the returned report.
    pub(crate) collect_operations: bool,
    /// Maximum number of ZIP entries processed concurrently.
    pub(crate) concurrency: usize,
    /// Maximum size for planned source ZIP blocks.
    pub(crate) source_block_size: usize,
    /// Maximum gap that can be read while coalescing adjacent source spans.
    pub(crate) source_block_merge_gap: usize,
    /// Maximum number of ranged source `GetObject` requests in flight.
    pub(crate) source_get_concurrency: usize,
    /// Maximum bytes held by the planned source block window.
    pub(crate) source_window_capacity: usize,
    /// Available memory budget, in MiB, used to derive the source block window.
    pub(crate) source_window_memory_budget_mb: Option<u64>,
}

/// Options for extracting a local ZIP file into a local directory.
#[derive(Clone)]
pub struct LocalUnzipOptions {
    /// Source ZIP file path.
    pub(crate) source_zip: PathBuf,
    /// Destination local directory.
    pub(crate) destination_dir: PathBuf,
    /// ZIP entry selection. Empty selection extracts every supported entry.
    pub(crate) selection: UnzipSelection,
    /// Comparison policy for embedded catalogs and entry hashing.
    pub(crate) comparison: ComparisonMode,
    /// Collect one operation record per processed entry in the returned report.
    pub(crate) collect_operations: bool,
    /// Maximum number of ZIP entries processed concurrently.
    pub(crate) concurrency: usize,
}

impl S3PrefixUploadOptions {
    /// Creates upload options for an S3 source prefix and destination object.
    pub fn new(source: S3Prefix, destination: S3Object) -> Self {
        Self {
            source,
            destination,
            include_catalog: true,
            compression: ZipCompression::Deflate,
            body_chunk_size: DEFAULT_BODY_CHUNK_SIZE,
            pipe_capacity: DEFAULT_PIPE_CAPACITY,
            progress: None,
        }
    }

    /// Omits the embedded update catalog from the ZIP.
    pub fn without_catalog(mut self) -> Self {
        self.include_catalog = false;
        self
    }

    /// Sets the compression method used for regular file entries.
    pub fn with_compression(mut self, compression: ZipCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the buffer size used when streaming the ZIP body to S3.
    pub fn with_body_chunk_size(mut self, body_chunk_size: usize) -> Self {
        self.body_chunk_size = body_chunk_size;
        self
    }

    /// Sets the in-memory pipe capacity between ZIP production and S3 upload.
    pub fn with_pipe_capacity(mut self, pipe_capacity: usize) -> Self {
        self.pipe_capacity = pipe_capacity;
        self
    }

    /// Sets the progress callback invoked during source listing and ZIP streaming.
    pub fn with_progress(self, callback: impl Fn(UploadProgress) + Send + Sync + 'static) -> Self {
        self.with_progress_handler(UploadProgressHandler::new(callback))
    }

    /// Sets the progress handler invoked during source listing and ZIP streaming.
    pub fn with_progress_handler(mut self, progress: UploadProgressHandler) -> Self {
        self.progress = Some(progress);
        self
    }
}

impl LocalZipOptions {
    /// Creates options for a local source directory and local destination ZIP.
    pub fn new(source_dir: impl Into<PathBuf>, destination_zip: impl Into<PathBuf>) -> Self {
        Self {
            source_dir: source_dir.into(),
            destination_zip: destination_zip.into(),
            include_catalog: true,
            compression: ZipCompression::Deflate,
            progress: None,
        }
    }

    /// Omits the embedded update catalog from the ZIP.
    pub fn without_catalog(mut self) -> Self {
        self.include_catalog = false;
        self
    }

    /// Sets the compression method used for regular file entries.
    pub fn with_compression(mut self, compression: ZipCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the progress callback invoked during upload preparation and ZIP streaming.
    pub fn with_progress(self, callback: impl Fn(UploadProgress) + Send + Sync + 'static) -> Self {
        self.with_progress_handler(UploadProgressHandler::new(callback))
    }

    /// Sets the progress handler invoked during upload preparation and ZIP streaming.
    pub fn with_progress_handler(mut self, progress: UploadProgressHandler) -> Self {
        self.progress = Some(progress);
        self
    }
}

impl std::fmt::Debug for LocalZipOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalZipOptions")
            .field("source_dir", &self.source_dir)
            .field("destination_zip", &self.destination_zip)
            .field("include_catalog", &self.include_catalog)
            .field("compression", &self.compression)
            .field(
                "progress",
                &self.progress.as_ref().map(|_| "UploadProgressHandler"),
            )
            .finish()
    }
}

impl std::fmt::Debug for S3PrefixUploadOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3PrefixUploadOptions")
            .field("source", &self.source)
            .field("destination", &self.destination)
            .field("include_catalog", &self.include_catalog)
            .field("compression", &self.compression)
            .field("body_chunk_size", &self.body_chunk_size)
            .field("pipe_capacity", &self.pipe_capacity)
            .field(
                "progress",
                &self.progress.as_ref().map(|_| "UploadProgressHandler"),
            )
            .finish()
    }
}

impl S3PrefixLocalZipOptions {
    /// Creates options for an S3 source prefix and local destination ZIP.
    pub fn new(source: S3Prefix, destination_zip: impl Into<PathBuf>) -> Self {
        Self {
            source,
            destination_zip: destination_zip.into(),
            include_catalog: true,
            compression: ZipCompression::Deflate,
            progress: None,
        }
    }

    /// Omits the embedded update catalog from the ZIP.
    pub fn without_catalog(mut self) -> Self {
        self.include_catalog = false;
        self
    }

    /// Sets the compression method used for regular file entries.
    pub fn with_compression(mut self, compression: ZipCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the progress callback invoked during source listing and ZIP streaming.
    pub fn with_progress(self, callback: impl Fn(UploadProgress) + Send + Sync + 'static) -> Self {
        self.with_progress_handler(UploadProgressHandler::new(callback))
    }

    /// Sets the progress handler invoked during source listing and ZIP streaming.
    pub fn with_progress_handler(mut self, progress: UploadProgressHandler) -> Self {
        self.progress = Some(progress);
        self
    }
}

impl std::fmt::Debug for S3PrefixLocalZipOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3PrefixLocalZipOptions")
            .field("source", &self.source)
            .field("destination_zip", &self.destination_zip)
            .field("include_catalog", &self.include_catalog)
            .field("compression", &self.compression)
            .field(
                "progress",
                &self.progress.as_ref().map(|_| "UploadProgressHandler"),
            )
            .finish()
    }
}

impl UploadOptions {
    /// Creates upload options for a local source directory and destination object.
    pub fn new(source_dir: impl Into<PathBuf>, destination: S3Object) -> Self {
        Self {
            source_dir: source_dir.into(),
            destination,
            include_catalog: true,
            compression: ZipCompression::Deflate,
            body_chunk_size: DEFAULT_BODY_CHUNK_SIZE,
            pipe_capacity: DEFAULT_PIPE_CAPACITY,
            progress: None,
        }
    }

    /// Omits the embedded update catalog from the ZIP.
    pub fn without_catalog(mut self) -> Self {
        self.include_catalog = false;
        self
    }

    /// Sets the compression method used for regular file entries.
    pub fn with_compression(mut self, compression: ZipCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the buffer size used when streaming the ZIP body to S3.
    pub fn with_body_chunk_size(mut self, body_chunk_size: usize) -> Self {
        self.body_chunk_size = body_chunk_size;
        self
    }

    /// Sets the in-memory pipe capacity between ZIP production and S3 upload.
    pub fn with_pipe_capacity(mut self, pipe_capacity: usize) -> Self {
        self.pipe_capacity = pipe_capacity;
        self
    }

    /// Sets the progress callback invoked during upload preparation and ZIP streaming.
    pub fn with_progress(self, callback: impl Fn(UploadProgress) + Send + Sync + 'static) -> Self {
        self.with_progress_handler(UploadProgressHandler::new(callback))
    }

    /// Sets the progress handler invoked during upload preparation and ZIP streaming.
    pub fn with_progress_handler(mut self, progress: UploadProgressHandler) -> Self {
        self.progress = Some(progress);
        self
    }
}

impl LocalZipSyncOptions {
    /// Creates extract options for a local source ZIP file and destination prefix.
    pub fn new(source_zip: impl Into<PathBuf>, destination: S3Prefix) -> Self {
        Self {
            source_zip: source_zip.into(),
            destination,
            cleanup: DestinationCleanup::default(),
            selection: UnzipSelection::default(),
            comparison: ComparisonMode::default(),
            collect_operations: true,
            concurrency: DEFAULT_CONCURRENCY,
            body_chunk_size: DEFAULT_BODY_CHUNK_SIZE,
            pipe_capacity: DEFAULT_PIPE_CAPACITY,
        }
    }

    /// Sets ZIP entry selection patterns.
    pub fn with_selection(mut self, selection: impl Into<UnzipSelection>) -> Self {
        self.selection = selection.into();
        self
    }

    /// Deletes destination objects under the prefix that are not present in the ZIP.
    ///
    /// This requires a non-empty destination prefix and cannot be combined with
    /// a non-empty selection.
    pub fn delete_extra_objects(mut self) -> Self {
        self.cleanup = DestinationCleanup::DeleteExtra;
        self
    }

    /// Sets the destination cleanup policy.
    pub fn with_cleanup(mut self, cleanup: DestinationCleanup) -> Self {
        self.cleanup = cleanup;
        self
    }

    /// Ignores any embedded catalog and hashes ZIP entries for comparison.
    pub fn force_hash_comparison(mut self) -> Self {
        self.comparison = ComparisonMode::HashEntries;
        self
    }

    /// Sets the ZIP entry comparison policy.
    pub fn with_comparison_mode(mut self, comparison: ComparisonMode) -> Self {
        self.comparison = comparison;
        self
    }

    /// Omits per-object operation records from the returned report.
    pub fn without_operations(mut self) -> Self {
        self.collect_operations = false;
        self
    }

    /// Sets the maximum number of ZIP entries processed concurrently.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the buffer size used when streaming entry bodies to S3.
    pub fn with_body_chunk_size(mut self, body_chunk_size: usize) -> Self {
        self.body_chunk_size = body_chunk_size;
        self
    }

    /// Sets the in-memory pipe capacity between decompression and S3 upload.
    pub fn with_pipe_capacity(mut self, pipe_capacity: usize) -> Self {
        self.pipe_capacity = pipe_capacity;
        self
    }
}

impl std::fmt::Debug for LocalZipSyncOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalZipSyncOptions")
            .field("source_zip", &self.source_zip)
            .field("destination", &self.destination)
            .field("cleanup", &self.cleanup)
            .field("selection", &self.selection)
            .field("comparison", &self.comparison)
            .field("collect_operations", &self.collect_operations)
            .field("concurrency", &self.concurrency)
            .field("body_chunk_size", &self.body_chunk_size)
            .field("pipe_capacity", &self.pipe_capacity)
            .finish()
    }
}

impl S3ZipLocalUnzipOptions {
    /// Creates extract options for a source ZIP object and local destination directory.
    pub fn new(source: S3Object, destination_dir: impl Into<PathBuf>) -> Self {
        Self {
            source,
            destination_dir: destination_dir.into(),
            selection: UnzipSelection::default(),
            collect_diagnostics: false,
            comparison: ComparisonMode::default(),
            collect_operations: true,
            concurrency: DEFAULT_CONCURRENCY,
            source_block_size: DEFAULT_SOURCE_BLOCK_SIZE,
            source_block_merge_gap: DEFAULT_SOURCE_BLOCK_MERGE_GAP,
            source_get_concurrency: DEFAULT_SOURCE_GET_CONCURRENCY,
            source_window_capacity: DEFAULT_SOURCE_WINDOW_CAPACITY,
            source_window_memory_budget_mb: None,
        }
    }

    /// Sets ZIP entry selection patterns.
    pub fn with_selection(mut self, selection: impl Into<UnzipSelection>) -> Self {
        self.selection = selection.into();
        self
    }

    /// Collects source scheduler diagnostics in the returned report.
    pub fn collect_diagnostics(mut self) -> Self {
        self.collect_diagnostics = true;
        self
    }

    /// Ignores any embedded catalog and hashes ZIP entries for comparison.
    pub fn force_hash_comparison(mut self) -> Self {
        self.comparison = ComparisonMode::HashEntries;
        self
    }

    /// Sets the ZIP entry comparison policy.
    pub fn with_comparison_mode(mut self, comparison: ComparisonMode) -> Self {
        self.comparison = comparison;
        self
    }

    /// Omits per-entry operation records from the returned report.
    pub fn without_operations(mut self) -> Self {
        self.collect_operations = false;
        self
    }

    /// Sets the maximum number of ZIP entries processed concurrently.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the maximum size for planned source ZIP blocks.
    pub fn with_source_block_size(mut self, source_block_size: usize) -> Self {
        self.source_block_size = source_block_size;
        self
    }

    /// Sets the maximum gap that can be read while coalescing adjacent source spans.
    pub fn with_source_block_merge_gap(mut self, source_block_merge_gap: usize) -> Self {
        self.source_block_merge_gap = source_block_merge_gap;
        self
    }

    /// Sets the maximum number of ranged source `GetObject` requests in flight.
    pub fn with_source_get_concurrency(mut self, source_get_concurrency: usize) -> Self {
        self.source_get_concurrency = source_get_concurrency;
        self
    }

    /// Sets the maximum bytes held by the planned source block window.
    pub fn with_source_window_capacity(mut self, source_window_capacity: usize) -> Self {
        self.source_window_capacity = source_window_capacity;
        self
    }

    /// Sets the available memory budget, in MiB, used to derive the source block window.
    pub fn with_source_window_memory_budget_mb(
        mut self,
        source_window_memory_budget_mb: u64,
    ) -> Self {
        self.source_window_memory_budget_mb = Some(source_window_memory_budget_mb);
        self
    }
}

impl std::fmt::Debug for S3ZipLocalUnzipOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ZipLocalUnzipOptions")
            .field("source", &self.source)
            .field("destination_dir", &self.destination_dir)
            .field("selection", &self.selection)
            .field("collect_diagnostics", &self.collect_diagnostics)
            .field("comparison", &self.comparison)
            .field("collect_operations", &self.collect_operations)
            .field("concurrency", &self.concurrency)
            .field("source_block_size", &self.source_block_size)
            .field("source_block_merge_gap", &self.source_block_merge_gap)
            .field("source_get_concurrency", &self.source_get_concurrency)
            .field("source_window_capacity", &self.source_window_capacity)
            .field(
                "source_window_memory_budget_mb",
                &self.source_window_memory_budget_mb,
            )
            .finish()
    }
}

impl LocalUnzipOptions {
    /// Creates extract options for a source ZIP file and local destination directory.
    pub fn new(source_zip: impl Into<PathBuf>, destination_dir: impl Into<PathBuf>) -> Self {
        Self {
            source_zip: source_zip.into(),
            destination_dir: destination_dir.into(),
            selection: UnzipSelection::default(),
            comparison: ComparisonMode::default(),
            collect_operations: true,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Sets ZIP entry selection patterns.
    pub fn with_selection(mut self, selection: impl Into<UnzipSelection>) -> Self {
        self.selection = selection.into();
        self
    }

    /// Ignores any embedded catalog and hashes ZIP entries for comparison.
    pub fn force_hash_comparison(mut self) -> Self {
        self.comparison = ComparisonMode::HashEntries;
        self
    }

    /// Sets the ZIP entry comparison policy.
    pub fn with_comparison_mode(mut self, comparison: ComparisonMode) -> Self {
        self.comparison = comparison;
        self
    }

    /// Omits per-entry operation records from the returned report.
    pub fn without_operations(mut self) -> Self {
        self.collect_operations = false;
        self
    }

    /// Sets the maximum number of ZIP entries processed concurrently.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

impl std::fmt::Debug for LocalUnzipOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalUnzipOptions")
            .field("source_zip", &self.source_zip)
            .field("destination_dir", &self.destination_dir)
            .field("selection", &self.selection)
            .field("comparison", &self.comparison)
            .field("collect_operations", &self.collect_operations)
            .field("concurrency", &self.concurrency)
            .finish()
    }
}

impl std::fmt::Debug for UploadOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UploadOptions")
            .field("source_dir", &self.source_dir)
            .field("destination", &self.destination)
            .field("include_catalog", &self.include_catalog)
            .field("compression", &self.compression)
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
    /// The source has been scanned and the total entry count is known.
    Planned {
        /// Total number of files and preserved directory entries included in the ZIP.
        total_files: usize,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
    },
    /// A file or preserved directory entry has started streaming into the ZIP writer.
    FileStarted {
        /// One-based index of the entry currently being streamed.
        current_file: usize,
        /// Total number of files and preserved directory entries included in the ZIP.
        total_files: usize,
        /// Number of entries that have finished streaming into the ZIP.
        processed_files: usize,
        /// Uncompressed bytes that have finished streaming into the ZIP.
        processed_bytes: u64,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
        /// ZIP path of the entry that just started.
        path: String,
    },
    /// A file is still streaming and byte progress has advanced.
    FileProgress {
        /// One-based index of the entry currently being streamed.
        current_file: usize,
        /// Total number of files and preserved directory entries included in the ZIP.
        total_files: usize,
        /// Number of entries that have finished streaming into the ZIP.
        processed_files: usize,
        /// Uncompressed bytes that have streamed into the ZIP producer so far.
        processed_bytes: u64,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
        /// ZIP path of the file currently being streamed.
        path: String,
    },
    /// One file or preserved directory entry has finished streaming into the ZIP writer.
    FileFinished {
        /// Number of entries that have finished streaming into the ZIP.
        processed_files: usize,
        /// Total number of files and preserved directory entries included in the ZIP.
        total_files: usize,
        /// Uncompressed bytes that have finished streaming into the ZIP.
        processed_bytes: u64,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
        /// ZIP path of the entry that just finished.
        path: String,
    },
    /// ZIP production has finished writing into the upload pipe.
    ///
    /// S3 multipart upload completion may still be in progress when this event
    /// is emitted.
    Finished {
        /// Total number of files and preserved directory entries included in the ZIP.
        total_files: usize,
        /// Total uncompressed bytes across all files.
        total_bytes: u64,
    },
}
