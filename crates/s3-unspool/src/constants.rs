pub(crate) const DEFAULT_CONCURRENCY: usize = 64;
pub(crate) const DEFAULT_PUT_CONCURRENCY: usize = 16;
pub(crate) const DEFAULT_SOURCE_BLOCK_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_SOURCE_BLOCK_MERGE_GAP: usize = 256 * 1024;
pub(crate) const DEFAULT_SOURCE_GET_CONCURRENCY: usize = 4;
pub(crate) const DEFAULT_SOURCE_WINDOW_CAPACITY: usize = 64 * 1024 * 1024;
pub(crate) const ADAPTIVE_CACHE_BASE_OVERHEAD: u64 = 64 * 1024 * 1024;
pub(crate) const ADAPTIVE_CACHE_WORKER_OVERHEAD: u64 = 12 * 1024 * 1024;
pub(crate) const ADAPTIVE_CACHE_FILE_OVERHEAD: u64 = 2 * 1024;
pub(crate) const ADAPTIVE_CACHE_LARGE_THRESHOLD: u64 = 512 * 1024 * 1024;
pub(crate) const ADAPTIVE_CACHE_LARGE_RSS_SLACK: u64 = 384 * 1024 * 1024;
pub(crate) const ADAPTIVE_CACHE_MAX_WINDOW_CAPACITY: u64 = 512 * 1024 * 1024;
pub(crate) const ADAPTIVE_SOURCE_GET_MEMORY_STEP_MB: u64 = 256;
pub(crate) const ADAPTIVE_SOURCE_MAX_GET_CONCURRENCY: usize = 8;
pub(crate) const GET_OBJECT_MAX_ATTEMPTS: usize = 3;
pub(crate) const PUT_OBJECT_MAX_ATTEMPTS: usize = 6;
pub(crate) const PUT_OBJECT_RETRY_BASE_DELAY_MS: u64 = 250;
pub(crate) const PUT_OBJECT_RETRY_MAX_DELAY_MS: u64 = 5_000;
pub(crate) const PUT_OBJECT_SLOWDOWN_RETRY_BASE_DELAY_MS: u64 = 1_000;
pub(crate) const PUT_OBJECT_SLOWDOWN_RETRY_MAX_DELAY_MS: u64 = 30_000;
pub(crate) const DEFAULT_BODY_CHUNK_SIZE: usize = 256 * 1024;
pub(crate) const DEFAULT_PIPE_CAPACITY: usize = 1024 * 1024;
pub(crate) const MAX_BODY_CHUNK_SIZE: usize = 16 * 1024 * 1024;
pub(crate) const MAX_PIPE_CAPACITY: usize = 64 * 1024 * 1024;
pub(crate) const S3_SINGLE_PUT_LIMIT: u64 = 5 * 1024 * 1024 * 1024;
pub(crate) const EMBEDDED_CATALOG_VERSION: u32 = 1;
pub(crate) const EMBEDDED_CATALOG_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Reserved ZIP entry path used for the optional embedded update catalog.
///
/// ZIPs produced by [`crate::upload_directory_zip_to_s3`] include this entry by
/// default. It is consumed as metadata during extraction and is never written to
/// the destination prefix.
pub const EMBEDDED_CATALOG_PATH: &str = ".s3-unspool/catalog.v1.json";
