use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_zip::tokio::read::fs::ZipFileReader;
use async_zip::{Compression, StoredZipEntry};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use crc32fast::Hasher as Crc32Hasher;
use futures_util::TryStreamExt;
use futures_util::stream::{self, StreamExt};
use http_body::Frame;
use http_body_util::StreamBody;
use md5::{Digest, Md5};
use tokio::io::AsyncRead;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;
#[cfg(windows)]
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, INVALID_HANDLE_VALUE,
};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_CASE_SENSITIVE_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileCaseSensitiveInfo,
    GetFileInformationByHandleEx, OPEN_EXISTING,
};
#[cfg(windows)]
use windows_sys::Win32::System::SystemServices::FILE_CS_FLAG_CASE_SENSITIVE_DIR;

use crate::catalog::{EmbeddedCatalog, catalog_md5_by_path};
use crate::constants::{EMBEDDED_CATALOG_MAX_BYTES, EMBEDDED_CATALOG_PATH};
use crate::constants::{MAX_BODY_CHUNK_SIZE, MAX_PIPE_CAPACITY};
use crate::entry_reader::{EntryReader, entry_reader};
use crate::error::{Error, Result, aws_error_context, aws_error_message};
use crate::options::{
    LocalUnzipOptions, LocalZipSyncOptions, PutRetryPolicy, RetryJitter, S3ZipLocalUnzipOptions,
    SyncOptions, adaptive_source_window_capacity,
};
use crate::range::{
    BlockStore, SourceClient, SourceDiagnosticsCollector, plan_source_blocks,
    start_source_scheduler,
};
use crate::report::{
    DryRunDiagnostics, DryRunObjectReport, DryRunOperationStatus, LocalUnzipDiagnostics,
    LocalUnzipReport, LocalZipToS3Report, ObjectReport, OperationStatus, PutDiagnostics,
    PutRetryDiagnostics, SyncDiagnostics, SyncReport, SyncSummary, UnzipDryRunReport,
    UnzipDryRunSummary, summarize_dry_run_operation, summarize_operation,
};
use crate::s3_uri::{S3Prefix, normalize_etag};
use crate::source::head_source;
use crate::upload::{
    invalid_local_path, is_platform_path_alias_symlink, replace_temp_file, temp_sibling_path,
};
use crate::zip_manifest::{
    ManifestEntry, load_zip_manifest, normalize_zip_entry_path, validate_crc32_value,
};

const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);
const PUT_OBJECT_PRODUCER_ERROR_GRACE: Duration = Duration::from_millis(25);

#[derive(Clone, Debug)]
pub(crate) struct DestinationObject {
    pub(crate) etag: Option<String>,
    pub(crate) size: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExtractDigest {
    bytes: u64,
    md5: String,
}

#[derive(Debug)]
struct ExtractProgress {
    total_entries: usize,
    processed_entries: AtomicUsize,
    uploaded_new: AtomicUsize,
    uploaded_changed: AtomicUsize,
    skipped_unchanged: AtomicUsize,
    conditional_conflicts: AtomicUsize,
    errors: AtomicUsize,
}

#[derive(Debug, Default)]
struct PutDiagnosticsCollector {
    failed_attempts: AtomicU64,
    retry_attempts: AtomicU64,
    throttled_attempts: AtomicU64,
    throttle_waits: AtomicU64,
    throttle_wait_millis: AtomicU64,
    failures_by_error_code: Mutex<BTreeMap<String, u64>>,
}

impl PutDiagnosticsCollector {
    fn record_failure(&self, error_code: impl Into<String>) -> u64 {
        let count = self.failed_attempts.fetch_add(1, Ordering::Relaxed) + 1;
        let mut failures = self
            .failures_by_error_code
            .lock()
            .expect("put diagnostics mutex is not poisoned");
        *failures.entry(error_code.into()).or_default() += 1;
        count
    }

    fn record_retry(&self) {
        self.retry_attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_throttled_attempt(&self) {
        self.throttled_attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_throttle_wait(&self, duration: Duration) {
        self.throttle_waits.fetch_add(1, Ordering::Relaxed);
        self.throttle_wait_millis
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> PutDiagnostics {
        PutDiagnostics {
            failed_attempts: self.failed_attempts.load(Ordering::Relaxed),
            failures_by_error_code: self
                .failures_by_error_code
                .lock()
                .expect("put diagnostics mutex is not poisoned")
                .clone(),
            retry_attempts: self.retry_attempts.load(Ordering::Relaxed),
            throttled_attempts: self.throttled_attempts.load(Ordering::Relaxed),
            throttle_waits: self.throttle_waits.load(Ordering::Relaxed),
            throttle_wait_millis: self.throttle_wait_millis.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct PutThrottle {
    cooldown_until: Mutex<Option<Instant>>,
    diagnostics: Option<Arc<PutDiagnosticsCollector>>,
}

impl PutThrottle {
    fn new(diagnostics: Option<Arc<PutDiagnosticsCollector>>) -> Self {
        Self {
            cooldown_until: Mutex::new(None),
            diagnostics,
        }
    }

    async fn wait(&self) {
        loop {
            let delay = {
                let cooldown_until = self
                    .cooldown_until
                    .lock()
                    .expect("put throttle mutex is not poisoned");
                cooldown_until.and_then(|deadline| deadline.checked_duration_since(Instant::now()))
            };
            let Some(delay) = delay else {
                return;
            };
            if delay.is_zero() {
                return;
            }
            if let Some(diagnostics) = &self.diagnostics {
                diagnostics.record_throttle_wait(delay);
            }
            tokio::time::sleep(delay).await;
        }
    }

    fn throttle(&self, delay: Duration) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record_throttled_attempt();
        }
        if delay.is_zero() {
            return;
        }
        let deadline = Instant::now() + delay;
        let mut cooldown_until = self
            .cooldown_until
            .lock()
            .expect("put throttle mutex is not poisoned");
        if cooldown_until.is_none_or(|current| deadline > current) {
            *cooldown_until = Some(deadline);
        }
    }
}

impl ExtractProgress {
    fn new(total_entries: usize) -> Self {
        Self {
            total_entries,
            processed_entries: AtomicUsize::new(0),
            uploaded_new: AtomicUsize::new(0),
            uploaded_changed: AtomicUsize::new(0),
            skipped_unchanged: AtomicUsize::new(0),
            conditional_conflicts: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
        }
    }

    fn record_operation(&self, operation: &ObjectReport) {
        self.processed_entries.fetch_add(1, Ordering::Relaxed);
        match operation.status {
            OperationStatus::UploadedNew => {
                self.uploaded_new.fetch_add(1, Ordering::Relaxed);
            }
            OperationStatus::UploadedChanged => {
                self.uploaded_changed.fetch_add(1, Ordering::Relaxed);
            }
            OperationStatus::SkippedUnchanged => {
                self.skipped_unchanged.fetch_add(1, Ordering::Relaxed);
            }
            OperationStatus::ConditionalConflict => {
                self.conditional_conflicts.fetch_add(1, Ordering::Relaxed);
            }
            OperationStatus::DeletedExtra => {}
            OperationStatus::Error => {
                self.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn log_progress(
        &self,
        source_diagnostics: Option<&SourceDiagnosticsCollector>,
        put_diagnostics: Option<&PutDiagnosticsCollector>,
        elapsed: Duration,
        message: &'static str,
    ) {
        let processed_entries = self.processed_entries.load(Ordering::Relaxed);
        let uploaded_new = self.uploaded_new.load(Ordering::Relaxed);
        let uploaded_changed = self.uploaded_changed.load(Ordering::Relaxed);
        let skipped_unchanged = self.skipped_unchanged.load(Ordering::Relaxed);
        let conditional_conflicts = self.conditional_conflicts.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);

        if let Some(diagnostics) = source_diagnostics {
            let source = diagnostics.snapshot();
            let put = put_diagnostics
                .map(PutDiagnosticsCollector::snapshot)
                .unwrap_or_default();
            tracing::info!(
                elapsed_ms = elapsed.as_millis() as u64,
                processed_entries,
                total_entries = self.total_entries,
                uploaded_new,
                uploaded_changed,
                skipped_unchanged,
                conditional_conflicts,
                errors,
                source_get_attempts = source.source_get_attempts,
                source_get_retries = source.source_get_retries,
                source_get_request_errors = source.source_get_request_errors,
                source_get_body_errors = source.source_get_body_errors,
                source_get_short_body_errors = source.source_get_short_body_errors,
                planned_blocks = source.planned_blocks,
                fetched_blocks = source.fetched_blocks,
                block_hits = source.block_hits,
                block_waits = source.block_waits,
                block_releases = source.block_releases,
                block_misses = source.block_misses,
                block_refetches = source.block_refetches,
                active_gets = diagnostics.active_gets(),
                active_gets_high_water = source.active_gets_high_water,
                source_amplification = source.source_amplification,
                put_failed_attempts = put.failed_attempts,
                put_retry_attempts = put.retry_attempts,
                put_throttled_attempts = put.throttled_attempts,
                put_throttle_waits = put.throttle_waits,
                put_throttle_wait_millis = put.throttle_wait_millis,
                put_failures_by_error_code = ?put.failures_by_error_code,
                "{message}"
            );
        } else {
            tracing::info!(
                elapsed_ms = elapsed.as_millis() as u64,
                processed_entries,
                total_entries = self.total_entries,
                uploaded_new,
                uploaded_changed,
                skipped_unchanged,
                conditional_conflicts,
                errors,
                "{message}"
            );
        }
    }
}

fn start_progress_logger(
    progress: Arc<ExtractProgress>,
    source_diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
    put_diagnostics: Option<Arc<PutDiagnosticsCollector>>,
) -> Option<JoinHandle<()>> {
    tracing::enabled!(tracing::Level::INFO).then(|| {
        let started = Instant::now();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(PROGRESS_LOG_INTERVAL).await;
                progress.log_progress(
                    source_diagnostics.as_deref(),
                    put_diagnostics.as_deref(),
                    started.elapsed(),
                    "entry processing progress",
                );
            }
        })
    })
}

async fn stop_progress_logger(progress_task: Option<JoinHandle<()>>) {
    if let Some(task) = progress_task {
        task.abort();
        let _ = task.await;
    }
}

/// Extracts missing or changed files from an S3 ZIP object into an S3 prefix.
///
/// The source ZIP is read with ranged `GetObject` requests. Destination objects
/// are listed once, compared by key and ETag, and written with conditional
/// `PutObject` requests.
///
/// See [`SyncOptions`] for tuning and behavior controls.
pub async fn sync_zip_to_s3(client: &Client, options: SyncOptions) -> Result<SyncReport> {
    sync_zip_to_s3_with_clients(client, client, options).await
}

/// Extracts missing or changed files from an S3 ZIP object into an S3 prefix,
/// using separate S3 clients for source reads and destination writes.
///
/// This is useful for long-running Lambda or service processes that want
/// independent HTTP pools for ranged source `GetObject` calls and streaming
/// destination `PutObject` calls. For most callers, [`sync_zip_to_s3`] is the
/// simpler entry point.
pub async fn sync_zip_to_s3_with_clients(
    source_client: &Client,
    destination_client: &Client,
    mut options: SyncOptions,
) -> Result<SyncReport> {
    validate_options(&options)?;
    let started = Instant::now();
    tracing::info!(
        source_bucket = %options.source.bucket,
        source_key = %options.source.key,
        destination_bucket = %options.destination.bucket,
        destination_prefix = %options.destination.prefix,
        delete_extra = options.delete_extra,
        ignore_embedded_catalog = options.ignore_embedded_catalog,
        collect_diagnostics = options.collect_diagnostics,
        collect_operations = options.collect_operations,
        fail_on_conditional_conflict = options.fail_on_conditional_conflict,
        concurrency = options.concurrency,
        source_block_size = options.source_block_size,
        source_block_merge_gap = options.source_block_merge_gap,
        source_get_concurrency = options.source_get_concurrency,
        source_window_capacity = options.source_window_capacity,
        source_window_memory_budget_mb = ?options.source_window_memory_budget_mb,
        put_concurrency = options.put_concurrency,
        put_max_attempts = options.put_retry_policy.max_attempts,
        put_base_delay_ms = duration_millis_u64(options.put_retry_policy.base_delay),
        put_max_delay_ms = duration_millis_u64(options.put_retry_policy.max_delay),
        put_slowdown_base_delay_ms = duration_millis_u64(options.put_retry_policy.slowdown_base_delay),
        put_slowdown_max_delay_ms = duration_millis_u64(options.put_retry_policy.slowdown_max_delay),
        put_jitter = ?options.put_retry_policy.jitter,
        body_chunk_size = options.body_chunk_size,
        pipe_capacity = options.pipe_capacity,
        "s3 zip sync started"
    );

    let source_head = head_source(source_client, &options.source).await?;
    tracing::info!(
        source_bucket = %options.source.bucket,
        source_key = %options.source.key,
        source_zip_bytes = source_head.len,
        source_etag = ?source_head.etag.as_deref(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "source object metadata loaded"
    );
    let diagnostics = options
        .collect_diagnostics
        .then(|| Arc::new(SourceDiagnosticsCollector::new(source_head.len)));
    let put_diagnostics = options
        .collect_diagnostics
        .then(|| Arc::new(PutDiagnosticsCollector::default()));
    let source = Arc::new(SourceClient {
        client: source_client.clone(),
        bucket: options.source.bucket.clone(),
        key: options.source.key.clone(),
        len: source_head.len,
        etag: source_head.etag,
        diagnostics: diagnostics.clone(),
    });

    let manifest = load_zip_manifest(
        Arc::clone(&source),
        &options.destination,
        options.ignore_embedded_catalog,
        options.source_block_size,
        Some(crate::constants::S3_SINGLE_PUT_LIMIT),
    )
    .await?;
    resolve_source_window_capacity(&mut options, source_head.len, manifest.entries.len());
    validate_source_range_options(&options, source_head.len)?;
    let entries_with_catalog_md5 = manifest
        .entries
        .iter()
        .filter(|entry| entry.catalog_md5.is_some())
        .count();
    tracing::info!(
        zip_files = manifest.entries.len(),
        entries_with_catalog_md5,
        source_window_capacity = options.source_window_capacity,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "zip manifest loaded"
    );
    let destination_objects = list_destination(destination_client, &options.destination).await?;
    tracing::info!(
        destination_bucket = %options.destination.bucket,
        destination_prefix = %options.destination.prefix,
        destination_objects = destination_objects.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "destination prefix listed"
    );
    let entries = manifest.entries;
    let total_entries = entries.len();
    let expected_keys = options.delete_extra.then(|| {
        entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<HashSet<_>>()
    });
    let classified = classify_entries(entries, &destination_objects);

    let mut summary = SyncSummary {
        zip_files: total_entries,
        destination_objects: destination_objects.len(),
        ..SyncSummary::default()
    };
    let mut operations = Vec::new();
    let mut fail_fast_error = None;
    let progress = Arc::new(ExtractProgress::new(total_entries));
    tracing::info!(
        zip_files = total_entries,
        concurrency = options.concurrency,
        skipped_without_source = classified.reports.len(),
        hash_jobs = classified.hash_jobs.len(),
        upload_jobs = classified.upload_jobs.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "entry processing started"
    );
    let progress_task = start_progress_logger(
        Arc::clone(&progress),
        diagnostics.clone(),
        put_diagnostics.clone(),
    );

    for operation in classified.reports {
        record_operation(
            &mut summary,
            &mut operations,
            &progress,
            &options,
            operation,
            &mut fail_fast_error,
            true,
        );
        if fail_fast_error.is_some() {
            break;
        }
    }

    let mut upload_jobs = classified.upload_jobs;
    if fail_fast_error.is_none() && !classified.hash_jobs.is_empty() {
        let hash_results = run_hash_phase(
            Arc::clone(&source),
            classified.hash_jobs,
            &options,
            source_head.len,
            diagnostics.clone(),
        )
        .await;
        for result in hash_results {
            match result {
                HashPhaseResult::Operation(operation) => {
                    record_operation(
                        &mut summary,
                        &mut operations,
                        &progress,
                        &options,
                        operation,
                        &mut fail_fast_error,
                        true,
                    );
                }
                HashPhaseResult::Upload(job) => upload_jobs.push(job),
            }
            if fail_fast_error.is_some() {
                break;
            }
        }
    }

    if fail_fast_error.is_none() && !upload_jobs.is_empty() {
        let upload_results = run_upload_phase(
            destination_client.clone(),
            Arc::clone(&source),
            upload_jobs,
            &options,
            source_head.len,
            PhaseObservers {
                source_diagnostics: diagnostics.clone(),
                put_diagnostics: put_diagnostics.clone(),
                progress: Arc::clone(&progress),
            },
        )
        .await;
        for operation in upload_results {
            record_operation(
                &mut summary,
                &mut operations,
                &progress,
                &options,
                operation,
                &mut fail_fast_error,
                false,
            );
            if fail_fast_error.is_some() {
                break;
            }
        }
    }

    stop_progress_logger(progress_task).await;
    progress.log_progress(
        diagnostics.as_deref(),
        put_diagnostics.as_deref(),
        started.elapsed(),
        "entry processing completed",
    );

    if let Some(err) = fail_fast_error {
        tracing::warn!(
            error = %err,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "s3 zip sync stopped by fail-fast conditional conflict"
        );
        return Err(err);
    }

    if options.delete_extra {
        let expected_keys = expected_keys.expect("delete-extra expected keys are prepared");
        let extras = destination_objects
            .keys()
            .filter(|key| !expected_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        tracing::info!(
            extra_objects = extras.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "delete-extra processing started"
        );
        let delete_reports =
            delete_extra_objects(destination_client, &options.destination, extras).await;
        for operation in delete_reports {
            summarize_operation(&mut summary, &operation);
            if options.collect_operations {
                operations.push(operation);
            }
        }
    }

    let source_diagnostics = diagnostics
        .as_ref()
        .map(|diagnostics| diagnostics.snapshot());
    let put_diagnostics = put_diagnostics
        .as_ref()
        .map(|diagnostics| diagnostics.snapshot());
    if let Some(source) = &source_diagnostics {
        let put = put_diagnostics.clone().unwrap_or_default();
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            zip_files = summary.zip_files,
            uploaded_new = summary.uploaded_new,
            uploaded_changed = summary.uploaded_changed,
            skipped_unchanged = summary.skipped_unchanged,
            conditional_conflicts = summary.conditional_conflicts,
            deleted_extra = summary.deleted_extra,
            errors = summary.errors,
            source_get_attempts = source.source_get_attempts,
            source_get_retries = source.source_get_retries,
            source_get_errors = source.source_get_errors,
            planned_blocks = source.planned_blocks,
            fetched_blocks = source.fetched_blocks,
            fetched_source_bytes = source.fetched_source_bytes,
            source_amplification = source.source_amplification,
            active_gets_high_water = source.active_gets_high_water,
            put_failed_attempts = put.failed_attempts,
            put_retry_attempts = put.retry_attempts,
            put_throttled_attempts = put.throttled_attempts,
            put_throttle_waits = put.throttle_waits,
            put_throttle_wait_millis = put.throttle_wait_millis,
            put_failures_by_error_code = ?put.failures_by_error_code,
            "s3 zip sync completed"
        );
    } else {
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            zip_files = summary.zip_files,
            uploaded_new = summary.uploaded_new,
            uploaded_changed = summary.uploaded_changed,
            skipped_unchanged = summary.skipped_unchanged,
            conditional_conflicts = summary.conditional_conflicts,
            deleted_extra = summary.deleted_extra,
            errors = summary.errors,
            "s3 zip sync completed"
        );
    }

    let report = SyncReport {
        source: options.source,
        destination: options.destination,
        summary,
        diagnostics: source_diagnostics.map(|source| SyncDiagnostics {
            concurrency: options.concurrency,
            put_concurrency: options.put_concurrency,
            put_retry: put_retry_diagnostics(&options.put_retry_policy),
            source_block_size: options.source_block_size,
            source_block_merge_gap: options.source_block_merge_gap,
            source_get_concurrency: options.source_get_concurrency,
            source_window_capacity: options.source_window_capacity,
            source,
            put: put_diagnostics.unwrap_or_default(),
        }),
        operations,
    };
    Ok(report)
}

/// Extracts a local ZIP file into an S3 prefix.
///
/// Entries are streamed from the local ZIP file and written to S3 with
/// conditional `PutObject` requests after listing the destination prefix.
pub async fn unzip_file_to_s3(
    client: &Client,
    options: LocalZipSyncOptions,
) -> Result<LocalZipToS3Report> {
    validate_local_zip_sync_options(&options)?;
    let reader = open_local_zip_reader(&options.source_zip).await?;
    let source_len = tokio::fs::metadata(&options.source_zip).await?.len();
    let mut entries = local_zip_entries_for_s3(
        reader.file().entries(),
        &options.destination,
        source_len,
        true,
    )?;
    if !options.ignore_embedded_catalog {
        let catalog = load_local_embedded_catalog(&reader, entries.catalog_index).await;
        apply_local_catalog(&mut entries.entries, catalog);
    }

    let destination_objects = list_destination(client, &options.destination).await?;
    let expected_keys = options.delete_extra.then(|| {
        entries
            .entries
            .iter()
            .map(|entry| entry.destination.clone())
            .collect::<HashSet<_>>()
    });
    let total_entries = entries.entries.len();
    let mut summary = SyncSummary {
        zip_files: total_entries,
        destination_objects: destination_objects.len(),
        ..SyncSummary::default()
    };
    let mut operations = Vec::new();

    let mut stream = stream::iter(entries.entries)
        .map(|entry| {
            unzip_local_entry_to_s3(client, &reader, entry, &destination_objects, &options)
        })
        .buffer_unordered(options.concurrency);

    while let Some(operation) = stream.next().await {
        summarize_operation(&mut summary, &operation);
        if options.collect_operations {
            operations.push(operation);
        }
    }
    drop(stream);

    if options.delete_extra {
        let expected_keys = expected_keys.expect("delete-extra expected keys are prepared");
        let extras = destination_objects
            .keys()
            .filter(|key| !expected_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for operation in delete_extra_objects(client, &options.destination, extras).await {
            summarize_operation(&mut summary, &operation);
            if options.collect_operations {
                operations.push(operation);
            }
        }
    }

    Ok(LocalZipToS3Report {
        source_zip: options.source_zip.display().to_string(),
        destination: options.destination,
        summary,
        operations,
    })
}

/// Extracts an S3 ZIP object into a local directory.
///
/// Existing local files are overwritten, ZIP directory entries are created as
/// local directories, and extra local files are left untouched.
pub async fn unzip_s3_zip_to_local(
    client: &Client,
    mut options: S3ZipLocalUnzipOptions,
) -> Result<LocalUnzipReport> {
    validate_s3_zip_local_unzip_options(&options)?;
    let destination_root = prepare_local_destination_root(&options.destination_dir).await?;
    let started = Instant::now();
    let source_head = head_source(client, &options.source).await?;
    let diagnostics = options
        .collect_diagnostics
        .then(|| Arc::new(SourceDiagnosticsCollector::new(source_head.len)));
    let source = Arc::new(SourceClient {
        client: client.clone(),
        bucket: options.source.bucket.clone(),
        key: options.source.key.clone(),
        len: source_head.len,
        etag: source_head.etag,
        diagnostics: diagnostics.clone(),
    });
    let local_destination = S3Prefix {
        bucket: "__local__".to_string(),
        prefix: String::new(),
    };
    let manifest = load_zip_manifest(
        Arc::clone(&source),
        &local_destination,
        options.ignore_embedded_catalog,
        options.source_block_size,
        None,
    )
    .await?;
    resolve_s3_zip_local_window_capacity(&mut options, source_head.len, manifest.entries.len());
    validate_s3_zip_local_source_range_options(&options, source_head.len)?;

    let entries = manifest.entries;
    validate_local_destination_zip_paths(entries.iter().map(|entry| entry.zip_path.as_str()))?;
    let destination_case_insensitive =
        destination_uses_case_insensitive_paths(&destination_root).await?;
    validate_local_destination_path_collisions(
        entries
            .iter()
            .map(|entry| (entry.zip_path.as_str(), entry.is_directory)),
        destination_case_insensitive,
    )?;
    let total_entries = entries.len();
    let progress = Arc::new(ExtractProgress::new(total_entries));
    let sync_options = local_unzip_source_sync_options(&options);
    let (store, scheduler) = start_source_phase(
        Arc::clone(&source),
        &entries,
        &sync_options,
        source_head.len,
        diagnostics.clone(),
    );
    let mut summary = SyncSummary {
        zip_files: total_entries,
        ..SyncSummary::default()
    };
    let mut operations = if options.collect_operations {
        Vec::with_capacity(entries.len())
    } else {
        Vec::new()
    };
    let mut stream = stream::iter(entries)
        .map(|entry| {
            let store = Arc::clone(&store);
            let destination_root = destination_root.clone();
            async move { unzip_s3_entry_to_local(store, entry, destination_root).await }
        })
        .buffer_unordered(options.concurrency);

    while let Some(operation) = stream.next().await {
        progress.record_operation(&operation);
        summarize_operation(&mut summary, &operation);
        if options.collect_operations {
            operations.push(operation);
        }
    }
    let _ = scheduler.await;

    progress.log_progress(
        diagnostics.as_deref(),
        None,
        started.elapsed(),
        "local unzip completed",
    );

    Ok(LocalUnzipReport {
        source_zip: options.source.uri(),
        destination_dir: options.destination_dir.display().to_string(),
        summary,
        diagnostics: diagnostics.map(|diagnostics| LocalUnzipDiagnostics {
            concurrency: options.concurrency,
            source_block_size: options.source_block_size,
            source_block_merge_gap: options.source_block_merge_gap,
            source_get_concurrency: options.source_get_concurrency,
            source_window_capacity: options.source_window_capacity,
            source: diagnostics.snapshot(),
        }),
        operations,
    })
}

/// Extracts a local ZIP file into a local directory.
///
/// Existing local files are overwritten, ZIP directory entries are created as
/// local directories, and extra local files are left untouched.
pub async fn unzip_file_to_local(options: LocalUnzipOptions) -> Result<LocalUnzipReport> {
    validate_local_unzip_options(&options)?;
    let destination_root = prepare_local_destination_root(&options.destination_dir).await?;
    let reader = open_local_zip_reader(&options.source_zip).await?;
    let source_len = tokio::fs::metadata(&options.source_zip).await?.len();
    let entries = local_zip_entries_for_local(reader.file().entries(), source_len, false)?.entries;
    validate_local_destination_zip_paths(entries.iter().map(|entry| entry.zip_path.as_str()))?;
    let destination_case_insensitive =
        destination_uses_case_insensitive_paths(&destination_root).await?;
    validate_local_destination_path_collisions(
        entries
            .iter()
            .map(|entry| (entry.zip_path.as_str(), entry.is_directory)),
        destination_case_insensitive,
    )?;
    reject_local_unzip_source_archive_target(&options.source_zip, &destination_root, &entries)
        .await?;
    let total_entries = entries.len();
    let mut summary = SyncSummary {
        zip_files: total_entries,
        ..SyncSummary::default()
    };
    let mut operations = Vec::new();

    let mut stream = stream::iter(entries)
        .map(|entry| unzip_local_entry_to_local(&reader, entry, &destination_root))
        .buffer_unordered(options.concurrency);

    while let Some(operation) = stream.next().await {
        summarize_operation(&mut summary, &operation);
        if options.collect_operations {
            operations.push(operation);
        }
    }
    drop(stream);

    Ok(LocalUnzipReport {
        source_zip: options.source_zip.display().to_string(),
        destination_dir: options.destination_dir.display().to_string(),
        summary,
        diagnostics: None,
        operations,
    })
}

/// Plans extracting an S3 ZIP object into an S3 prefix without writing or deleting objects.
pub async fn dry_run_sync_zip_to_s3(
    client: &Client,
    options: SyncOptions,
) -> Result<UnzipDryRunReport> {
    dry_run_sync_zip_to_s3_with_clients(client, client, options).await
}

/// Plans extracting an S3 ZIP object into an S3 prefix with separate source and destination clients.
pub async fn dry_run_sync_zip_to_s3_with_clients(
    source_client: &Client,
    destination_client: &Client,
    mut options: SyncOptions,
) -> Result<UnzipDryRunReport> {
    validate_options(&options)?;
    let source_head = head_source(source_client, &options.source).await?;
    let diagnostics = options
        .collect_diagnostics
        .then(|| Arc::new(SourceDiagnosticsCollector::new(source_head.len)));
    let source = Arc::new(SourceClient {
        client: source_client.clone(),
        bucket: options.source.bucket.clone(),
        key: options.source.key.clone(),
        len: source_head.len,
        etag: source_head.etag,
        diagnostics: diagnostics.clone(),
    });

    let manifest = load_zip_manifest(
        Arc::clone(&source),
        &options.destination,
        options.ignore_embedded_catalog,
        options.source_block_size,
        Some(crate::constants::S3_SINGLE_PUT_LIMIT),
    )
    .await?;
    resolve_source_window_capacity(&mut options, source_head.len, manifest.entries.len());
    validate_source_range_options(&options, source_head.len)?;

    let destination_objects = list_destination(destination_client, &options.destination).await?;
    let entries = manifest.entries;
    let total_entries = entries.len();
    let expected_keys = options.delete_extra.then(|| {
        entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<HashSet<_>>()
    });
    let classified = classify_entries(entries, &destination_objects);
    let mut summary = UnzipDryRunSummary {
        zip_files: total_entries,
        destination_objects: destination_objects.len(),
        ..UnzipDryRunSummary::default()
    };
    let mut operations = Vec::new();

    for operation in classified.reports {
        record_dry_run_operation(
            &mut summary,
            &mut operations,
            options.collect_operations,
            dry_run_report_from_object_report(operation),
        );
    }

    let mut upload_jobs = classified.upload_jobs;
    if !classified.hash_jobs.is_empty() {
        let hash_results = run_hash_phase(
            Arc::clone(&source),
            classified.hash_jobs,
            &options,
            source_head.len,
            diagnostics.clone(),
        )
        .await;
        for result in hash_results {
            match result {
                HashPhaseResult::Operation(operation) => record_dry_run_operation(
                    &mut summary,
                    &mut operations,
                    options.collect_operations,
                    dry_run_report_from_object_report(operation),
                ),
                HashPhaseResult::Upload(job) => upload_jobs.push(job),
            }
        }
    }

    for job in upload_jobs {
        record_dry_run_operation(
            &mut summary,
            &mut operations,
            options.collect_operations,
            dry_run_upload_job_report(job),
        );
    }

    if options.delete_extra {
        let expected_keys = expected_keys.expect("delete-extra expected keys are prepared");
        for key in destination_objects
            .keys()
            .filter(|key| !expected_keys.contains(*key))
        {
            record_dry_run_operation(
                &mut summary,
                &mut operations,
                options.collect_operations,
                dry_run_delete_extra_report(key.clone(), destination_objects.get(key)),
            );
        }
    }

    Ok(UnzipDryRunReport {
        source_zip: options.source.uri(),
        destination: options.destination.uri(),
        summary,
        diagnostics: diagnostics.map(|diagnostics| DryRunDiagnostics {
            concurrency: options.concurrency,
            source_block_size: options.source_block_size,
            source_block_merge_gap: options.source_block_merge_gap,
            source_get_concurrency: options.source_get_concurrency,
            source_window_capacity: options.source_window_capacity,
            source: diagnostics.snapshot(),
        }),
        operations,
    })
}

/// Plans extracting a local ZIP file into an S3 prefix without writing or deleting objects.
pub async fn dry_run_unzip_file_to_s3(
    client: &Client,
    options: LocalZipSyncOptions,
) -> Result<UnzipDryRunReport> {
    validate_local_zip_sync_options(&options)?;
    let reader = open_local_zip_reader(&options.source_zip).await?;
    let source_len = tokio::fs::metadata(&options.source_zip).await?.len();
    let mut entries = local_zip_entries_for_s3(
        reader.file().entries(),
        &options.destination,
        source_len,
        true,
    )?;
    if !options.ignore_embedded_catalog {
        let catalog = load_local_embedded_catalog(&reader, entries.catalog_index).await;
        apply_local_catalog(&mut entries.entries, catalog);
    }

    let destination_objects = list_destination(client, &options.destination).await?;
    let expected_keys = options.delete_extra.then(|| {
        entries
            .entries
            .iter()
            .map(|entry| entry.destination.clone())
            .collect::<HashSet<_>>()
    });
    let total_entries = entries.entries.len();
    let mut summary = UnzipDryRunSummary {
        zip_files: total_entries,
        destination_objects: destination_objects.len(),
        ..UnzipDryRunSummary::default()
    };
    let mut operations = Vec::new();

    let mut stream = stream::iter(entries.entries)
        .map(|entry| dry_run_local_zip_entry_to_s3(&reader, entry, &destination_objects))
        .buffer_unordered(options.concurrency);

    while let Some(operation) = stream.next().await {
        record_dry_run_operation(
            &mut summary,
            &mut operations,
            options.collect_operations,
            operation,
        );
    }
    drop(stream);

    if options.delete_extra {
        let expected_keys = expected_keys.expect("delete-extra expected keys are prepared");
        for key in destination_objects
            .keys()
            .filter(|key| !expected_keys.contains(*key))
        {
            record_dry_run_operation(
                &mut summary,
                &mut operations,
                options.collect_operations,
                dry_run_delete_extra_report(key.clone(), destination_objects.get(key)),
            );
        }
    }

    Ok(UnzipDryRunReport {
        source_zip: options.source_zip.display().to_string(),
        destination: options.destination.uri(),
        summary,
        diagnostics: None,
        operations,
    })
}

/// Plans extracting an S3 ZIP object into a local directory without writing files.
pub async fn dry_run_unzip_s3_zip_to_local(
    client: &Client,
    mut options: S3ZipLocalUnzipOptions,
) -> Result<UnzipDryRunReport> {
    validate_s3_zip_local_unzip_options(&options)?;
    let destination_root =
        validate_local_destination_root_for_dry_run(&options.destination_dir).await?;
    let source_head = head_source(client, &options.source).await?;
    let diagnostics = options
        .collect_diagnostics
        .then(|| Arc::new(SourceDiagnosticsCollector::new(source_head.len)));
    let source = Arc::new(SourceClient {
        client: client.clone(),
        bucket: options.source.bucket.clone(),
        key: options.source.key.clone(),
        len: source_head.len,
        etag: source_head.etag,
        diagnostics: diagnostics.clone(),
    });
    let local_destination = S3Prefix {
        bucket: "__local__".to_string(),
        prefix: String::new(),
    };
    let manifest = load_zip_manifest(
        Arc::clone(&source),
        &local_destination,
        options.ignore_embedded_catalog,
        options.source_block_size,
        None,
    )
    .await?;
    resolve_s3_zip_local_window_capacity(&mut options, source_head.len, manifest.entries.len());
    validate_s3_zip_local_source_range_options(&options, source_head.len)?;

    let entries = manifest.entries;
    validate_local_destination_zip_paths(entries.iter().map(|entry| entry.zip_path.as_str()))?;
    let destination_case_insensitive =
        destination_uses_case_insensitive_paths_for_dry_run(&options.destination_dir).await?;
    validate_local_destination_path_collisions(
        entries
            .iter()
            .map(|entry| (entry.zip_path.as_str(), entry.is_directory)),
        destination_case_insensitive,
    )?;

    let mut summary = UnzipDryRunSummary {
        zip_files: entries.len(),
        ..UnzipDryRunSummary::default()
    };
    let mut operations = Vec::new();
    for entry in entries {
        record_dry_run_operation(
            &mut summary,
            &mut operations,
            options.collect_operations,
            dry_run_manifest_entry_to_local(&destination_root, entry).await,
        );
    }

    Ok(UnzipDryRunReport {
        source_zip: options.source.uri(),
        destination: options.destination_dir.display().to_string(),
        summary,
        diagnostics: diagnostics.map(|diagnostics| DryRunDiagnostics {
            concurrency: options.concurrency,
            source_block_size: options.source_block_size,
            source_block_merge_gap: options.source_block_merge_gap,
            source_get_concurrency: options.source_get_concurrency,
            source_window_capacity: options.source_window_capacity,
            source: diagnostics.snapshot(),
        }),
        operations,
    })
}

/// Plans extracting a local ZIP file into a local directory without writing files.
pub async fn dry_run_unzip_file_to_local(options: LocalUnzipOptions) -> Result<UnzipDryRunReport> {
    validate_local_unzip_options(&options)?;
    let destination_root =
        validate_local_destination_root_for_dry_run(&options.destination_dir).await?;
    let reader = open_local_zip_reader(&options.source_zip).await?;
    let source_len = tokio::fs::metadata(&options.source_zip).await?.len();
    let entries = local_zip_entries_for_local(reader.file().entries(), source_len, false)?.entries;
    validate_local_destination_zip_paths(entries.iter().map(|entry| entry.zip_path.as_str()))?;
    let destination_case_insensitive =
        destination_uses_case_insensitive_paths_for_dry_run(&options.destination_dir).await?;
    validate_local_destination_path_collisions(
        entries
            .iter()
            .map(|entry| (entry.zip_path.as_str(), entry.is_directory)),
        destination_case_insensitive,
    )?;
    reject_local_unzip_source_archive_target(&options.source_zip, &destination_root, &entries)
        .await?;

    let mut summary = UnzipDryRunSummary {
        zip_files: entries.len(),
        ..UnzipDryRunSummary::default()
    };
    let mut operations = Vec::new();
    for entry in entries {
        record_dry_run_operation(
            &mut summary,
            &mut operations,
            options.collect_operations,
            dry_run_local_zip_entry_to_local(&destination_root, &entry).await,
        );
    }

    Ok(UnzipDryRunReport {
        source_zip: options.source_zip.display().to_string(),
        destination: options.destination_dir.display().to_string(),
        summary,
        diagnostics: None,
        operations,
    })
}

type LocalZipReader = ZipFileReader;

#[derive(Clone, Debug)]
struct LocalZipEntry {
    index: usize,
    zip_path: String,
    destination: String,
    size: u64,
    compressed_size: u64,
    compression: Compression,
    crc32: u32,
    catalog_md5: Option<String>,
    is_directory: bool,
}

struct LocalZipEntries {
    entries: Vec<LocalZipEntry>,
    catalog_index: Option<usize>,
}

async fn open_local_zip_reader(path: &Path) -> Result<LocalZipReader> {
    ZipFileReader::new(path)
        .await
        .map_err(|err| invalid_local_path(path, format!("cannot open ZIP file: {err}")))
}

fn local_zip_entries_for_s3(
    stored_entries: &[StoredZipEntry],
    destination: &S3Prefix,
    source_len: u64,
    enforce_s3_limit: bool,
) -> Result<LocalZipEntries> {
    local_zip_entries(stored_entries, source_len, enforce_s3_limit, |zip_path| {
        destination.join_key(zip_path)
    })
}

fn local_zip_entries_for_local(
    stored_entries: &[StoredZipEntry],
    source_len: u64,
    enforce_s3_limit: bool,
) -> Result<LocalZipEntries> {
    local_zip_entries(stored_entries, source_len, enforce_s3_limit, str::to_string)
}

fn local_zip_entries(
    stored_entries: &[StoredZipEntry],
    source_len: u64,
    enforce_s3_limit: bool,
    destination: impl Fn(&str) -> String,
) -> Result<LocalZipEntries> {
    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    let mut catalog_index = None;

    for (index, stored) in stored_entries.iter().enumerate() {
        let raw_path = stored
            .filename()
            .as_str()
            .map_err(|err| Error::InvalidZipEntry {
                path: format!("{:?}", stored.filename().as_bytes()),
                reason: err.to_string(),
            })?;
        let zip_entry_path = normalize_zip_entry_path(raw_path)?;
        let zip_path = zip_entry_path.path;
        let is_directory = zip_entry_path.is_directory;

        if !is_directory && zip_path == EMBEDDED_CATALOG_PATH {
            catalog_index = Some(index);
            continue;
        }

        validate_local_stored_entry(
            stored,
            &zip_path,
            is_directory,
            enforce_s3_limit,
            source_len,
        )?;
        if !seen.insert(zip_path.clone()) {
            return Err(Error::DuplicateZipPath(zip_path));
        }

        entries.push(LocalZipEntry {
            index,
            destination: destination(&zip_path),
            zip_path,
            size: stored.uncompressed_size(),
            compressed_size: stored.compressed_size(),
            compression: stored.compression(),
            crc32: stored.crc32(),
            catalog_md5: None,
            is_directory,
        });
    }

    Ok(LocalZipEntries {
        entries,
        catalog_index,
    })
}

fn validate_local_stored_entry(
    stored: &StoredZipEntry,
    zip_path: &str,
    is_directory: bool,
    enforce_s3_limit: bool,
    source_len: u64,
) -> Result<()> {
    let source_span_end = stored
        .header_offset()
        .checked_add(stored.header_size())
        .and_then(|offset| offset.checked_add(stored.compressed_size()))
        .ok_or_else(|| Error::InvalidZipEntry {
            path: zip_path.to_string(),
            reason: "central directory entry source span overflowed".to_string(),
        })?;
    if source_span_end > source_len {
        return Err(Error::InvalidZipEntry {
            path: zip_path.to_string(),
            reason: format!(
                "central directory entry source span ends at {source_span_end}, beyond source ZIP length {source_len}"
            ),
        });
    }

    if is_directory {
        if stored.uncompressed_size() != 0 || stored.compressed_size() != 0 {
            return Err(Error::InvalidZipEntry {
                path: zip_path.to_string(),
                reason: "directory entries must be zero length".to_string(),
            });
        }
        if stored.crc32() != 0 {
            return Err(Error::InvalidZipEntry {
                path: zip_path.to_string(),
                reason: "directory entries must have a zero CRC32".to_string(),
            });
        }
        return Ok(());
    }

    match stored.compression() {
        Compression::Stored | Compression::Deflate => {}
        other => {
            return Err(Error::InvalidZipEntry {
                path: zip_path.to_string(),
                reason: format!("unsupported compression method {other:?}"),
            });
        }
    }

    if enforce_s3_limit && stored.uncompressed_size() > crate::constants::S3_SINGLE_PUT_LIMIT {
        return Err(Error::EntryTooLarge {
            path: zip_path.to_string(),
            size: stored.uncompressed_size(),
        });
    }

    Ok(())
}

async fn load_local_embedded_catalog(
    reader: &LocalZipReader,
    catalog_index: Option<usize>,
) -> HashMap<String, String> {
    let Some(catalog_index) = catalog_index else {
        return HashMap::new();
    };
    let Some(catalog_entry) = reader.file().entries().get(catalog_index) else {
        tracing::warn!(catalog_index, "local embedded catalog index is missing");
        return HashMap::new();
    };
    let catalog_size = catalog_entry.uncompressed_size();
    let catalog_compressed_size = catalog_entry.compressed_size();
    if catalog_size > EMBEDDED_CATALOG_MAX_BYTES
        || catalog_compressed_size > EMBEDDED_CATALOG_MAX_BYTES
    {
        tracing::warn!(
            catalog_size,
            catalog_compressed_size,
            max_bytes = EMBEDDED_CATALOG_MAX_BYTES,
            "local embedded catalog exceeded size limit"
        );
        return HashMap::new();
    }
    let expected_crc32 = catalog_entry.crc32();

    let mut catalog_reader = match reader.reader_without_entry(catalog_index).await {
        Ok(reader) => reader,
        Err(err) => {
            tracing::warn!(error = %err, "failed to open local embedded catalog");
            return HashMap::new();
        }
    };
    let mut catalog_bytes = Vec::new();
    {
        let mut limited_reader = futures_lite::io::AsyncReadExt::take(
            &mut catalog_reader,
            EMBEDDED_CATALOG_MAX_BYTES + 1,
        );
        if let Err(err) =
            futures_lite::io::AsyncReadExt::read_to_end(&mut limited_reader, &mut catalog_bytes)
                .await
        {
            tracing::warn!(error = %err, "failed to read local embedded catalog");
            return HashMap::new();
        }
    }
    if catalog_bytes.len() > EMBEDDED_CATALOG_MAX_BYTES as usize {
        tracing::warn!(
            read_bytes = catalog_bytes.len(),
            max_bytes = EMBEDDED_CATALOG_MAX_BYTES,
            "local embedded catalog exceeded size limit"
        );
        return HashMap::new();
    }
    if let Err(err) = validate_crc32_value(expected_crc32, catalog_reader.compute_hash()) {
        tracing::warn!(error = %err, "local embedded catalog CRC validation failed");
        return HashMap::new();
    }
    match serde_json::from_slice::<EmbeddedCatalog>(&catalog_bytes) {
        Ok(catalog) => catalog_md5_by_path(catalog),
        Err(err) => {
            tracing::warn!(error = %err, "failed to parse local embedded catalog");
            HashMap::new()
        }
    }
}

fn apply_local_catalog(entries: &mut [LocalZipEntry], catalog: HashMap<String, String>) {
    if catalog.is_empty() {
        return;
    }
    for entry in entries {
        entry.catalog_md5 = catalog.get(&entry.zip_path).cloned();
    }
}

async fn unzip_local_entry_to_s3(
    client: &Client,
    reader: &LocalZipReader,
    entry: LocalZipEntry,
    destination_objects: &HashMap<String, DestinationObject>,
    options: &LocalZipSyncOptions,
) -> ObjectReport {
    let existing = destination_objects.get(&entry.destination);
    if entry.is_directory {
        return put_local_directory_marker(client, entry, existing, options).await;
    }

    let destination_etag = existing.and_then(|destination| destination.etag.clone());
    if let (Some(catalog_md5), Some(destination_etag)) =
        (entry.catalog_md5.as_ref(), destination_etag.as_ref())
        && normalize_etag(destination_etag).as_ref() == Some(catalog_md5)
        && existing.and_then(|destination| destination.size) == Some(entry.size)
    {
        return ObjectReport {
            status: OperationStatus::SkippedUnchanged,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(entry.size),
            md5: Some(catalog_md5.clone()),
            destination_etag: Some(destination_etag.clone()),
            message: None,
        };
    }

    let entry_reader = match reader.reader_without_entry(entry.index).await {
        Ok(reader) => reader.compat(),
        Err(err) => {
            return local_entry_error(&entry, destination_etag, err.to_string());
        }
    };

    if let Some(destination) = existing
        && let Some(destination_etag) = &destination.etag
        && let Some(destination_md5) =
            comparable_destination_md5(destination, destination_etag, &local_manifest_entry(&entry))
    {
        match digest_local_reader(entry_reader, &entry).await {
            Ok(digest) if digest.md5 == destination_md5 => {
                return ObjectReport {
                    status: OperationStatus::SkippedUnchanged,
                    key: entry.destination,
                    zip_path: Some(entry.zip_path),
                    size: Some(digest.bytes),
                    md5: Some(digest.md5),
                    destination_etag: Some(destination_etag.clone()),
                    message: None,
                };
            }
            Ok(_) => {}
            Err(err) => {
                return local_entry_error(&entry, Some(destination_etag.clone()), err.to_string());
            }
        }

        let entry_reader = match reader.reader_without_entry(entry.index).await {
            Ok(reader) => reader.compat(),
            Err(err) => {
                return local_entry_error(&entry, Some(destination_etag.clone()), err.to_string());
            }
        };
        return put_local_file_entry_to_s3(
            client,
            entry_reader,
            entry,
            PutCondition::IfMatch(destination_etag.clone()),
            Some(destination_etag.clone()),
            options,
        )
        .await;
    }

    let condition = destination_etag
        .clone()
        .map(PutCondition::IfMatch)
        .unwrap_or(PutCondition::IfNoneMatch);
    put_local_file_entry_to_s3(
        client,
        entry_reader,
        entry,
        condition,
        destination_etag,
        options,
    )
    .await
}

async fn put_local_directory_marker(
    client: &Client,
    entry: LocalZipEntry,
    existing: Option<&DestinationObject>,
    options: &LocalZipSyncOptions,
) -> ObjectReport {
    if let Some(existing) = existing {
        return match existing.size {
            Some(0) => ObjectReport {
                status: OperationStatus::SkippedUnchanged,
                key: entry.destination,
                zip_path: Some(entry.zip_path),
                size: Some(0),
                md5: Some(empty_md5()),
                destination_etag: existing.etag.clone(),
                message: None,
            },
            Some(size) => local_directory_marker_error(
                entry,
                existing.etag.clone(),
                format!("destination directory marker key exists with nonzero size {size}"),
            ),
            None => local_directory_marker_error(
                entry,
                existing.etag.clone(),
                "destination directory marker was listed without a size".to_string(),
            ),
        };
    }

    let mut request = client
        .put_object()
        .bucket(&options.destination.bucket)
        .key(&entry.destination)
        .content_length(0)
        .body(ByteStream::from_static(b""));
    request = request.if_none_match("*");
    match request.send().await {
        Ok(_) => ObjectReport {
            status: OperationStatus::UploadedNew,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(0),
            md5: Some(empty_md5()),
            destination_etag: None,
            message: None,
        },
        Err(err) if is_conditional_put_conflict(&err) => ObjectReport {
            status: OperationStatus::ConditionalConflict,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(0),
            md5: Some(empty_md5()),
            destination_etag: None,
            message: Some(aws_error_context(&err)),
        },
        Err(err) => ObjectReport {
            status: OperationStatus::Error,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(0),
            md5: None,
            destination_etag: None,
            message: Some(aws_error_context(&err)),
        },
    }
}

fn local_directory_marker_error(
    entry: LocalZipEntry,
    destination_etag: Option<String>,
    message: String,
) -> ObjectReport {
    ObjectReport {
        status: OperationStatus::Error,
        key: entry.destination,
        zip_path: Some(entry.zip_path),
        size: Some(0),
        md5: None,
        destination_etag,
        message: Some(message),
    }
}

async fn put_local_file_entry_to_s3<R>(
    client: &Client,
    entry_reader: R,
    entry: LocalZipEntry,
    condition: PutCondition,
    destination_etag: Option<String>,
    options: &LocalZipSyncOptions,
) -> ObjectReport
where
    R: AsyncRead + Send + Unpin + 'static,
{
    if entry.size == 0 {
        let digest = match digest_local_reader(entry_reader, &entry).await {
            Ok(digest) => digest,
            Err(err) => return local_entry_error(&entry, destination_etag, err.to_string()),
        };
        let mut request = client
            .put_object()
            .bucket(&options.destination.bucket)
            .key(&entry.destination)
            .content_length(0)
            .body(ByteStream::from_static(b""));
        request = match &condition {
            PutCondition::IfNoneMatch => request.if_none_match("*"),
            PutCondition::IfMatch(etag) => request.if_match(etag),
        };

        return match request.send().await {
            Ok(_) => ObjectReport {
                status: if destination_etag.is_some() {
                    OperationStatus::UploadedChanged
                } else {
                    OperationStatus::UploadedNew
                },
                key: entry.destination,
                zip_path: Some(entry.zip_path),
                size: Some(digest.bytes),
                md5: Some(digest.md5),
                destination_etag,
                message: None,
            },
            Err(err) if is_conditional_put_conflict(&err) => ObjectReport {
                status: OperationStatus::ConditionalConflict,
                key: entry.destination,
                zip_path: Some(entry.zip_path),
                size: Some(entry.size),
                md5: Some(digest.md5),
                destination_etag,
                message: Some(aws_error_context(&err)),
            },
            Err(err) => local_entry_error(&entry, destination_etag, aws_error_context(&err)),
        };
    }

    let (writer, reader) = tokio::io::duplex(options.pipe_capacity);
    let producer_entry = entry.clone();
    let mut producer = AbortOnDropJoinHandle::new(tokio::spawn(async move {
        write_local_entry_to_pipe(writer, entry_reader, producer_entry).await
    }));

    let stream = ReaderStream::with_capacity(reader, options.body_chunk_size).map_ok(Frame::data);
    let body = ByteStream::new(SdkBody::from_body_1_x(StreamBody::new(stream)));
    let content_length = match i64::try_from(entry.size) {
        Ok(length) => length,
        Err(_) => {
            producer.abort();
            return local_entry_error(
                &entry,
                destination_etag,
                "entry size does not fit S3 content length".to_string(),
            );
        }
    };
    let mut request = client
        .put_object()
        .bucket(&options.destination.bucket)
        .key(&entry.destination)
        .content_length(content_length)
        .body(body);
    request = match &condition {
        PutCondition::IfNoneMatch => request.if_none_match("*"),
        PutCondition::IfMatch(etag) => request.if_match(etag),
    };

    match request.send().await {
        Ok(_) => match producer.join().await {
            Ok(Ok(digest)) => ObjectReport {
                status: if destination_etag.is_some() {
                    OperationStatus::UploadedChanged
                } else {
                    OperationStatus::UploadedNew
                },
                key: entry.destination,
                zip_path: Some(entry.zip_path),
                size: Some(digest.bytes),
                md5: Some(digest.md5),
                destination_etag,
                message: None,
            },
            Ok(Err(err)) => local_entry_error(&entry, destination_etag, err.to_string()),
            Err(err) => local_entry_error(&entry, destination_etag, err.to_string()),
        },
        Err(err) if is_conditional_put_conflict(&err) => {
            producer.abort();
            let _ = producer.join().await;
            ObjectReport {
                status: OperationStatus::ConditionalConflict,
                key: entry.destination,
                zip_path: Some(entry.zip_path),
                size: Some(entry.size),
                md5: entry.catalog_md5,
                destination_etag,
                message: Some(aws_error_context(&err)),
            }
        }
        Err(err) => {
            producer.abort();
            let _ = producer.join().await;
            local_entry_error(&entry, destination_etag, aws_error_context(&err))
        }
    }
}

async fn unzip_local_entry_to_local(
    reader: &LocalZipReader,
    entry: LocalZipEntry,
    destination_root: &Path,
) -> ObjectReport {
    if entry.is_directory {
        return create_local_directory_entry(destination_root, &entry).await;
    }

    let existed = local_destination_exists(destination_root, &entry).await;
    let entry_reader = match reader.reader_without_entry(entry.index).await {
        Ok(reader) => reader.compat(),
        Err(err) => {
            return local_destination_error(destination_root, &entry, err.to_string());
        }
    };
    write_reader_to_local_destination(destination_root, &entry, entry_reader, existed).await
}

async fn unzip_s3_entry_to_local(
    store: Arc<BlockStore>,
    entry: ManifestEntry,
    destination_root: PathBuf,
) -> ObjectReport {
    let local_entry = local_entry_from_manifest(&entry);
    if entry.is_directory {
        return create_local_directory_entry(&destination_root, &local_entry).await;
    }

    let existed = local_destination_exists(&destination_root, &local_entry).await;
    let reader = match entry_reader(store, &entry).await {
        Ok(reader) => reader,
        Err(err) => {
            return local_destination_error(&destination_root, &local_entry, err.to_string());
        }
    };
    write_reader_to_local_destination(&destination_root, &local_entry, reader, existed).await
}

async fn create_local_directory_entry(
    destination_root: &Path,
    entry: &LocalZipEntry,
) -> ObjectReport {
    let destination = local_destination_path(destination_root, &entry.zip_path);
    let existed = path_is_directory(&destination).await;
    let result = create_directory_no_symlink(destination_root, &entry.zip_path).await;
    match result {
        Ok(()) => ObjectReport {
            status: if existed {
                OperationStatus::SkippedUnchanged
            } else {
                OperationStatus::UploadedNew
            },
            key: destination.display().to_string(),
            zip_path: Some(entry.zip_path.clone()),
            size: Some(0),
            md5: Some(empty_md5()),
            destination_etag: None,
            message: None,
        },
        Err(err) => local_destination_error(destination_root, entry, err.to_string()),
    }
}

async fn write_reader_to_local_destination<R>(
    destination_root: &Path,
    entry: &LocalZipEntry,
    reader: R,
    existed: bool,
) -> ObjectReport
where
    R: AsyncRead + Unpin,
{
    let destination = local_destination_path(destination_root, &entry.zip_path);
    let result = async {
        create_parent_dirs_for_file(destination_root, &entry.zip_path).await?;
        reject_existing_symlink_or_directory(&destination).await?;
        let temp_path = temp_sibling_path(&destination)?;
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await
            .map_err(|err| invalid_local_path(&temp_path, format!("cannot create file: {err}")))?;
        let digest = match write_local_entry_to_file(file, reader, entry).await {
            Ok(digest) => digest,
            Err(err) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(err);
            }
        };
        replace_temp_file(&temp_path, &destination, "completed file").await?;
        Ok(digest)
    }
    .await;

    match result {
        Ok(digest) => ObjectReport {
            status: if existed {
                OperationStatus::UploadedChanged
            } else {
                OperationStatus::UploadedNew
            },
            key: destination.display().to_string(),
            zip_path: Some(entry.zip_path.clone()),
            size: Some(digest.bytes),
            md5: Some(digest.md5),
            destination_etag: None,
            message: None,
        },
        Err(err) => local_destination_error(destination_root, entry, err.to_string()),
    }
}

async fn write_local_entry_to_pipe<R>(
    writer: DuplexStream,
    reader: R,
    entry: LocalZipEntry,
) -> Result<ExtractDigest>
where
    R: AsyncRead + Unpin,
{
    write_local_entry_to_writer(writer, reader, &entry).await
}

async fn write_local_entry_to_file<R>(
    file: tokio::fs::File,
    reader: R,
    entry: &LocalZipEntry,
) -> Result<ExtractDigest>
where
    R: AsyncRead + Unpin,
{
    write_local_entry_to_writer(file, reader, entry).await
}

async fn write_local_entry_to_writer<W, R>(
    mut writer: W,
    mut reader: R,
    entry: &LocalZipEntry,
) -> Result<ExtractDigest>
where
    W: tokio::io::AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let mut hasher = Md5::new();
    let mut crc32 = Crc32Hasher::new();
    let mut bytes = 0_u64;
    const BUFFER_SIZE: usize = 64 * 1024;
    let mut buffer = vec![0_u8; BUFFER_SIZE];
    let mut pending = vec![0_u8; BUFFER_SIZE];
    let mut pending_len = 0;

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let next_bytes = bytes.saturating_add(read as u64);
        validate_local_extracted_size_not_exceeded(entry, next_bytes)?;
        if pending_len != 0 {
            writer.write_all(&pending[..pending_len]).await?;
        }
        hasher.update(&buffer[..read]);
        crc32.update(&buffer[..read]);
        std::mem::swap(&mut pending, &mut buffer);
        pending_len = read;
        bytes = next_bytes;
    }

    validate_local_extracted_size(entry, bytes)?;
    validate_crc32_value(entry.crc32, crc32.finalize())?;
    if pending_len != 0 {
        writer.write_all(&pending[..pending_len]).await?;
    }
    writer.shutdown().await?;
    Ok(ExtractDigest {
        bytes,
        md5: hex::encode(hasher.finalize()),
    })
}

async fn digest_local_reader<R>(reader: R, entry: &LocalZipEntry) -> Result<ExtractDigest>
where
    R: AsyncRead + Unpin,
{
    write_local_entry_to_writer(tokio::io::sink(), reader, entry).await
}

fn validate_local_extracted_size(entry: &LocalZipEntry, bytes: u64) -> Result<()> {
    if bytes == entry.size {
        Ok(())
    } else {
        Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: format!(
                "entry produced {bytes} bytes but central directory declared {} bytes",
                entry.size
            ),
        })
    }
}

fn validate_local_extracted_size_not_exceeded(entry: &LocalZipEntry, bytes: u64) -> Result<()> {
    if bytes <= entry.size {
        Ok(())
    } else {
        validate_local_extracted_size(entry, bytes)
    }
}

fn local_entry_error(
    entry: &LocalZipEntry,
    destination_etag: Option<String>,
    message: String,
) -> ObjectReport {
    ObjectReport {
        status: OperationStatus::Error,
        key: entry.destination.clone(),
        zip_path: Some(entry.zip_path.clone()),
        size: Some(entry.size),
        md5: entry.catalog_md5.clone(),
        destination_etag,
        message: Some(message),
    }
}

fn local_destination_error(
    destination_root: &Path,
    entry: &LocalZipEntry,
    message: String,
) -> ObjectReport {
    ObjectReport {
        status: OperationStatus::Error,
        key: local_destination_path(destination_root, &entry.zip_path)
            .display()
            .to_string(),
        zip_path: Some(entry.zip_path.clone()),
        size: Some(entry.size),
        md5: entry.catalog_md5.clone(),
        destination_etag: None,
        message: Some(message),
    }
}

fn record_dry_run_operation(
    summary: &mut UnzipDryRunSummary,
    operations: &mut Vec<DryRunObjectReport>,
    collect_operations: bool,
    operation: DryRunObjectReport,
) {
    summarize_dry_run_operation(summary, &operation);
    if collect_operations {
        operations.push(operation);
    }
}

fn dry_run_report_from_object_report(operation: ObjectReport) -> DryRunObjectReport {
    let status = match operation.status {
        OperationStatus::UploadedNew => DryRunOperationStatus::WouldUploadNew,
        OperationStatus::UploadedChanged => DryRunOperationStatus::WouldUploadChanged,
        OperationStatus::SkippedUnchanged => DryRunOperationStatus::SkippedUnchanged,
        OperationStatus::DeletedExtra => DryRunOperationStatus::WouldDeleteExtra,
        OperationStatus::ConditionalConflict | OperationStatus::Error => {
            DryRunOperationStatus::Error
        }
    };
    DryRunObjectReport {
        status,
        key: operation.key,
        zip_path: operation.zip_path,
        size: operation.size,
        md5: operation.md5,
        destination_etag: operation.destination_etag,
        message: operation.message,
    }
}

fn dry_run_upload_job_report(job: UploadJob) -> DryRunObjectReport {
    let (status, destination_etag) = match job.condition {
        PutCondition::IfNoneMatch => (DryRunOperationStatus::WouldUploadNew, None),
        PutCondition::IfMatch(etag) => (DryRunOperationStatus::WouldUploadChanged, Some(etag)),
    };
    DryRunObjectReport {
        status,
        key: job.entry.key,
        zip_path: Some(job.entry.zip_path),
        size: job
            .comparison_digest
            .as_ref()
            .map(|digest| digest.bytes)
            .or(Some(job.entry.size)),
        md5: job
            .comparison_digest
            .map(|digest| digest.md5)
            .or(job.entry.catalog_md5),
        destination_etag,
        message: None,
    }
}

fn dry_run_delete_extra_report(
    key: String,
    destination: Option<&DestinationObject>,
) -> DryRunObjectReport {
    DryRunObjectReport {
        status: DryRunOperationStatus::WouldDeleteExtra,
        key,
        zip_path: None,
        size: destination.and_then(|destination| destination.size),
        md5: None,
        destination_etag: destination.and_then(|destination| destination.etag.clone()),
        message: None,
    }
}

async fn dry_run_local_zip_entry_to_s3(
    reader: &LocalZipReader,
    entry: LocalZipEntry,
    destination_objects: &HashMap<String, DestinationObject>,
) -> DryRunObjectReport {
    let existing = destination_objects.get(&entry.destination);
    if entry.is_directory {
        return dry_run_local_directory_marker(entry, existing);
    }

    let destination_etag = existing.and_then(|destination| destination.etag.clone());
    if let (Some(catalog_md5), Some(destination_etag)) =
        (entry.catalog_md5.as_ref(), destination_etag.as_ref())
        && normalize_etag(destination_etag).as_ref() == Some(catalog_md5)
        && existing.and_then(|destination| destination.size) == Some(entry.size)
    {
        return DryRunObjectReport {
            status: DryRunOperationStatus::SkippedUnchanged,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(entry.size),
            md5: Some(catalog_md5.clone()),
            destination_etag: Some(destination_etag.clone()),
            message: None,
        };
    }

    let Some(destination) = existing else {
        return DryRunObjectReport {
            status: DryRunOperationStatus::WouldUploadNew,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(entry.size),
            md5: entry.catalog_md5,
            destination_etag: None,
            message: None,
        };
    };
    let Some(destination_etag) = destination.etag.clone() else {
        return dry_run_local_entry_error(
            &entry,
            None,
            "destination object was listed without an ETag".to_string(),
        );
    };

    if let Some(destination_md5) = comparable_destination_md5(
        destination,
        &destination_etag,
        &local_manifest_entry(&entry),
    ) {
        let entry_reader = match reader.reader_without_entry(entry.index).await {
            Ok(reader) => reader.compat(),
            Err(err) => {
                return dry_run_local_entry_error(&entry, Some(destination_etag), err.to_string());
            }
        };
        match digest_local_reader(entry_reader, &entry).await {
            Ok(digest) if digest.md5 == destination_md5 => {
                return DryRunObjectReport {
                    status: DryRunOperationStatus::SkippedUnchanged,
                    key: entry.destination,
                    zip_path: Some(entry.zip_path),
                    size: Some(digest.bytes),
                    md5: Some(digest.md5),
                    destination_etag: Some(destination_etag),
                    message: None,
                };
            }
            Ok(digest) => {
                return DryRunObjectReport {
                    status: DryRunOperationStatus::WouldUploadChanged,
                    key: entry.destination,
                    zip_path: Some(entry.zip_path),
                    size: Some(digest.bytes),
                    md5: Some(digest.md5),
                    destination_etag: Some(destination_etag),
                    message: None,
                };
            }
            Err(err) => {
                return dry_run_local_entry_error(&entry, Some(destination_etag), err.to_string());
            }
        }
    }

    DryRunObjectReport {
        status: DryRunOperationStatus::WouldUploadChanged,
        key: entry.destination,
        zip_path: Some(entry.zip_path),
        size: Some(entry.size),
        md5: entry.catalog_md5,
        destination_etag: Some(destination_etag),
        message: None,
    }
}

fn dry_run_local_directory_marker(
    entry: LocalZipEntry,
    existing: Option<&DestinationObject>,
) -> DryRunObjectReport {
    let Some(existing) = existing else {
        return DryRunObjectReport {
            status: DryRunOperationStatus::WouldUploadNew,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(0),
            md5: Some(empty_md5()),
            destination_etag: None,
            message: None,
        };
    };

    match existing.size {
        Some(0) => DryRunObjectReport {
            status: DryRunOperationStatus::SkippedUnchanged,
            key: entry.destination,
            zip_path: Some(entry.zip_path),
            size: Some(0),
            md5: Some(empty_md5()),
            destination_etag: existing.etag.clone(),
            message: None,
        },
        Some(size) => dry_run_local_entry_error(
            &entry,
            existing.etag.clone(),
            format!("destination directory marker key exists with nonzero size {size}"),
        ),
        None => dry_run_local_entry_error(
            &entry,
            existing.etag.clone(),
            "destination directory marker was listed without a size".to_string(),
        ),
    }
}

fn dry_run_local_entry_error(
    entry: &LocalZipEntry,
    destination_etag: Option<String>,
    message: String,
) -> DryRunObjectReport {
    DryRunObjectReport {
        status: DryRunOperationStatus::Error,
        key: entry.destination.clone(),
        zip_path: Some(entry.zip_path.clone()),
        size: Some(entry.size),
        md5: entry.catalog_md5.clone(),
        destination_etag,
        message: Some(message),
    }
}

async fn dry_run_manifest_entry_to_local(
    destination_root: &Path,
    entry: ManifestEntry,
) -> DryRunObjectReport {
    let entry = local_entry_from_manifest(&entry);
    dry_run_local_zip_entry_to_local(destination_root, &entry).await
}

async fn dry_run_local_zip_entry_to_local(
    destination_root: &Path,
    entry: &LocalZipEntry,
) -> DryRunObjectReport {
    if entry.is_directory {
        dry_run_local_directory_entry_to_local(destination_root, entry).await
    } else {
        dry_run_local_file_entry_to_local(destination_root, entry).await
    }
}

async fn dry_run_local_directory_entry_to_local(
    destination_root: &Path,
    entry: &LocalZipEntry,
) -> DryRunObjectReport {
    let destination = local_destination_path(destination_root, &entry.zip_path);
    let mut current = destination_root.to_path_buf();
    let mut missing_component = false;
    for component in entry.zip_path.trim_end_matches('/').split('/') {
        if component.is_empty() {
            continue;
        }
        current.push(component);
        if missing_component {
            continue;
        }
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return dry_run_local_destination_error(
                    &destination,
                    entry,
                    "destination path component cannot be a symbolic link".to_string(),
                );
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return dry_run_local_destination_error(
                    &destination,
                    entry,
                    "destination path component exists and is not a directory".to_string(),
                );
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                missing_component = true;
            }
            Err(err) => {
                return dry_run_local_destination_error(
                    &destination,
                    entry,
                    format!("cannot inspect destination path component: {err}"),
                );
            }
        }
    }

    DryRunObjectReport {
        status: if missing_component {
            DryRunOperationStatus::WouldUploadNew
        } else {
            DryRunOperationStatus::SkippedUnchanged
        },
        key: destination.display().to_string(),
        zip_path: Some(entry.zip_path.clone()),
        size: Some(0),
        md5: Some(empty_md5()),
        destination_etag: None,
        message: None,
    }
}

async fn dry_run_local_file_entry_to_local(
    destination_root: &Path,
    entry: &LocalZipEntry,
) -> DryRunObjectReport {
    let destination = local_destination_path(destination_root, &entry.zip_path);
    if let Err(err) = validate_parent_dirs_for_dry_run(destination_root, &entry.zip_path).await {
        return dry_run_local_destination_error(&destination, entry, err.to_string());
    }

    match tokio::fs::symlink_metadata(&destination).await {
        Ok(metadata) if metadata.file_type().is_symlink() => dry_run_local_destination_error(
            &destination,
            entry,
            "destination file cannot be a symbolic link".to_string(),
        ),
        Ok(metadata) if metadata.is_dir() => dry_run_local_destination_error(
            &destination,
            entry,
            "destination file path is a directory".to_string(),
        ),
        Ok(_) => DryRunObjectReport {
            status: DryRunOperationStatus::WouldUploadChanged,
            key: destination.display().to_string(),
            zip_path: Some(entry.zip_path.clone()),
            size: Some(entry.size),
            md5: entry.catalog_md5.clone(),
            destination_etag: None,
            message: None,
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => DryRunObjectReport {
            status: DryRunOperationStatus::WouldUploadNew,
            key: destination.display().to_string(),
            zip_path: Some(entry.zip_path.clone()),
            size: Some(entry.size),
            md5: entry.catalog_md5.clone(),
            destination_etag: None,
            message: None,
        },
        Err(err) => dry_run_local_destination_error(
            &destination,
            entry,
            format!("cannot inspect destination file: {err}"),
        ),
    }
}

fn dry_run_local_destination_error(
    destination: &Path,
    entry: &LocalZipEntry,
    message: String,
) -> DryRunObjectReport {
    DryRunObjectReport {
        status: DryRunOperationStatus::Error,
        key: destination.display().to_string(),
        zip_path: Some(entry.zip_path.clone()),
        size: Some(entry.size),
        md5: entry.catalog_md5.clone(),
        destination_etag: None,
        message: Some(message),
    }
}

fn local_entry_from_manifest(entry: &ManifestEntry) -> LocalZipEntry {
    LocalZipEntry {
        index: 0,
        zip_path: entry.zip_path.clone(),
        destination: entry.zip_path.clone(),
        size: entry.size,
        compressed_size: entry.compressed_size,
        compression: entry.compression,
        crc32: entry.crc32,
        catalog_md5: entry.catalog_md5.clone(),
        is_directory: entry.is_directory,
    }
}

fn local_manifest_entry(entry: &LocalZipEntry) -> ManifestEntry {
    ManifestEntry {
        source_offset: 0,
        source_span_start: 0,
        source_span_end: entry.compressed_size,
        zip_path: entry.zip_path.clone(),
        key: entry.destination.clone(),
        size: entry.size,
        compressed_size: entry.compressed_size,
        compression: entry.compression,
        crc32: entry.crc32,
        catalog_md5: entry.catalog_md5.clone(),
        is_directory: entry.is_directory,
    }
}

async fn prepare_local_destination_root(destination: &Path) -> Result<PathBuf> {
    create_destination_directory_no_symlink(destination).await?;
    Ok(destination.to_path_buf())
}

async fn validate_local_destination_root_for_dry_run(destination: &Path) -> Result<PathBuf> {
    match tokio::fs::symlink_metadata(destination).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(invalid_local_path(
                destination,
                "destination directory cannot be a symbolic link".to_string(),
            ));
        }
        Ok(metadata) if metadata.is_dir() => {
            validate_existing_directory_chain_no_symlink(destination).await?;
        }
        Ok(_) => {
            return Err(invalid_local_path(
                destination,
                "destination must be a directory".to_string(),
            ));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            validate_missing_destination_parent_chain(destination).await?;
        }
        Err(err) => {
            return Err(invalid_local_path(
                destination,
                format!("cannot inspect destination directory: {err}"),
            ));
        }
    }

    Ok(destination.to_path_buf())
}

async fn validate_missing_destination_parent_chain(destination: &Path) -> Result<()> {
    let mut current = destination;
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() {
            break;
        }

        match tokio::fs::symlink_metadata(parent).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_local_path(
                    parent,
                    "destination path component cannot be a symbolic link".to_string(),
                ));
            }
            Ok(metadata) if metadata.is_dir() => {
                validate_existing_directory_chain_no_symlink(parent).await?;
                return Ok(());
            }
            Ok(_) => {
                return Err(invalid_local_path(
                    parent,
                    "destination path component exists and is not a directory".to_string(),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                current = parent;
            }
            Err(err) => {
                return Err(invalid_local_path(
                    parent,
                    format!("cannot inspect destination path component: {err}"),
                ));
            }
        }
    }

    Ok(())
}

async fn validate_parent_dirs_for_dry_run(root: &Path, zip_path: &str) -> Result<()> {
    let Some((parent, _file_name)) = zip_path.rsplit_once('/') else {
        return Ok(());
    };

    let mut current = root.to_path_buf();
    for component in parent.split('/') {
        if component.is_empty() {
            continue;
        }
        current.push(component);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_local_path(
                    &current,
                    "destination path component cannot be a symbolic link".to_string(),
                ));
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(invalid_local_path(
                    &current,
                    "destination path component exists and is not a directory".to_string(),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(invalid_local_path(
                    &current,
                    format!("cannot inspect destination path component: {err}"),
                ));
            }
        }
    }

    Ok(())
}

async fn destination_uses_case_insensitive_paths_for_dry_run(destination: &Path) -> Result<bool> {
    let probe_root = existing_case_probe_root(destination).await?;
    if let Some(case_insensitive) = platform_case_insensitive_paths(&probe_root)? {
        return Ok(case_insensitive);
    }
    if let Some(case_insensitive) =
        infer_case_insensitive_paths_from_existing_names(&probe_root).await?
    {
        return Ok(case_insensitive);
    }
    Ok(default_case_insensitive_paths())
}

async fn existing_case_probe_root(destination: &Path) -> Result<PathBuf> {
    match tokio::fs::symlink_metadata(destination).await {
        Ok(metadata) if metadata.is_dir() => return Ok(destination.to_path_buf()),
        Ok(_) => {
            return Err(invalid_local_path(
                destination,
                "destination must be a directory".to_string(),
            ));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(invalid_local_path(
                destination,
                format!("cannot inspect destination directory: {err}"),
            ));
        }
    }

    let mut current = destination;
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() {
            break;
        }
        match tokio::fs::symlink_metadata(parent).await {
            Ok(metadata) if metadata.is_dir() => return Ok(parent.to_path_buf()),
            Ok(_) => {
                return Err(invalid_local_path(
                    parent,
                    "destination path component exists and is not a directory".to_string(),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                current = parent;
            }
            Err(err) => {
                return Err(invalid_local_path(
                    parent,
                    format!("cannot inspect destination path component: {err}"),
                ));
            }
        }
    }

    Ok(PathBuf::from("."))
}

#[cfg(target_os = "macos")]
fn platform_case_insensitive_paths(path: &Path) -> Result<Option<bool>> {
    let path = c_path(path)?;
    let result = unsafe { libc::pathconf(path.as_ptr(), libc::_PC_CASE_SENSITIVE) };
    match result {
        0 => Ok(Some(true)),
        1.. => Ok(Some(false)),
        _ => Ok(None),
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn platform_case_insensitive_paths(path: &Path) -> Result<Option<bool>> {
    if let Some(case_insensitive) = linux_case_insensitive_paths(path)? {
        return Ok(Some(case_insensitive));
    }
    Ok(None)
}

#[cfg(target_os = "linux")]
fn linux_case_insensitive_paths(path: &Path) -> Result<Option<bool>> {
    if linux_directory_has_casefold_flag(path)? {
        return Ok(Some(true));
    }
    let path = c_path(path)?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    let result = unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Ok(None);
    }
    let stat = unsafe { stat.assume_init() };
    let filesystem_type = stat.f_type;
    if filesystem_type == libc::EXT4_SUPER_MAGIC || filesystem_type == libc::F2FS_SUPER_MAGIC {
        return Ok(Some(false));
    }
    const EXFAT_SUPER_MAGIC: libc::c_long = 0x2011_bab0;
    const NTFS_SB_MAGIC: libc::c_long = 0x5346_544e;
    const CIFS_SUPER_MAGIC: libc::c_long = 0xff53_4d42;
    if filesystem_type == libc::MSDOS_SUPER_MAGIC
        || filesystem_type == libc::SMB_SUPER_MAGIC
        || filesystem_type == EXFAT_SUPER_MAGIC
        || filesystem_type == NTFS_SB_MAGIC
        || filesystem_type == CIFS_SUPER_MAGIC
    {
        return Ok(Some(true));
    }
    Ok(None)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn linux_case_insensitive_paths(_path: &Path) -> Result<Option<bool>> {
    Ok(None)
}

#[cfg(target_os = "linux")]
fn linux_directory_has_casefold_flag(path: &Path) -> Result<bool> {
    const FS_CASEFOLD_FL: libc::c_int = 0x4000_0000;
    let display_path = path.to_path_buf();
    let path = c_path(path)?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Ok(false);
    }
    let mut flags = 0 as libc::c_int;
    let result = unsafe { libc::ioctl(fd, libc::FS_IOC_GETFLAGS, &mut flags) };
    let close_result = unsafe { libc::close(fd) };
    if close_result != 0 {
        let close_error = std::io::Error::last_os_error();
        return Err(invalid_local_path(
            &display_path,
            format!(
                "cannot close destination directory after case-sensitivity probe: {close_error}"
            ),
        ));
    }
    Ok(result == 0 && flags & FS_CASEFOLD_FL != 0)
}

#[cfg(unix)]
async fn infer_case_insensitive_paths_from_existing_names(path: &Path) -> Result<Option<bool>> {
    let mut entries = match tokio::fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(_) => return Ok(None),
    };
    while let Some(entry) = entries.next_entry().await.map_err(|err| {
        invalid_local_path(
            path,
            format!("cannot read destination directory entry: {err}"),
        )
    })? {
        let name = entry.file_name();
        let Some(alternate_name) = alternate_case_name(&name) else {
            continue;
        };
        let alternate_path = path.join(alternate_name);
        let original_metadata = tokio::fs::symlink_metadata(entry.path())
            .await
            .map_err(|err| {
                invalid_local_path(
                    &entry.path(),
                    format!("cannot inspect destination directory entry: {err}"),
                )
            })?;
        match tokio::fs::symlink_metadata(&alternate_path).await {
            Ok(alternate_metadata) => {
                return Ok(Some(
                    original_metadata.dev() == alternate_metadata.dev()
                        && original_metadata.ino() == alternate_metadata.ino(),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Some(false)),
            Err(_) => continue,
        }
    }
    Ok(None)
}

#[cfg(not(unix))]
async fn infer_case_insensitive_paths_from_existing_names(_path: &Path) -> Result<Option<bool>> {
    Ok(None)
}

#[cfg(windows)]
fn platform_case_insensitive_paths(path: &Path) -> Result<Option<bool>> {
    let display_path = path.to_path_buf();
    let path = windows_path(path);
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(invalid_local_path(
            &display_path,
            format!(
                "cannot open destination directory for case-sensitivity probe: {}",
                std::io::Error::last_os_error()
            ),
        ));
    }

    let mut info = FILE_CASE_SENSITIVE_INFO { Flags: 0 };
    let query_result = unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileCaseSensitiveInfo,
            &mut info as *mut FILE_CASE_SENSITIVE_INFO as *mut std::ffi::c_void,
            std::mem::size_of::<FILE_CASE_SENSITIVE_INFO>() as u32,
        )
    };
    let query_error = (query_result == 0).then(std::io::Error::last_os_error);
    let close_result = unsafe { CloseHandle(handle) };
    if close_result == 0 {
        return Err(invalid_local_path(
            &display_path,
            format!(
                "cannot close destination directory after case-sensitivity probe: {}",
                std::io::Error::last_os_error()
            ),
        ));
    }

    if let Some(err) = query_error {
        return match err.raw_os_error() {
            Some(code)
                if code == ERROR_INVALID_FUNCTION as i32
                    || code == ERROR_INVALID_PARAMETER as i32 =>
            {
                Ok(None)
            }
            _ => Err(invalid_local_path(
                &display_path,
                format!("cannot query destination case sensitivity: {err}"),
            )),
        };
    }

    Ok(Some(info.Flags & FILE_CS_FLAG_CASE_SENSITIVE_DIR == 0))
}

#[cfg(all(not(unix), not(windows)))]
fn platform_case_insensitive_paths(_path: &Path) -> Result<Option<bool>> {
    Ok(None)
}

#[cfg(windows)]
fn windows_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn alternate_case_name(name: &std::ffi::OsStr) -> Option<String> {
    let name = name.to_str()?;
    let mut changed = false;
    let alternate = name
        .chars()
        .map(|character| {
            if character.is_ascii_lowercase() {
                changed = true;
                character.to_ascii_uppercase()
            } else if character.is_ascii_uppercase() {
                changed = true;
                character.to_ascii_lowercase()
            } else {
                character
            }
        })
        .collect::<String>();
    changed.then_some(alternate)
}

fn default_case_insensitive_paths() -> bool {
    cfg!(windows)
}

#[cfg(unix)]
fn c_path(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| invalid_local_path(path, "path contains an interior NUL byte".to_string()))
}

async fn destination_uses_case_insensitive_paths(root: &Path) -> Result<bool> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let probe_name = format!(".s3-unspool-case-probe-{}-{nanos}-a", std::process::id());
    let probe_path = root.join(&probe_name);
    let alternate_path = root.join(probe_name.to_ascii_uppercase());

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe_path)
        .await
        .map_err(|err| {
            invalid_local_path(
                &probe_path,
                format!("cannot create case-sensitivity probe: {err}"),
            )
        })?;
    drop(file);

    let result = match tokio::fs::symlink_metadata(&alternate_path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(invalid_local_path(
            &alternate_path,
            format!("cannot inspect case-sensitivity probe: {err}"),
        )),
    };
    let cleanup = tokio::fs::remove_file(&probe_path).await;

    match (result, cleanup) {
        (Ok(case_insensitive), Ok(())) => Ok(case_insensitive),
        (Ok(case_insensitive), Err(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(case_insensitive)
        }
        (Ok(_), Err(err)) => Err(invalid_local_path(
            &probe_path,
            format!("cannot remove case-sensitivity probe: {err}"),
        )),
        (Err(err), _) => Err(err),
    }
}

async fn create_destination_directory_no_symlink(destination: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(destination).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(invalid_local_path(
                destination,
                "destination directory cannot be a symbolic link".to_string(),
            ));
        }
        Ok(metadata) if metadata.is_dir() => {
            validate_existing_directory_chain_no_symlink(destination).await?;
            return Ok(());
        }
        Ok(_) => {
            return Err(invalid_local_path(
                destination,
                "destination must be a directory".to_string(),
            ));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(invalid_local_path(
                destination,
                format!("cannot inspect destination directory: {err}"),
            ));
        }
    }

    let mut missing = vec![destination.to_path_buf()];
    let mut current = destination;
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() {
            break;
        }

        match tokio::fs::symlink_metadata(parent).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_local_path(
                    parent,
                    "destination path component cannot be a symbolic link".to_string(),
                ));
            }
            Ok(metadata) if metadata.is_dir() => {
                validate_existing_directory_chain_no_symlink(parent).await?;
                break;
            }
            Ok(_) => {
                return Err(invalid_local_path(
                    parent,
                    "destination path component exists and is not a directory".to_string(),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                missing.push(parent.to_path_buf());
                current = parent;
            }
            Err(err) => {
                return Err(invalid_local_path(
                    parent,
                    format!("cannot inspect destination path component: {err}"),
                ));
            }
        }
    }

    for path in missing.into_iter().rev() {
        ensure_directory_component(&path).await?;
    }

    Ok(())
}

async fn validate_existing_directory_chain_no_symlink(path: &Path) -> Result<()> {
    let mut current = Some(path);
    while let Some(path) = current {
        if path.as_os_str().is_empty() {
            break;
        }

        match tokio::fs::symlink_metadata(path).await {
            Ok(metadata)
                if metadata.file_type().is_symlink() && is_platform_path_alias_symlink(path) => {}
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_local_path(
                    path,
                    "destination path component cannot be a symbolic link".to_string(),
                ));
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(invalid_local_path(
                    path,
                    "destination path component exists and is not a directory".to_string(),
                ));
            }
            Err(err) => {
                return Err(invalid_local_path(
                    path,
                    format!("cannot inspect destination path component: {err}"),
                ));
            }
        }

        current = path.parent();
    }
    Ok(())
}

fn local_destination_path(root: &Path, zip_path: &str) -> PathBuf {
    let mut path = root.to_path_buf();
    for component in zip_path.trim_end_matches('/').split('/') {
        if !component.is_empty() {
            path.push(component);
        }
    }
    path
}

fn validate_local_destination_path_collisions<'a>(
    entries: impl IntoIterator<Item = (&'a str, bool)>,
    case_insensitive: bool,
) -> Result<()> {
    let mut destinations = HashMap::<String, (String, bool)>::new();
    for (zip_path, is_directory) in entries {
        let local_path = zip_path.trim_end_matches('/').to_string();
        let destination_key = normalize_local_collision_key(&local_path, case_insensitive);
        if let Some((previous, _)) =
            destinations.insert(destination_key, (zip_path.to_string(), is_directory))
        {
            return Err(Error::InvalidZipEntry {
                path: zip_path.to_string(),
                reason: format!(
                    "maps to the same local destination path as ZIP entry `{previous}`"
                ),
            });
        }
    }

    let file_destinations = destinations
        .iter()
        .filter(|(_, (_, is_directory))| !is_directory)
        .map(|(local_path, (zip_path, _))| (local_path.as_str(), zip_path.as_str()))
        .collect::<HashMap<_, _>>();
    for (local_path, (zip_path, _)) in &destinations {
        let mut ancestor = String::new();
        let mut components = local_path.split('/').peekable();
        while let Some(component) = components.next() {
            if components.peek().is_none() {
                break;
            }
            if !ancestor.is_empty() {
                ancestor.push('/');
            }
            ancestor.push_str(component);
            if let Some(file_zip_path) = file_destinations.get(ancestor.as_str()) {
                return Err(Error::InvalidZipEntry {
                    path: zip_path.clone(),
                    reason: format!(
                        "maps under the same local destination path as file ZIP entry `{file_zip_path}`"
                    ),
                });
            }
        }
    }

    Ok(())
}

fn validate_local_destination_zip_paths<'a>(
    zip_paths: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    for zip_path in zip_paths {
        for component in zip_path.trim_end_matches('/').split('/') {
            validate_local_destination_component(zip_path, component)?;
        }
    }
    Ok(())
}

fn validate_local_destination_component(zip_path: &str, component: &str) -> Result<()> {
    let has_windows_reserved_character = component
        .chars()
        .any(|character| matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*'));
    let has_control_character = component.chars().any(|character| character.is_control());
    let has_reserved_suffix = component.ends_with([' ', '.']);
    let stem = component
        .split_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(component)
        .trim_end_matches([' ', '.']);
    let upper_stem = stem.to_ascii_uppercase();
    let is_reserved_device_name = matches!(
        upper_stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || is_numbered_windows_device_name(&upper_stem, "COM")
        || is_numbered_windows_device_name(&upper_stem, "LPT");

    if has_windows_reserved_character
        || has_control_character
        || has_reserved_suffix
        || is_reserved_device_name
    {
        Err(Error::InvalidZipEntry {
            path: zip_path.to_string(),
            reason: format!("unsafe local path component `{component}`"),
        })
    } else {
        Ok(())
    }
}

fn is_numbered_windows_device_name(value: &str, prefix: &str) -> bool {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return false;
    };
    matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
}

fn normalize_local_collision_key(path: &str, case_insensitive: bool) -> String {
    if case_insensitive {
        path.to_lowercase()
    } else {
        path.to_string()
    }
}

async fn reject_local_unzip_source_archive_target(
    source_zip: &Path,
    destination_root: &Path,
    entries: &[LocalZipEntry],
) -> Result<()> {
    let source_zip = tokio::fs::canonicalize(source_zip).await.map_err(|err| {
        invalid_local_path(source_zip, format!("cannot inspect source ZIP: {err}"))
    })?;

    for entry in entries {
        let destination = local_destination_path(destination_root, &entry.zip_path);
        match tokio::fs::canonicalize(&destination).await {
            Ok(destination) if destination == source_zip => {
                return Err(invalid_local_path(
                    &destination,
                    format!(
                        "ZIP entry `{}` would overwrite the source archive {}",
                        entry.zip_path,
                        source_zip.display()
                    ),
                ));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(invalid_local_path(
                    &destination,
                    format!("cannot inspect destination path: {err}"),
                ));
            }
        }
    }

    Ok(())
}

async fn local_destination_exists(root: &Path, entry: &LocalZipEntry) -> bool {
    tokio::fs::symlink_metadata(local_destination_path(root, &entry.zip_path))
        .await
        .is_ok()
}

async fn path_is_directory(path: &Path) -> bool {
    tokio::fs::symlink_metadata(path)
        .await
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

async fn create_directory_no_symlink(root: &Path, zip_path: &str) -> Result<()> {
    let mut current = root.to_path_buf();
    for component in zip_path.trim_end_matches('/').split('/') {
        if component.is_empty() {
            continue;
        }
        current.push(component);
        ensure_directory_component(&current).await?;
    }
    Ok(())
}

async fn create_parent_dirs_for_file(root: &Path, zip_path: &str) -> Result<()> {
    let Some((parent, _file_name)) = zip_path.rsplit_once('/') else {
        return Ok(());
    };
    create_directory_no_symlink(root, parent).await
}

async fn ensure_directory_component(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_local_path(
            path,
            "destination path component cannot be a symbolic link".to_string(),
        )),
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(invalid_local_path(
            path,
            "destination path component exists and is not a directory".to_string(),
        )),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            create_directory_component(path).await
        }
        Err(err) => Err(invalid_local_path(
            path,
            format!("cannot inspect destination path component: {err}"),
        )),
    }
}

async fn create_directory_component(path: &Path) -> Result<()> {
    match tokio::fs::create_dir(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            match tokio::fs::symlink_metadata(path).await {
                Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_local_path(
                    path,
                    "destination path component cannot be a symbolic link".to_string(),
                )),
                Ok(metadata) if metadata.is_dir() => Ok(()),
                Ok(_) => Err(invalid_local_path(
                    path,
                    "destination path component exists and is not a directory".to_string(),
                )),
                Err(err) => Err(invalid_local_path(
                    path,
                    format!("cannot inspect destination path component after create race: {err}"),
                )),
            }
        }
        Err(err) => Err(invalid_local_path(
            path,
            format!("cannot create directory: {err}"),
        )),
    }
}

async fn reject_existing_symlink_or_directory(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_local_path(
            path,
            "destination file cannot be a symbolic link".to_string(),
        )),
        Ok(metadata) if metadata.is_dir() => Err(invalid_local_path(
            path,
            "destination file path is a directory".to_string(),
        )),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(invalid_local_path(
            path,
            format!("cannot inspect destination file: {err}"),
        )),
    }
}

fn validate_local_zip_sync_options(options: &LocalZipSyncOptions) -> Result<()> {
    validate_upload_stream_options(options.body_chunk_size, options.pipe_capacity)?;
    if options.delete_extra && options.destination.prefix.is_empty() {
        return Err(Error::InvalidOption(
            "delete_extra requires a non-empty destination prefix".to_string(),
        ));
    }
    if options.concurrency == 0 {
        return Err(Error::InvalidOption(
            "concurrency must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_local_unzip_options(options: &LocalUnzipOptions) -> Result<()> {
    if options.concurrency == 0 {
        return Err(Error::InvalidOption(
            "concurrency must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_s3_zip_local_unzip_options(options: &S3ZipLocalUnzipOptions) -> Result<()> {
    if options.concurrency == 0 {
        return Err(Error::InvalidOption(
            "concurrency must be greater than zero".to_string(),
        ));
    }
    if options.source_get_concurrency == 0 {
        return Err(Error::InvalidOption(
            "source_get_concurrency must be greater than zero".to_string(),
        ));
    }
    if options.source_block_size == 0 {
        return Err(Error::InvalidOption(
            "source_block_size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_s3_zip_local_source_range_options(
    options: &S3ZipLocalUnzipOptions,
    source_len: u64,
) -> Result<()> {
    let mut sync = local_unzip_source_sync_options(options);
    sync.source_window_capacity = options.source_window_capacity;
    validate_source_range_options(&sync, source_len)
}

fn resolve_s3_zip_local_window_capacity(
    options: &mut S3ZipLocalUnzipOptions,
    source_zip_bytes: u64,
    zip_file_count: usize,
) {
    if let Some(memory_mb) = options.source_window_memory_budget_mb {
        options.source_window_capacity = adaptive_source_window_capacity(
            memory_mb,
            source_zip_bytes,
            options.concurrency,
            zip_file_count,
            options.source_block_size,
            options.source_get_concurrency,
        );
    }
}

fn local_unzip_source_sync_options(options: &S3ZipLocalUnzipOptions) -> SyncOptions {
    let mut sync = SyncOptions::new(
        options.source.clone(),
        S3Prefix {
            bucket: "__local__".to_string(),
            prefix: String::new(),
        },
    );
    sync.collect_diagnostics = options.collect_diagnostics;
    sync.ignore_embedded_catalog = options.ignore_embedded_catalog;
    sync.collect_operations = options.collect_operations;
    sync.concurrency = options.concurrency;
    sync.source_block_size = options.source_block_size;
    sync.source_block_merge_gap = options.source_block_merge_gap;
    sync.source_get_concurrency = options.source_get_concurrency;
    sync.source_window_capacity = options.source_window_capacity;
    sync.source_window_memory_budget_mb = options.source_window_memory_budget_mb;
    sync
}

fn validate_upload_stream_options(body_chunk_size: usize, pipe_capacity: usize) -> Result<()> {
    if body_chunk_size == 0 {
        return Err(Error::InvalidOption(
            "body_chunk_size must be greater than zero".to_string(),
        ));
    }
    if body_chunk_size > MAX_BODY_CHUNK_SIZE {
        return Err(Error::InvalidOption(format!(
            "body_chunk_size must be less than or equal to {MAX_BODY_CHUNK_SIZE}"
        )));
    }
    if pipe_capacity == 0 {
        return Err(Error::InvalidOption(
            "pipe_capacity must be greater than zero".to_string(),
        ));
    }
    if pipe_capacity > MAX_PIPE_CAPACITY {
        return Err(Error::InvalidOption(format!(
            "pipe_capacity must be less than or equal to {MAX_PIPE_CAPACITY}"
        )));
    }
    Ok(())
}

fn log_operation_issue(operation: &ObjectReport, progress: &ExtractProgress) {
    match operation.status {
        OperationStatus::Error => {
            let error_count = progress.errors.load(Ordering::Relaxed);
            if should_log_issue(error_count) {
                tracing::warn!(
                    error_count,
                    key = %operation.key,
                    zip_path = ?operation.zip_path.as_deref(),
                    destination_etag = ?operation.destination_etag.as_deref(),
                    message = ?operation.message.as_deref(),
                    "entry processing error"
                );
            }
        }
        OperationStatus::ConditionalConflict => {
            let conflict_count = progress.conditional_conflicts.load(Ordering::Relaxed);
            if should_log_issue(conflict_count) {
                tracing::warn!(
                    conflict_count,
                    key = %operation.key,
                    zip_path = ?operation.zip_path.as_deref(),
                    destination_etag = ?operation.destination_etag.as_deref(),
                    message = ?operation.message.as_deref(),
                    "entry conditional write conflict"
                );
            }
        }
        OperationStatus::UploadedNew
        | OperationStatus::UploadedChanged
        | OperationStatus::SkippedUnchanged
        | OperationStatus::DeletedExtra => {}
    }
}

fn should_log_issue(count: usize) -> bool {
    count <= 20 || count.is_multiple_of(100)
}

fn record_operation(
    summary: &mut SyncSummary,
    operations: &mut Vec<ObjectReport>,
    progress: &ExtractProgress,
    options: &SyncOptions,
    operation: ObjectReport,
    fail_fast_error: &mut Option<Error>,
    update_progress: bool,
) {
    summarize_operation(summary, &operation);
    if update_progress {
        progress.record_operation(&operation);
        log_operation_issue(&operation, progress);
    }
    if let Some(err) = conditional_conflict_error(
        &options.destination,
        &operation,
        options.fail_on_conditional_conflict,
    ) {
        *fail_fast_error = Some(err);
    }
    if options.collect_operations {
        operations.push(operation);
    }
}

pub(crate) fn conditional_conflict_error(
    destination: &S3Prefix,
    operation: &ObjectReport,
    fail_fast: bool,
) -> Option<Error> {
    if !fail_fast || operation.status != OperationStatus::ConditionalConflict {
        return None;
    }

    Some(Error::ConditionalConflict {
        bucket: destination.bucket.clone(),
        key: operation.key.clone(),
        message: operation
            .message
            .clone()
            .unwrap_or_else(|| "destination object changed after listing".to_string()),
    })
}

async fn list_destination(
    client: &Client,
    destination: &S3Prefix,
) -> Result<HashMap<String, DestinationObject>> {
    let mut result = HashMap::new();
    let mut continuation = None::<String>;
    let list_prefix = normalized_list_prefix(&destination.prefix);

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(&destination.bucket)
            .prefix(&list_prefix);

        if let Some(token) = continuation.take() {
            request = request.continuation_token(token);
        }

        let output = request.send().await.map_err(|err| Error::S3 {
            operation: "ListObjectsV2",
            bucket: destination.bucket.clone(),
            key: list_prefix.clone(),
            message: aws_error_message(&err),
        })?;

        for object in output.contents() {
            if let Some(key) = object.key() {
                result.insert(
                    key.to_string(),
                    DestinationObject {
                        etag: object.e_tag().map(str::to_string),
                        size: object.size().and_then(|size| u64::try_from(size).ok()),
                    },
                );
            }
        }

        if output.is_truncated().unwrap_or(false) {
            continuation = output.next_continuation_token().map(str::to_string);
            if continuation.is_none() {
                return Err(Error::S3 {
                    operation: "ListObjectsV2",
                    bucket: destination.bucket.clone(),
                    key: list_prefix.clone(),
                    message: "response was truncated without a continuation token".to_string(),
                });
            }
        } else {
            break;
        }
    }

    Ok(result)
}

pub(crate) fn normalized_list_prefix(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

#[derive(Default)]
struct ClassifiedEntries {
    reports: Vec<ObjectReport>,
    hash_jobs: Vec<HashJob>,
    upload_jobs: Vec<UploadJob>,
}

#[derive(Clone)]
struct HashJob {
    entry: ManifestEntry,
    destination_etag: String,
    destination_md5: String,
}

#[derive(Clone)]
struct UploadJob {
    entry: ManifestEntry,
    condition: PutCondition,
    comparison_digest: Option<ExtractDigest>,
}

enum HashPhaseResult {
    Operation(ObjectReport),
    Upload(UploadJob),
}

fn classify_entries(
    entries: Vec<ManifestEntry>,
    destination_objects: &HashMap<String, DestinationObject>,
) -> ClassifiedEntries {
    let mut classified = ClassifiedEntries::default();

    for entry in entries {
        let existing = destination_objects.get(&entry.key);
        if entry.is_directory {
            classify_directory_entry(entry, existing, &mut classified);
            continue;
        }

        if let Some(report) = catalog_skip_report(&entry, existing) {
            classified.reports.push(report);
            continue;
        }

        let Some(destination) = existing else {
            classified.upload_jobs.push(UploadJob {
                entry,
                condition: PutCondition::IfNoneMatch,
                comparison_digest: None,
            });
            continue;
        };

        let Some(destination_etag) = destination.etag.clone() else {
            classified.reports.push(entry_error(
                &entry,
                None,
                "destination object was listed without an ETag".to_string(),
            ));
            continue;
        };

        if entry.catalog_md5.is_some() {
            classified.upload_jobs.push(UploadJob {
                entry,
                condition: PutCondition::IfMatch(destination_etag),
                comparison_digest: None,
            });
            continue;
        }

        if let Some(destination_md5) =
            comparable_destination_md5(destination, &destination_etag, &entry)
        {
            classified.hash_jobs.push(HashJob {
                entry,
                destination_etag,
                destination_md5,
            });
        } else {
            classified.upload_jobs.push(UploadJob {
                entry,
                condition: PutCondition::IfMatch(destination_etag),
                comparison_digest: None,
            });
        }
    }

    classified
}

fn classify_directory_entry(
    entry: ManifestEntry,
    existing: Option<&DestinationObject>,
    classified: &mut ClassifiedEntries,
) {
    let Some(destination) = existing else {
        classified.upload_jobs.push(UploadJob {
            entry,
            condition: PutCondition::IfNoneMatch,
            comparison_digest: None,
        });
        return;
    };

    match destination.size {
        Some(0) => classified.reports.push(ObjectReport {
            status: OperationStatus::SkippedUnchanged,
            key: entry.key,
            zip_path: Some(entry.zip_path),
            size: Some(0),
            md5: Some(empty_md5()),
            destination_etag: destination.etag.clone(),
            message: None,
        }),
        Some(size) => classified.reports.push(entry_error(
            &entry,
            destination.etag.clone(),
            format!("destination directory marker key exists with nonzero size {size}"),
        )),
        None => classified.reports.push(entry_error(
            &entry,
            destination.etag.clone(),
            "destination directory marker was listed without a size".to_string(),
        )),
    }
}

async fn run_hash_phase(
    source: Arc<SourceClient>,
    jobs: Vec<HashJob>,
    options: &SyncOptions,
    source_len: u64,
    diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
) -> Vec<HashPhaseResult> {
    let entries = jobs.iter().map(|job| job.entry.clone()).collect::<Vec<_>>();
    let (store, scheduler) = start_source_phase(
        Arc::clone(&source),
        &entries,
        options,
        source_len,
        diagnostics,
    );
    let mut results = Vec::with_capacity(jobs.len());
    let mut stream = stream::iter(jobs)
        .map(|job| {
            let store = Arc::clone(&store);
            async move {
                match extract_digest(store, &job.entry).await {
                    Ok(digest) if digest.md5 == job.destination_md5 => {
                        HashPhaseResult::Operation(ObjectReport {
                            status: OperationStatus::SkippedUnchanged,
                            key: job.entry.key,
                            zip_path: Some(job.entry.zip_path),
                            size: Some(digest.bytes),
                            md5: Some(digest.md5),
                            destination_etag: Some(job.destination_etag),
                            message: None,
                        })
                    }
                    Ok(digest) => HashPhaseResult::Upload(UploadJob {
                        entry: job.entry,
                        condition: PutCondition::IfMatch(job.destination_etag),
                        comparison_digest: Some(digest),
                    }),
                    Err(err) => HashPhaseResult::Operation(entry_error(
                        &job.entry,
                        Some(job.destination_etag),
                        err.to_string(),
                    )),
                }
            }
        })
        .buffer_unordered(options.concurrency);

    while let Some(result) = stream.next().await {
        results.push(result);
    }
    let _ = scheduler.await;
    results
}

async fn run_upload_phase(
    client: Client,
    source: Arc<SourceClient>,
    jobs: Vec<UploadJob>,
    options: &SyncOptions,
    source_len: u64,
    observers: PhaseObservers,
) -> Vec<ObjectReport> {
    let entries = jobs.iter().map(|job| job.entry.clone()).collect::<Vec<_>>();
    let put_diagnostics_for_throttle = observers.put_diagnostics.clone();
    let context = Arc::new(UploadPhaseContext {
        client,
        put_diagnostics: observers.put_diagnostics,
        put_semaphore: Arc::new(Semaphore::new(options.put_concurrency.max(1))),
        put_throttle: Arc::new(PutThrottle::new(put_diagnostics_for_throttle)),
    });
    let (store, scheduler) = start_source_phase(
        Arc::clone(&source),
        &entries,
        options,
        source_len,
        observers.source_diagnostics,
    );
    let mut reports = Vec::with_capacity(jobs.len());
    let mut stream = stream::iter(jobs)
        .map(|job| {
            let context = Arc::clone(&context);
            let store = Arc::clone(&store);
            let task_options = options.clone();
            async move { upload_entry_job(context, store, job, &task_options).await }
        })
        .buffer_unordered(options.concurrency);

    let mut stopped_early = false;
    while let Some(report) = stream.next().await {
        observers.progress.record_operation(&report);
        log_operation_issue(&report, &observers.progress);
        stopped_early = options.fail_on_conditional_conflict
            && report.status == OperationStatus::ConditionalConflict;
        reports.push(report);
        if stopped_early {
            break;
        }
    }
    drop(stream);
    if stopped_early {
        scheduler.abort();
        let _ = scheduler.await;
    } else {
        let _ = scheduler.await;
    }
    reports
}

fn start_source_phase(
    source: Arc<SourceClient>,
    entries: &[ManifestEntry],
    options: &SyncOptions,
    source_len: u64,
    diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
) -> (Arc<BlockStore>, JoinHandle<()>) {
    let plan = plan_source_blocks(
        entries,
        source_len,
        options.source_block_size,
        options.source_block_merge_gap,
    );
    let store = BlockStore::with_source(
        plan,
        entries,
        options.source_window_capacity,
        diagnostics,
        source,
        options.source_get_concurrency,
    );
    let scheduler = start_source_scheduler(Arc::clone(&store));
    (store, scheduler)
}

pub(crate) fn catalog_skip_report(
    entry: &ManifestEntry,
    existing: Option<&DestinationObject>,
) -> Option<ObjectReport> {
    let catalog_md5 = entry.catalog_md5.as_ref()?;
    let destination = existing?;
    let destination_etag = destination.etag.clone()?;
    let destination_md5 = normalize_etag(&destination_etag)?;

    (destination_md5 == *catalog_md5).then(|| ObjectReport {
        status: OperationStatus::SkippedUnchanged,
        key: entry.key.clone(),
        zip_path: Some(entry.zip_path.clone()),
        size: Some(entry.size),
        md5: Some(catalog_md5.clone()),
        destination_etag: Some(destination_etag),
        message: None,
    })
}

pub(crate) fn comparable_destination_md5(
    destination: &DestinationObject,
    destination_etag: &str,
    entry: &ManifestEntry,
) -> Option<String> {
    if destination.size.is_some_and(|size| size != entry.size) {
        return None;
    }

    normalize_etag(destination_etag)
}

struct UploadPhaseContext {
    client: Client,
    put_diagnostics: Option<Arc<PutDiagnosticsCollector>>,
    put_semaphore: Arc<Semaphore>,
    put_throttle: Arc<PutThrottle>,
}

async fn upload_entry_job(
    context: Arc<UploadPhaseContext>,
    store: Arc<BlockStore>,
    job: UploadJob,
    options: &SyncOptions,
) -> ObjectReport {
    let destination_etag = match &job.condition {
        PutCondition::IfNoneMatch => None,
        PutCondition::IfMatch(etag) => Some(etag.clone()),
    };
    let source_context = PutSourceContext {
        store,
        put_diagnostics: context.put_diagnostics.clone(),
        put_semaphore: Arc::clone(&context.put_semaphore),
        put_throttle: Arc::clone(&context.put_throttle),
    };
    match put_entry_stream(
        &context.client,
        source_context,
        &job.entry,
        job.condition.clone(),
        options,
    )
    .await
    {
        PutResult::Uploaded(upload_digest) => {
            let status = if destination_etag.is_some() {
                OperationStatus::UploadedChanged
            } else {
                OperationStatus::UploadedNew
            };
            ObjectReport {
                status,
                key: job.entry.key,
                zip_path: Some(job.entry.zip_path),
                size: Some(upload_digest.bytes),
                md5: Some(upload_digest.md5),
                destination_etag,
                message: None,
            }
        }
        PutResult::ConditionalConflict(message) => ObjectReport {
            status: OperationStatus::ConditionalConflict,
            key: job.entry.key,
            zip_path: Some(job.entry.zip_path),
            size: job
                .comparison_digest
                .as_ref()
                .map(|digest| digest.bytes)
                .or(Some(job.entry.size)),
            md5: job
                .comparison_digest
                .map(|digest| digest.md5)
                .or_else(|| job.entry.catalog_md5.clone()),
            destination_etag,
            message: Some(message),
        },
        PutResult::Failed(message) => entry_error(&job.entry, destination_etag, message),
    }
}

#[derive(Clone)]
enum PutCondition {
    IfNoneMatch,
    IfMatch(String),
}

enum PutResult {
    Uploaded(ExtractDigest),
    ConditionalConflict(String),
    Failed(String),
}

struct PhaseObservers {
    source_diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
    put_diagnostics: Option<Arc<PutDiagnosticsCollector>>,
    progress: Arc<ExtractProgress>,
}

struct PutSourceContext {
    store: Arc<BlockStore>,
    put_diagnostics: Option<Arc<PutDiagnosticsCollector>>,
    put_semaphore: Arc<Semaphore>,
    put_throttle: Arc<PutThrottle>,
}

async fn put_entry_stream(
    client: &Client,
    source_context: PutSourceContext,
    entry: &ManifestEntry,
    condition: PutCondition,
    options: &SyncOptions,
) -> PutResult {
    let mut last_failure = None;
    let max_attempts = options.put_retry_policy.max_attempts;

    for attempt in 1..=max_attempts {
        source_context.put_throttle.wait().await;
        let Ok(put_permit) = source_context.put_semaphore.acquire().await else {
            return PutResult::Failed("destination PUT semaphore is closed".to_string());
        };

        let replay_scheduler =
            (attempt > 1).then(|| source_context.store.start_entry_replay(entry));
        let result = put_entry_stream_once(
            client,
            Arc::clone(&source_context.store),
            entry,
            &condition,
            options,
            source_context.put_diagnostics.as_deref(),
        )
        .await;
        drop(put_permit);
        if let Some(replay_scheduler) = replay_scheduler {
            let _ = replay_scheduler.await;
        }

        match result {
            PutAttemptResult::Uploaded(digest) => return PutResult::Uploaded(digest),
            PutAttemptResult::ConditionalConflict(message) => {
                return PutResult::ConditionalConflict(message);
            }
            PutAttemptResult::Failed {
                message,
                retryable,
                error_code,
                failure_count,
            } => {
                if should_log_issue(usize::try_from(failure_count).unwrap_or(usize::MAX)) {
                    tracing::warn!(
                        attempt,
                        max_attempts,
                        retryable,
                        error_code = ?error_code.as_deref(),
                        key = %entry.key,
                        zip_path = %entry.zip_path,
                        size = entry.size,
                        message = %message,
                        "destination PUT attempt failed"
                    );
                }
                if retryable && attempt < max_attempts {
                    if let Some(diagnostics) = source_context.put_diagnostics.as_deref() {
                        diagnostics.record_retry();
                    }
                    let throttled = error_code
                        .as_deref()
                        .is_some_and(is_put_throttle_error_code);
                    let delay = put_retry_delay(&options.put_retry_policy, attempt, throttled);
                    if throttled {
                        source_context.put_throttle.throttle(delay);
                    } else {
                        tokio::time::sleep(delay).await;
                    }
                    last_failure = Some(message);
                    continue;
                }
                return PutResult::Failed(message);
            }
        }
    }

    PutResult::Failed(last_failure.unwrap_or_else(|| "PutObject failed".to_string()))
}

fn put_retry_delay(policy: &PutRetryPolicy, attempt: usize, throttled: bool) -> Duration {
    let (base, max) = if throttled {
        (policy.slowdown_base_delay, policy.slowdown_max_delay)
    } else {
        (policy.base_delay, policy.max_delay)
    };
    let delay = capped_exponential_delay(base, max, attempt);
    match policy.jitter {
        RetryJitter::Full => full_jitter(delay),
        RetryJitter::None => delay,
    }
}

fn put_retry_diagnostics(policy: &PutRetryPolicy) -> PutRetryDiagnostics {
    PutRetryDiagnostics {
        max_attempts: policy.max_attempts,
        base_delay_ms: duration_millis_u64(policy.base_delay),
        max_delay_ms: duration_millis_u64(policy.max_delay),
        slowdown_base_delay_ms: duration_millis_u64(policy.slowdown_base_delay),
        slowdown_max_delay_ms: duration_millis_u64(policy.slowdown_max_delay),
        jitter: policy.jitter,
    }
}

fn capped_exponential_delay(base: Duration, max: Duration, attempt: usize) -> Duration {
    let shift = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
    let delay_ms = duration_millis_u64(base)
        .saturating_mul(multiplier)
        .min(duration_millis_u64(max));
    Duration::from_millis(delay_ms)
}

fn full_jitter(delay: Duration) -> Duration {
    let millis = duration_millis_u64(delay);
    if millis == 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(fastrand::u64(0..=millis))
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

enum PutAttemptResult {
    Uploaded(ExtractDigest),
    ConditionalConflict(String),
    Failed {
        message: String,
        retryable: bool,
        error_code: Option<String>,
        failure_count: u64,
    },
}

struct AbortOnDropJoinHandle<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropJoinHandle<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    fn is_finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    async fn join(&mut self) -> std::result::Result<T, tokio::task::JoinError> {
        self.handle
            .take()
            .expect("abort-on-drop join handle has not been joined")
            .await
    }
}

impl<T> Drop for AbortOnDropJoinHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

async fn put_entry_stream_once(
    client: &Client,
    store: Arc<BlockStore>,
    entry: &ManifestEntry,
    condition: &PutCondition,
    options: &SyncOptions,
    put_diagnostics: Option<&PutDiagnosticsCollector>,
) -> PutAttemptResult {
    if entry.is_directory {
        return put_directory_marker_once(client, entry, condition, options, put_diagnostics).await;
    }

    if entry.size == 0 {
        return put_zero_length_entry_once(
            client,
            store,
            entry,
            condition,
            options,
            put_diagnostics,
        )
        .await;
    }

    let entry_body_reader = match entry_reader(store, entry).await {
        Ok(reader) => reader,
        Err(err) => {
            return PutAttemptResult::Failed {
                retryable: producer_error_is_retryable(&err),
                message: err.to_string(),
                error_code: None,
                failure_count: 0,
            };
        }
    };

    let (writer, reader) = tokio::io::duplex(options.pipe_capacity);
    let producer_entry = entry.clone();
    let mut producer = AbortOnDropJoinHandle::new(tokio::spawn(async move {
        write_extracted_entry(writer, entry_body_reader, producer_entry).await
    }));

    let stream = ReaderStream::with_capacity(reader, options.body_chunk_size).map_ok(Frame::data);
    let body = ByteStream::new(SdkBody::from_body_1_x(StreamBody::new(stream)));
    let content_length = match i64::try_from(entry.size) {
        Ok(length) => length,
        Err(_) => {
            producer.abort();
            return PutAttemptResult::Failed {
                message: format!("entry size {} does not fit S3 content length", entry.size),
                retryable: false,
                error_code: None,
                failure_count: 0,
            };
        }
    };

    let mut request = client
        .put_object()
        .bucket(&options.destination.bucket)
        .key(&entry.key)
        .content_length(content_length)
        .body(body);

    request = match condition {
        PutCondition::IfNoneMatch => request.if_none_match("*"),
        PutCondition::IfMatch(etag) => request.if_match(etag.as_str()),
    };

    match request.send().await {
        Ok(_) => match producer.join().await {
            Ok(Ok(digest)) => PutAttemptResult::Uploaded(digest),
            Ok(Err(err)) => PutAttemptResult::Failed {
                retryable: producer_error_is_retryable(&err),
                message: err.to_string(),
                error_code: None,
                failure_count: 0,
            },
            Err(err) => PutAttemptResult::Failed {
                message: err.to_string(),
                retryable: false,
                error_code: None,
                failure_count: 0,
            },
        },
        Err(err) if is_conditional_put_conflict(&err) => {
            record_put_failure(put_diagnostics, &err);
            producer.abort();
            let _ = producer.join().await;
            PutAttemptResult::ConditionalConflict(aws_error_context(&err))
        }
        Err(err) => {
            let (error_code, failure_count) = record_put_failure(put_diagnostics, &err);
            let message = format!("{}: {}", put_sdk_error_kind(&err), aws_error_context(&err));
            if let Some(result) = producer_result_after_send_error(&mut producer).await {
                put_failure_after_s3_error(result, message, Some(error_code), failure_count)
            } else {
                producer.abort();
                let _ = producer.join().await;
                PutAttemptResult::Failed {
                    message,
                    retryable: true,
                    error_code: Some(error_code),
                    failure_count,
                }
            }
        }
    }
}

async fn put_directory_marker_once(
    client: &Client,
    entry: &ManifestEntry,
    condition: &PutCondition,
    options: &SyncOptions,
    put_diagnostics: Option<&PutDiagnosticsCollector>,
) -> PutAttemptResult {
    let mut request = client
        .put_object()
        .bucket(&options.destination.bucket)
        .key(&entry.key)
        .content_length(0)
        .body(ByteStream::from_static(b""));

    request = match condition {
        PutCondition::IfNoneMatch => request.if_none_match("*"),
        PutCondition::IfMatch(etag) => request.if_match(etag.as_str()),
    };

    match request.send().await {
        Ok(_) => PutAttemptResult::Uploaded(ExtractDigest {
            bytes: 0,
            md5: empty_md5(),
        }),
        Err(err) if is_conditional_put_conflict(&err) => {
            record_put_failure(put_diagnostics, &err);
            PutAttemptResult::ConditionalConflict(aws_error_context(&err))
        }
        Err(err) => {
            let (error_code, failure_count) = record_put_failure(put_diagnostics, &err);
            PutAttemptResult::Failed {
                message: format!("{}: {}", put_sdk_error_kind(&err), aws_error_context(&err)),
                retryable: true,
                error_code: Some(error_code),
                failure_count,
            }
        }
    }
}

async fn put_zero_length_entry_once(
    client: &Client,
    store: Arc<BlockStore>,
    entry: &ManifestEntry,
    condition: &PutCondition,
    options: &SyncOptions,
    put_diagnostics: Option<&PutDiagnosticsCollector>,
) -> PutAttemptResult {
    let digest = match extract_digest(store, entry).await {
        Ok(digest) => digest,
        Err(err) => {
            return PutAttemptResult::Failed {
                retryable: producer_error_is_retryable(&err),
                message: err.to_string(),
                error_code: None,
                failure_count: 0,
            };
        }
    };

    let mut request = client
        .put_object()
        .bucket(&options.destination.bucket)
        .key(&entry.key)
        .content_length(0)
        .body(ByteStream::from_static(b""));

    request = match condition {
        PutCondition::IfNoneMatch => request.if_none_match("*"),
        PutCondition::IfMatch(etag) => request.if_match(etag.as_str()),
    };

    match request.send().await {
        Ok(_) => PutAttemptResult::Uploaded(digest),
        Err(err) if is_conditional_put_conflict(&err) => {
            record_put_failure(put_diagnostics, &err);
            PutAttemptResult::ConditionalConflict(aws_error_context(&err))
        }
        Err(err) => {
            let (error_code, failure_count) = record_put_failure(put_diagnostics, &err);
            PutAttemptResult::Failed {
                message: format!("{}: {}", put_sdk_error_kind(&err), aws_error_context(&err)),
                retryable: true,
                error_code: Some(error_code),
                failure_count,
            }
        }
    }
}

fn empty_md5() -> String {
    "d41d8cd98f00b204e9800998ecf8427e".to_string()
}

fn record_put_failure(
    diagnostics: Option<&PutDiagnosticsCollector>,
    err: &SdkError<PutObjectError>,
) -> (String, u64) {
    let error_code = put_failure_error_code(err);
    let failure_count = diagnostics
        .map(|diagnostics| diagnostics.record_failure(error_code.clone()))
        .unwrap_or(0);
    (error_code, failure_count)
}

async fn producer_result_after_send_error(
    producer: &mut AbortOnDropJoinHandle<Result<ExtractDigest>>,
) -> Option<std::result::Result<Result<ExtractDigest>, tokio::task::JoinError>> {
    if producer.is_finished() {
        return Some(producer.join().await);
    }

    tokio::time::sleep(PUT_OBJECT_PRODUCER_ERROR_GRACE).await;
    if producer.is_finished() {
        Some(producer.join().await)
    } else {
        None
    }
}

fn put_failure_after_s3_error(
    producer_result: std::result::Result<Result<ExtractDigest>, tokio::task::JoinError>,
    s3_message: String,
    error_code: Option<String>,
    failure_count: u64,
) -> PutAttemptResult {
    match producer_result {
        Ok(Ok(_)) => PutAttemptResult::Failed {
            message: s3_message,
            retryable: true,
            error_code,
            failure_count,
        },
        Ok(Err(err)) => PutAttemptResult::Failed {
            retryable: producer_error_is_retryable(&err),
            message: format!("{s3_message}; producer failed after PutObject error: {err}"),
            error_code,
            failure_count,
        },
        Err(err) => PutAttemptResult::Failed {
            message: format!("{s3_message}; producer task failed after PutObject error: {err}"),
            retryable: false,
            error_code,
            failure_count,
        },
    }
}

fn producer_error_is_retryable(err: &Error) -> bool {
    match err {
        Error::Io(err) => !matches!(
            err.kind(),
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput
        ),
        Error::S3 { .. } => true,
        Error::MultipartAbort { original, .. } => producer_error_is_retryable(original),
        Error::InvalidS3Uri { .. }
        | Error::InvalidLocalPath { .. }
        | Error::InvalidOption(_)
        | Error::ConditionalConflict { .. }
        | Error::InvalidZipEntry { .. }
        | Error::DuplicateZipPath(_)
        | Error::EntryTooLarge { .. }
        | Error::Zip(_)
        | Error::Join(_)
        | Error::Build(_) => false,
    }
}

fn put_sdk_error_kind(err: &SdkError<PutObjectError>) -> &'static str {
    match err {
        SdkError::ConstructionFailure(_) => "construction failure",
        SdkError::TimeoutError(_) => "timeout",
        SdkError::DispatchFailure(_) => "dispatch failure",
        SdkError::ResponseError(_) => "response error",
        SdkError::ServiceError(_) => "service error",
        _ => "sdk error",
    }
}

fn put_failure_error_code(err: &SdkError<PutObjectError>) -> String {
    err.code()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| put_sdk_error_kind(err).replace(' ', "_"))
}

fn is_put_throttle_error_code(code: &str) -> bool {
    matches!(
        code,
        "SlowDown"
            | "Throttling"
            | "ThrottlingException"
            | "TooManyRequestsException"
            | "RequestLimitExceeded"
            | "RequestThrottled"
            | "RequestThrottledException"
            | "ProvisionedThroughputExceededException"
            | "BandwidthLimitExceeded"
    )
}

async fn write_extracted_entry(
    mut writer: DuplexStream,
    mut reader: EntryReader,
    entry: ManifestEntry,
) -> Result<ExtractDigest> {
    let mut hasher = Md5::new();
    let mut crc32 = Crc32Hasher::new();
    let mut bytes = 0_u64;
    const BUFFER_SIZE: usize = 64 * 1024;
    let mut buffer = vec![0_u8; BUFFER_SIZE];
    let mut pending = vec![0_u8; BUFFER_SIZE];
    let mut pending_len = 0;

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        if pending_len != 0 {
            writer.write_all(&pending[..pending_len]).await?;
        }
        let next_bytes = bytes.saturating_add(read as u64);
        validate_extracted_size_not_exceeded(&entry, next_bytes)?;
        hasher.update(&buffer[..read]);
        crc32.update(&buffer[..read]);
        std::mem::swap(&mut pending, &mut buffer);
        pending_len = read;
        bytes = next_bytes;
    }

    validate_extracted_size(&entry, bytes)?;
    // Hold back the final chunk until CRC validation succeeds. If validation
    // fails, the S3 body is left short of Content-Length and PutObject fails.
    validate_crc32_value(entry.crc32, crc32.finalize())?;
    if pending_len != 0 {
        writer.write_all(&pending[..pending_len]).await?;
    }
    writer.shutdown().await?;

    Ok(ExtractDigest {
        bytes,
        md5: hex::encode(hasher.finalize()),
    })
}

async fn extract_digest(store: Arc<BlockStore>, entry: &ManifestEntry) -> Result<ExtractDigest> {
    let mut reader = entry_reader(store, entry).await?;
    let mut hasher = Md5::new();
    let mut crc32 = Crc32Hasher::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let next_bytes = bytes.saturating_add(read as u64);
        validate_extracted_size_not_exceeded(entry, next_bytes)?;
        hasher.update(&buffer[..read]);
        crc32.update(&buffer[..read]);
        bytes = next_bytes;
    }

    validate_extracted_size(entry, bytes)?;
    validate_crc32_value(entry.crc32, crc32.finalize())?;
    let md5 = hex::encode(hasher.finalize());

    Ok(ExtractDigest { bytes, md5 })
}

pub(crate) fn validate_extracted_size(entry: &ManifestEntry, bytes: u64) -> Result<()> {
    if bytes == entry.size {
        Ok(())
    } else {
        Err(extracted_size_error(entry, bytes))
    }
}

fn validate_extracted_size_not_exceeded(entry: &ManifestEntry, bytes: u64) -> Result<()> {
    if bytes <= entry.size {
        Ok(())
    } else {
        Err(extracted_size_error(entry, bytes))
    }
}

fn extracted_size_error(entry: &ManifestEntry, bytes: u64) -> Error {
    Error::InvalidZipEntry {
        path: entry.zip_path.clone(),
        reason: format!(
            "entry produced {bytes} bytes but central directory declared {} bytes",
            entry.size
        ),
    }
}

async fn delete_extra_objects(
    client: &Client,
    destination: &S3Prefix,
    keys: Vec<String>,
) -> Vec<ObjectReport> {
    let mut reports = Vec::new();

    for chunk in keys.chunks(1000) {
        let mut identifiers = Vec::with_capacity(chunk.len());
        for key in chunk {
            match ObjectIdentifier::builder().key(key).build() {
                Ok(identifier) => identifiers.push(identifier),
                Err(err) => {
                    reports.push(ObjectReport {
                        status: OperationStatus::Error,
                        key: key.clone(),
                        zip_path: None,
                        size: None,
                        md5: None,
                        destination_etag: None,
                        message: Some(err.to_string()),
                    });
                }
            }
        }

        if identifiers.is_empty() {
            continue;
        }

        let delete = match Delete::builder()
            .set_objects(Some(identifiers))
            .quiet(true)
            .build()
        {
            Ok(delete) => delete,
            Err(err) => {
                for key in chunk {
                    reports.push(ObjectReport {
                        status: OperationStatus::Error,
                        key: key.clone(),
                        zip_path: None,
                        size: None,
                        md5: None,
                        destination_etag: None,
                        message: Some(err.to_string()),
                    });
                }
                continue;
            }
        };

        match client
            .delete_objects()
            .bucket(&destination.bucket)
            .delete(delete)
            .send()
            .await
        {
            Ok(output) => {
                let failed = output
                    .errors()
                    .iter()
                    .filter_map(|err| {
                        err.key().map(|key| {
                            (
                                key.to_string(),
                                err.message().unwrap_or_default().to_string(),
                            )
                        })
                    })
                    .collect::<HashMap<_, _>>();
                for key in chunk {
                    if let Some(message) = failed.get(key) {
                        reports.push(ObjectReport {
                            status: OperationStatus::Error,
                            key: key.clone(),
                            zip_path: None,
                            size: None,
                            md5: None,
                            destination_etag: None,
                            message: Some(message.clone()),
                        });
                    } else {
                        reports.push(ObjectReport {
                            status: OperationStatus::DeletedExtra,
                            key: key.clone(),
                            zip_path: None,
                            size: None,
                            md5: None,
                            destination_etag: None,
                            message: None,
                        });
                    }
                }
            }
            Err(err) => {
                let message = aws_error_message(&err);
                for key in chunk {
                    reports.push(ObjectReport {
                        status: OperationStatus::Error,
                        key: key.clone(),
                        zip_path: None,
                        size: None,
                        md5: None,
                        destination_etag: None,
                        message: Some(message.clone()),
                    });
                }
            }
        }
    }

    reports
}

fn entry_error(
    entry: &ManifestEntry,
    destination_etag: Option<String>,
    message: String,
) -> ObjectReport {
    ObjectReport {
        status: OperationStatus::Error,
        key: entry.key.clone(),
        zip_path: Some(entry.zip_path.clone()),
        size: Some(entry.size),
        md5: None,
        destination_etag,
        message: Some(message),
    }
}

fn is_conditional_put_conflict(err: &SdkError<PutObjectError>) -> bool {
    if let SdkError::ServiceError(service) = err {
        let status = service.raw().status().as_u16();
        if status == 409 || status == 412 {
            return true;
        }
    }

    matches!(
        err.code(),
        Some("ConditionalRequestConflict" | "PreconditionFailed")
    )
}

pub(crate) fn validate_options(options: &SyncOptions) -> Result<()> {
    if options.delete_extra && options.destination.prefix.is_empty() {
        return Err(Error::InvalidOption(
            "delete_extra requires a non-empty destination prefix".to_string(),
        ));
    }
    if options.concurrency == 0 {
        return Err(Error::InvalidOption(
            "concurrency must be greater than zero".to_string(),
        ));
    }
    if options.put_concurrency == 0 {
        return Err(Error::InvalidOption(
            "put_concurrency must be greater than zero".to_string(),
        ));
    }
    validate_put_retry_policy(&options.put_retry_policy)?;
    if options.source_block_size == 0 {
        return Err(Error::InvalidOption(
            "source_block_size must be greater than zero".to_string(),
        ));
    }
    if options.source_get_concurrency == 0 {
        return Err(Error::InvalidOption(
            "source_get_concurrency must be greater than zero".to_string(),
        ));
    }
    if options.body_chunk_size == 0 {
        return Err(Error::InvalidOption(
            "body_chunk_size must be greater than zero".to_string(),
        ));
    }
    if options.body_chunk_size > MAX_BODY_CHUNK_SIZE {
        return Err(Error::InvalidOption(format!(
            "body_chunk_size must be less than or equal to {MAX_BODY_CHUNK_SIZE}"
        )));
    }
    if options.pipe_capacity == 0 {
        return Err(Error::InvalidOption(
            "pipe_capacity must be greater than zero".to_string(),
        ));
    }
    if options.pipe_capacity > MAX_PIPE_CAPACITY {
        return Err(Error::InvalidOption(format!(
            "pipe_capacity must be less than or equal to {MAX_PIPE_CAPACITY}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_source_range_options(
    options: &SyncOptions,
    source_zip_bytes: u64,
) -> Result<()> {
    let effective_block_size = u64::try_from(options.source_block_size)
        .unwrap_or(u64::MAX)
        .min(source_zip_bytes);
    if options.source_window_capacity != 0
        && effective_block_size > options.source_window_capacity as u64
    {
        return Err(Error::InvalidOption(
            "source_block_size must be less than or equal to source_window_capacity after clamping to the source ZIP size".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn resolve_source_window_capacity(
    options: &mut SyncOptions,
    source_zip_bytes: u64,
    zip_file_count: usize,
) {
    let Some(memory_mb) = options.source_window_memory_budget_mb else {
        return;
    };
    options.source_window_capacity = adaptive_source_window_capacity(
        memory_mb,
        source_zip_bytes,
        options.concurrency,
        zip_file_count,
        options.source_block_size,
        options.source_get_concurrency,
    );
}

fn validate_put_retry_policy(policy: &PutRetryPolicy) -> Result<()> {
    if policy.max_attempts == 0 {
        return Err(Error::InvalidOption(
            "put_retry_policy.max_attempts must be greater than zero".to_string(),
        ));
    }
    if policy.max_delay < policy.base_delay {
        return Err(Error::InvalidOption(
            "put_retry_policy.max_delay must be greater than or equal to base_delay".to_string(),
        ));
    }
    if policy.slowdown_max_delay < policy.slowdown_base_delay {
        return Err(Error::InvalidOption(
            "put_retry_policy.slowdown_max_delay must be greater than or equal to slowdown_base_delay".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_zip::Compression;
    use aws_sdk_s3::config::{Credentials, Region};

    use crate::range::{SourcePlan, SourceRange};
    use crate::{S3Object, S3Prefix};

    #[test]
    fn classify_directory_markers_for_preserved_s3_placeholders() {
        let entry = ManifestEntry {
            source_offset: 0,
            source_span_start: 0,
            source_span_end: 0,
            zip_path: "empty/".to_string(),
            key: "prefix/empty/".to_string(),
            size: 0,
            compressed_size: 0,
            compression: Compression::Stored,
            crc32: 0,
            catalog_md5: None,
            is_directory: true,
        };

        let classified = classify_entries(vec![entry.clone()], &std::collections::HashMap::new());
        assert_eq!(classified.upload_jobs.len(), 1);
        assert!(classified.reports.is_empty());

        let destination_objects = std::collections::HashMap::from([(
            entry.key.clone(),
            DestinationObject {
                etag: Some("\"d41d8cd98f00b204e9800998ecf8427e\"".to_string()),
                size: Some(0),
            },
        )]);
        let classified = classify_entries(vec![entry.clone()], &destination_objects);
        assert!(classified.upload_jobs.is_empty());
        assert_eq!(classified.reports.len(), 1);
        assert_eq!(
            classified.reports[0].status,
            OperationStatus::SkippedUnchanged
        );

        let destination_objects = std::collections::HashMap::from([(
            entry.key.clone(),
            DestinationObject {
                etag: Some("\"etag\"".to_string()),
                size: Some(1),
            },
        )]);
        let classified = classify_entries(vec![entry], &destination_objects);
        assert!(classified.upload_jobs.is_empty());
        assert_eq!(classified.reports.len(), 1);
        assert_eq!(classified.reports[0].status, OperationStatus::Error);
    }

    #[test]
    fn dry_run_s3_classification_reports_would_statuses_and_delete_extra() {
        let new_entry = manifest_entry("new.txt", 5, None);
        let changed_entry = manifest_entry("changed.txt", 5, None);
        let skipped_entry = manifest_entry(
            "same.txt",
            5,
            Some("2c1743a391305fbf367df8e4f069f9f9".to_string()),
        );
        let destination_objects = std::collections::HashMap::from([
            (
                changed_entry.key.clone(),
                DestinationObject {
                    etag: Some("\"different\"".to_string()),
                    size: Some(99),
                },
            ),
            (
                skipped_entry.key.clone(),
                DestinationObject {
                    etag: Some("\"2c1743a391305fbf367df8e4f069f9f9\"".to_string()),
                    size: Some(5),
                },
            ),
            (
                "prefix/extra.txt".to_string(),
                DestinationObject {
                    etag: Some("\"extra\"".to_string()),
                    size: Some(1),
                },
            ),
        ]);

        let expected_keys = [
            new_entry.key.clone(),
            changed_entry.key.clone(),
            skipped_entry.key.clone(),
        ]
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
        let classified = classify_entries(
            vec![new_entry, changed_entry, skipped_entry],
            &destination_objects,
        );
        let mut summary = UnzipDryRunSummary {
            zip_files: 3,
            destination_objects: destination_objects.len(),
            ..UnzipDryRunSummary::default()
        };
        let mut operations = Vec::new();

        for operation in classified.reports {
            record_dry_run_operation(
                &mut summary,
                &mut operations,
                true,
                dry_run_report_from_object_report(operation),
            );
        }
        for job in classified.upload_jobs {
            record_dry_run_operation(
                &mut summary,
                &mut operations,
                true,
                dry_run_upload_job_report(job),
            );
        }
        for key in destination_objects
            .keys()
            .filter(|key| !expected_keys.contains(*key))
        {
            record_dry_run_operation(
                &mut summary,
                &mut operations,
                true,
                dry_run_delete_extra_report(key.clone(), destination_objects.get(key)),
            );
        }

        assert_eq!(summary.would_upload_new, 1);
        assert_eq!(summary.would_upload_changed, 1);
        assert_eq!(summary.skipped_unchanged, 1);
        assert_eq!(summary.would_delete_extra, 1);
        assert!(operations.iter().any(|operation| {
            operation.status == DryRunOperationStatus::WouldDeleteExtra
                && operation.key == "prefix/extra.txt"
        }));
    }

    #[tokio::test]
    async fn dry_run_case_sensitivity_matches_destination_probe() {
        let destination = unique_test_dir("dry-run-case-probe");
        tokio::fs::create_dir_all(&destination).await.unwrap();
        tokio::fs::write(destination.join("sample.txt"), b"sample")
            .await
            .unwrap();

        let expected = destination_uses_case_insensitive_paths(&destination)
            .await
            .unwrap();
        let actual = destination_uses_case_insensitive_paths_for_dry_run(&destination)
            .await
            .unwrap();

        assert_eq!(actual, expected);
        tokio::fs::remove_dir_all(destination).await.unwrap();
    }

    #[tokio::test]
    async fn dry_run_case_sensitivity_matches_empty_destination_probe() {
        let destination = unique_test_dir("dry-run-empty-case-probe");
        tokio::fs::create_dir_all(&destination).await.unwrap();

        let expected = destination_uses_case_insensitive_paths(&destination)
            .await
            .unwrap();
        let actual = destination_uses_case_insensitive_paths_for_dry_run(&destination)
            .await
            .unwrap();

        assert_eq!(actual, expected);
        tokio::fs::remove_dir_all(destination).await.unwrap();
    }

    #[tokio::test]
    async fn dry_run_case_sensitivity_uses_existing_parent_for_missing_destination() {
        let destination_parent = unique_test_dir("dry-run-missing-case-probe");
        tokio::fs::create_dir_all(&destination_parent)
            .await
            .unwrap();
        tokio::fs::write(destination_parent.join("sample.txt"), b"sample")
            .await
            .unwrap();
        let missing_destination = destination_parent.join("missing");

        let expected = destination_uses_case_insensitive_paths(&destination_parent)
            .await
            .unwrap();
        let actual = destination_uses_case_insensitive_paths_for_dry_run(&missing_destination)
            .await
            .unwrap();

        assert_eq!(actual, expected);
        tokio::fs::remove_dir_all(destination_parent).await.unwrap();
    }

    #[tokio::test]
    async fn dry_run_case_sensitivity_uses_current_dir_for_missing_relative_destination() {
        let missing_destination = PathBuf::from(format!(
            "s3-unspool-missing-case-probe-{}",
            std::process::id()
        ));

        let probe_root = existing_case_probe_root(&missing_destination)
            .await
            .unwrap();

        assert_eq!(probe_root, PathBuf::from("."));
    }

    #[tokio::test]
    async fn local_zip_directory_marker_rejects_nonzero_existing_destination() {
        let entry = local_directory_entry("prefix/empty/");
        let existing = DestinationObject {
            etag: Some("\"etag\"".to_string()),
            size: Some(1),
        };
        let options = LocalZipSyncOptions::new(
            PathBuf::from("site.zip"),
            S3Prefix::parse("s3://destination-bucket/prefix/").unwrap(),
        );

        let report =
            put_local_directory_marker(&dummy_s3_client(), entry, Some(&existing), &options).await;

        assert_eq!(report.status, OperationStatus::Error);
        assert_eq!(report.destination_etag.as_deref(), Some("\"etag\""));
        assert!(matches!(
            report.message.as_deref(),
            Some(message) if message.contains("nonzero size 1")
        ));
    }

    #[tokio::test]
    async fn local_zip_directory_marker_rejects_unknown_size_existing_destination() {
        let entry = local_directory_entry("prefix/empty/");
        let existing = DestinationObject {
            etag: Some("\"etag\"".to_string()),
            size: None,
        };
        let options = LocalZipSyncOptions::new(
            PathBuf::from("site.zip"),
            S3Prefix::parse("s3://destination-bucket/prefix/").unwrap(),
        );

        let report =
            put_local_directory_marker(&dummy_s3_client(), entry, Some(&existing), &options).await;

        assert_eq!(report.status, OperationStatus::Error);
        assert_eq!(report.destination_etag.as_deref(), Some("\"etag\""));
        assert!(matches!(
            report.message.as_deref(),
            Some(message) if message.contains("without a size")
        ));
    }

    #[tokio::test]
    async fn producer_error_after_send_error_is_preserved() {
        let mut producer = AbortOnDropJoinHandle::new(tokio::spawn(async {
            Err(Error::InvalidZipEntry {
                path: "bad.txt".to_string(),
                reason: "CRC mismatch".to_string(),
            })
        }));
        while !producer.is_finished() {
            tokio::task::yield_now().await;
        }

        let result = producer_result_after_send_error(&mut producer)
            .await
            .expect("producer should be ready");

        match put_failure_after_s3_error(
            result,
            "S3 failed".to_string(),
            Some("SlowDown".into()),
            7,
        ) {
            PutAttemptResult::Failed {
                message,
                retryable,
                error_code,
                failure_count,
            } => {
                assert!(!retryable);
                assert!(message.contains("S3 failed"));
                assert!(message.contains("bad.txt"));
                assert_eq!(error_code.as_deref(), Some("SlowDown"));
                assert_eq!(failure_count, 7);
            }
            _ => panic!("expected producer failure to be preserved"),
        }
    }

    #[tokio::test]
    async fn unfinished_producer_after_send_error_is_not_treated_as_complete() {
        let mut producer: AbortOnDropJoinHandle<Result<ExtractDigest>> =
            AbortOnDropJoinHandle::new(tokio::spawn(async { std::future::pending().await }));

        assert!(
            producer_result_after_send_error(&mut producer)
                .await
                .is_none()
        );

        producer.abort();
        let _ = producer.join().await;
    }

    #[tokio::test]
    async fn zero_length_crc_mismatch_fails_before_put_object() {
        let path = "empty.txt";
        let source_span_end = (30 + path.len()) as u64;
        let entry = ManifestEntry {
            source_offset: 0,
            source_span_start: 0,
            source_span_end,
            zip_path: path.to_string(),
            key: format!("prefix/{path}"),
            size: 0,
            compressed_size: 0,
            compression: Compression::Stored,
            crc32: 1,
            catalog_md5: None,
            is_directory: false,
        };
        let store = zero_length_entry_store(&entry);
        let options = SyncOptions::new(
            S3Object::parse("s3://source-bucket/source.zip").unwrap(),
            S3Prefix::parse("s3://destination-bucket/prefix/").unwrap(),
        );

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            put_entry_stream_once(
                &dummy_s3_client(),
                store,
                &entry,
                &PutCondition::IfNoneMatch,
                &options,
                None,
            ),
        )
        .await
        .expect("CRC failure should happen before any S3 PutObject attempt");

        match result {
            PutAttemptResult::Failed {
                message,
                retryable,
                error_code,
                failure_count,
            } => {
                assert!(!retryable);
                assert!(message.contains("CRC"));
                assert_eq!(error_code, None);
                assert_eq!(failure_count, 0);
            }
            _ => panic!("expected zero-byte CRC mismatch to fail before PutObject"),
        }
    }

    #[tokio::test]
    async fn local_entry_crc_mismatch_leaves_pipe_short() {
        let data = (0..(64 * 1024 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut crc32 = Crc32Hasher::new();
        crc32.update(&data);
        let bad_crc32 = crc32.finalize() ^ u32::MAX;
        let entry = LocalZipEntry {
            index: 0,
            zip_path: "bad.txt".to_string(),
            destination: "prefix/bad.txt".to_string(),
            size: data.len() as u64,
            compressed_size: data.len() as u64,
            compression: Compression::Stored,
            crc32: bad_crc32,
            catalog_md5: None,
            is_directory: false,
        };
        let (writer, mut reader) = tokio::io::duplex(128 * 1024);
        let producer = tokio::spawn(write_local_entry_to_pipe(
            writer,
            std::io::Cursor::new(data.clone()),
            entry,
        ));
        let mut body = Vec::new();

        let read = tokio::time::timeout(Duration::from_secs(1), reader.read_to_end(&mut body))
            .await
            .expect("pipe should close after CRC failure")
            .unwrap();
        let result = producer.await.unwrap();

        let err = result.unwrap_err();
        assert!(err.to_string().contains("CRC"));
        assert_eq!(read, 64 * 1024);
        assert_eq!(body, data[..body.len()]);
        assert!(body.len() < data.len());
    }

    #[tokio::test]
    async fn local_entry_size_overflow_leaves_pipe_short() {
        let data = vec![7_u8; 64 * 1024 + 1];
        let mut crc32 = Crc32Hasher::new();
        crc32.update(&data);
        let entry = LocalZipEntry {
            index: 0,
            zip_path: "too-large.txt".to_string(),
            destination: "prefix/too-large.txt".to_string(),
            size: 64 * 1024,
            compressed_size: data.len() as u64,
            compression: Compression::Stored,
            crc32: crc32.finalize(),
            catalog_md5: None,
            is_directory: false,
        };
        let (writer, mut reader) = tokio::io::duplex(128 * 1024);
        let producer = tokio::spawn(write_local_entry_to_pipe(
            writer,
            std::io::Cursor::new(data),
            entry,
        ));
        let mut body = Vec::new();

        let read = tokio::time::timeout(Duration::from_secs(1), reader.read_to_end(&mut body))
            .await
            .expect("pipe should close after size overflow")
            .unwrap();
        let result = producer.await.unwrap();

        let err = result.unwrap_err();
        assert!(err.to_string().contains("central directory declared"));
        assert_eq!(read, 0);
        assert!(body.is_empty());
    }

    #[test]
    fn local_destination_collisions_respect_case_insensitive_filesystems() {
        validate_local_destination_path_collisions([("Readme", false), ("README", false)], false)
            .unwrap();

        let err = validate_local_destination_path_collisions(
            [("Readme", false), ("README", false)],
            true,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidZipEntry { reason, .. }
                if reason.contains("same local destination path")
        ));
    }

    #[test]
    fn local_destination_parent_collisions_respect_case_insensitive_filesystems() {
        validate_local_destination_path_collisions(
            [("Readme", false), ("README/child.txt", false)],
            false,
        )
        .unwrap();

        let err = validate_local_destination_path_collisions(
            [("Readme", false), ("README/child.txt", false)],
            true,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidZipEntry { reason, .. }
                if reason.contains("same local destination path as file ZIP entry")
        ));
    }

    #[tokio::test]
    async fn local_zero_length_crc_mismatch_fails_before_put_object() {
        let entry = LocalZipEntry {
            index: 0,
            zip_path: "empty.txt".to_string(),
            destination: "prefix/empty.txt".to_string(),
            size: 0,
            compressed_size: 0,
            compression: Compression::Stored,
            crc32: 1,
            catalog_md5: None,
            is_directory: false,
        };
        let options = LocalZipSyncOptions::new(
            "source.zip",
            S3Prefix::parse("s3://destination-bucket/prefix/").unwrap(),
        );

        let report = tokio::time::timeout(
            Duration::from_secs(1),
            put_local_file_entry_to_s3(
                &dummy_s3_client(),
                std::io::Cursor::new(Vec::<u8>::new()),
                entry,
                PutCondition::IfNoneMatch,
                None,
                &options,
            ),
        )
        .await
        .expect("CRC failure should happen before any S3 PutObject attempt");

        assert_eq!(report.status, OperationStatus::Error);
        assert!(
            report
                .message
                .as_deref()
                .is_some_and(|message| message.contains("CRC"))
        );
    }

    #[test]
    fn local_zip_sync_options_reject_delete_extra_at_bucket_root() {
        let mut options = LocalZipSyncOptions::new(
            "source.zip",
            S3Prefix::parse("s3://destination-bucket").unwrap(),
        );
        options.delete_extra = true;

        let err = validate_local_zip_sync_options(&options).unwrap_err();

        assert!(err.to_string().contains("delete_extra"));
    }

    #[tokio::test]
    async fn local_zip_entries_reject_directory_crc_mismatch() {
        let zip_bytes = test_zip_bytes(&[("nested/", Compression::Stored, b"".as_slice())]).await;
        let zip_bytes = set_central_crc32(zip_bytes, "nested/", 1);
        let source_len = zip_bytes.len() as u64;
        let reader = async_zip::base::read::mem::ZipFileReader::new(zip_bytes)
            .await
            .unwrap();
        let destination = S3Prefix::parse("s3://destination-bucket/prefix/").unwrap();

        let err =
            match local_zip_entries_for_s3(reader.file().entries(), &destination, source_len, true)
            {
                Ok(_) => panic!("directory entry with nonzero CRC should be rejected"),
                Err(err) => err,
            };

        assert!(matches!(
            err,
            Error::InvalidZipEntry { ref path, ref reason }
                if path == "nested/" && reason.contains("zero CRC32")
        ));
    }

    #[tokio::test]
    async fn local_embedded_catalog_falls_back_on_crc_mismatch() {
        let catalog_json =
            br#"{"version":1,"entries":[{"path":"a.txt","md5":"2c1743a391305fbf367df8e4f069f9f9"}]}"#;
        let zip_bytes = test_zip_bytes(&[
            ("a.txt", Compression::Stored, b"alpha".as_slice()),
            (
                EMBEDDED_CATALOG_PATH,
                Compression::Stored,
                catalog_json.as_slice(),
            ),
        ])
        .await;
        let zip_bytes = set_central_crc32(zip_bytes, EMBEDDED_CATALOG_PATH, 0);
        let path = unique_test_file("local-catalog-crc");
        tokio::fs::write(&path, zip_bytes).await.unwrap();
        let reader = open_local_zip_reader(&path).await.unwrap();
        let catalog_index = reader
            .file()
            .entries()
            .iter()
            .position(|entry| entry.filename().as_str().unwrap() == EMBEDDED_CATALOG_PATH);

        let catalog = load_local_embedded_catalog(&reader, catalog_index).await;

        assert!(catalog.is_empty());
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[test]
    fn put_retry_delay_uses_capped_exponential_backoff() {
        let policy = PutRetryPolicy {
            jitter: RetryJitter::None,
            ..PutRetryPolicy::default()
        };

        assert_eq!(
            put_retry_delay(&policy, 1, false),
            Duration::from_millis(250)
        );
        assert_eq!(
            put_retry_delay(&policy, 2, false),
            Duration::from_millis(500)
        );
        assert_eq!(
            put_retry_delay(&policy, 3, false),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            put_retry_delay(&policy, 6, false),
            Duration::from_millis(5_000)
        );
        assert_eq!(
            put_retry_delay(&policy, 60, false),
            Duration::from_millis(5_000)
        );
        assert_eq!(
            put_retry_delay(&policy, 1, true),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            put_retry_delay(&policy, 60, true),
            Duration::from_millis(30_000)
        );
    }

    #[test]
    fn put_diagnostics_counts_failed_attempts_by_error_code() {
        let diagnostics = PutDiagnosticsCollector::default();

        diagnostics.record_failure("dispatch_failure");
        diagnostics.record_failure("dispatch_failure");
        diagnostics.record_failure("SlowDown");

        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.failed_attempts, 3);
        assert_eq!(snapshot.retry_attempts, 0);
        assert_eq!(
            snapshot.failures_by_error_code.get("dispatch_failure"),
            Some(&2)
        );
        assert_eq!(snapshot.failures_by_error_code.get("SlowDown"), Some(&1));
    }

    #[test]
    fn slowdown_errors_are_classified_as_put_throttling() {
        assert!(is_put_throttle_error_code("SlowDown"));
        assert!(is_put_throttle_error_code("ThrottlingException"));
        assert!(!is_put_throttle_error_code("PreconditionFailed"));
        assert!(!is_put_throttle_error_code("service_error"));
    }

    #[tokio::test]
    async fn put_throttle_records_shared_waits() {
        let diagnostics = Arc::new(PutDiagnosticsCollector::default());
        let throttle = PutThrottle::new(Some(Arc::clone(&diagnostics)));

        throttle.throttle(Duration::from_millis(1));
        throttle.wait().await;

        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.throttled_attempts, 1);
        assert!(snapshot.throttle_waits >= 1);
    }

    fn zero_length_entry_store(entry: &ManifestEntry) -> Arc<BlockStore> {
        let mut header = Vec::new();
        header.extend_from_slice(&0x0403_4b50_u32.to_le_bytes());
        header.extend_from_slice(&20_u16.to_le_bytes());
        header.extend_from_slice(&0_u16.to_le_bytes());
        header.extend_from_slice(&0_u16.to_le_bytes());
        header.extend_from_slice(&0_u16.to_le_bytes());
        header.extend_from_slice(&0_u16.to_le_bytes());
        header.extend_from_slice(&0_u32.to_le_bytes());
        header.extend_from_slice(&0_u32.to_le_bytes());
        header.extend_from_slice(&0_u32.to_le_bytes());
        header.extend_from_slice(&(entry.zip_path.len() as u16).to_le_bytes());
        header.extend_from_slice(&0_u16.to_le_bytes());
        header.extend_from_slice(entry.zip_path.as_bytes());

        let plan = SourcePlan {
            planned_entries: 1,
            blocks: vec![SourceRange {
                start: 0,
                end: header.len() as u64 - 1,
            }],
        };
        let store = BlockStore::new(plan, header.len(), None);
        store.retain_entry(entry);
        store.finish_fetch(0, Ok(bytes::Bytes::from(header)));
        store
    }

    async fn test_zip_bytes(entries: &[(&str, Compression, &[u8])]) -> Vec<u8> {
        let mut writer = async_zip::base::write::ZipFileWriter::new(Vec::new());
        for (path, compression, data) in entries {
            let entry = async_zip::ZipEntryBuilder::new((*path).to_string().into(), *compression);
            writer.write_entry_whole(entry, data).await.unwrap();
        }
        writer.close().await.unwrap()
    }

    fn set_central_crc32(mut data: Vec<u8>, path: &str, crc32: u32) -> Vec<u8> {
        let signature = 0x0201_4b50_u32.to_le_bytes();
        let mut offset = 0;
        while let Some(relative) = data[offset..]
            .windows(signature.len())
            .position(|window| window == signature)
        {
            let index = offset + relative;
            let file_name_len = u16::from_le_bytes([data[index + 28], data[index + 29]]) as usize;
            let extra_len = u16::from_le_bytes([data[index + 30], data[index + 31]]) as usize;
            let comment_len = u16::from_le_bytes([data[index + 32], data[index + 33]]) as usize;
            let name_start = index + 46;
            let name_end = name_start + file_name_len;
            if &data[name_start..name_end] == path.as_bytes() {
                data[index + 16..index + 20].copy_from_slice(&crc32.to_le_bytes());
                return data;
            }
            offset = name_end + extra_len + comment_len;
        }
        panic!("central directory entry for {path} not found");
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "s3-unspool-{name}-{}-{nanos}.zip",
            std::process::id()
        ))
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("s3-unspool-{name}-{}-{nanos}", std::process::id()))
    }

    fn dummy_s3_client() -> Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test-access-key",
                "test-secret-key",
                None,
                None,
                "s3-unspool-test",
            ))
            .build();
        Client::from_conf(config)
    }

    fn manifest_entry(path: &str, size: u64, catalog_md5: Option<String>) -> ManifestEntry {
        ManifestEntry {
            source_offset: 0,
            source_span_start: 0,
            source_span_end: size,
            zip_path: path.to_string(),
            key: format!("prefix/{path}"),
            size,
            compressed_size: size,
            compression: Compression::Stored,
            crc32: 0,
            catalog_md5,
            is_directory: false,
        }
    }

    fn local_directory_entry(destination: &str) -> LocalZipEntry {
        LocalZipEntry {
            index: 0,
            zip_path: "empty/".to_string(),
            destination: destination.to_string(),
            size: 0,
            compressed_size: 0,
            compression: Compression::Stored,
            crc32: 0,
            catalog_md5: None,
            is_directory: true,
        }
    }
}
