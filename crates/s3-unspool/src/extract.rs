use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::io::ReaderStream;

use crate::constants::{MAX_BODY_CHUNK_SIZE, MAX_PIPE_CAPACITY};
use crate::entry_reader::{EntryReader, entry_reader};
use crate::error::{Error, Result, aws_error_context, aws_error_message};
use crate::options::{PutRetryPolicy, RetryJitter, SyncOptions, adaptive_source_window_capacity};
use crate::range::{
    BlockStore, SourceClient, SourceDiagnosticsCollector, plan_source_blocks,
    start_source_scheduler,
};
use crate::report::{
    ObjectReport, OperationStatus, PutDiagnostics, PutRetryDiagnostics, SyncDiagnostics,
    SyncReport, SyncSummary, summarize_operation,
};
use crate::s3_uri::{S3Prefix, normalize_etag};
use crate::source::head_source;
use crate::zip_manifest::{ManifestEntry, load_zip_manifest, validate_crc32_value};

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

    use crate::S3Object;
    use crate::range::{SourcePlan, SourceRange};

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
}
