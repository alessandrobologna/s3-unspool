use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::StalledStreamProtectionConfig;
use lambda_runtime::{Error as LambdaError, LambdaEvent, service_fn};
use s3_unspool::{
    ObjectReport, S3Object, S3Prefix, SyncDiagnostics, SyncOptions, SyncReport, SyncSummary,
    adaptive_source_get_concurrency, sync_zip_to_s3_with_clients,
};
use serde::{Deserialize, Serialize};

const LAMBDA_BASE_MEMORY_MB: f64 = 128.0;
const LAMBDA_BASE_CONCURRENCY: f64 = 4.0;
const MIN_LAMBDA_CONCURRENCY: usize = 4;
const MAX_LAMBDA_CONCURRENCY: usize = 16;

#[derive(Debug, Deserialize)]
struct InvokePayload {
    source: String,
    #[serde(alias = "destinationPrefix", alias = "destination")]
    destination_prefix: String,
    #[serde(default, alias = "deleteExtra")]
    delete_extra: bool,
    #[serde(default, alias = "collectDiagnostics")]
    diagnostics: bool,
    #[serde(default)]
    concurrency: Option<usize>,
    #[serde(default, alias = "ignoreCatalog", alias = "ignoreEmbeddedCatalog")]
    ignore_embedded_catalog: bool,
    #[serde(default, alias = "includeOperations")]
    include_operations: bool,
}

#[derive(Debug, Serialize)]
struct InvokeResponse {
    source: S3Object,
    destination: S3Prefix,
    summary: SyncSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostics: Option<SyncDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operations: Option<Vec<ObjectReport>>,
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .with_ansi(false)
        .with_target(false)
        .init();

    lambda_runtime::run(service_fn(handle)).await
}

async fn handle(event: LambdaEvent<InvokePayload>) -> Result<InvokeResponse, LambdaError> {
    trim_process_allocator();

    let (payload, context) = event.into_parts();
    let include_operations = payload.include_operations;
    let memory_mb = u64::try_from(context.env_config.memory).unwrap_or_default();
    let concurrency = payload
        .concurrency
        .map(clamp_lambda_concurrency)
        .unwrap_or_else(|| adaptive_lambda_concurrency(memory_mb));
    tracing::info!(
        request_id = %context.request_id,
        memory_mb,
        source = %payload.source,
        destination_prefix = %payload.destination_prefix,
        delete_extra = payload.delete_extra,
        diagnostics = payload.diagnostics,
        include_operations,
        ignore_embedded_catalog = payload.ignore_embedded_catalog,
        concurrency,
        "lambda extract invoke started"
    );

    let shared_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let source_client = Client::new(&shared_config);
    let destination_client = Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared_config)
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .upload_enabled(false)
                    .download_enabled(true)
                    .build(),
            )
            .build(),
    );

    let mut options = payload.into_options(concurrency)?;
    options.source_get_concurrency = adaptive_source_get_concurrency(memory_mb);
    options.put_concurrency =
        adaptive_lambda_put_concurrency(options.concurrency, options.source_get_concurrency);
    options.adaptive_source_window_memory_mb = Some(memory_mb);
    tracing::info!(
        request_id = %context.request_id,
        memory_mb,
        concurrency = options.concurrency,
        source_block_size = options.source_block_size,
        source_block_merge_gap = options.source_block_merge_gap,
        source_get_concurrency = options.source_get_concurrency,
        adaptive_source_window_memory_mb = ?options.adaptive_source_window_memory_mb,
        put_concurrency = options.put_concurrency,
        body_chunk_size = options.body_chunk_size,
        pipe_capacity = options.pipe_capacity,
        "lambda extract options prepared"
    );
    let report = sync_zip_to_s3_with_clients(&source_client, &destination_client, options).await;
    trim_process_allocator();

    let report = report.map_err(|err| {
        tracing::error!(
            request_id = %context.request_id,
            error = %err,
            "lambda extract invoke failed"
        );
        err
    })?;
    tracing::info!(
        request_id = %context.request_id,
        zip_files = report.summary.zip_files,
        destination_objects = report.summary.destination_objects,
        uploaded_new = report.summary.uploaded_new,
        uploaded_changed = report.summary.uploaded_changed,
        skipped_unchanged = report.summary.skipped_unchanged,
        conditional_conflicts = report.summary.conditional_conflicts,
        deleted_extra = report.summary.deleted_extra,
        errors = report.summary.errors,
        "lambda extract invoke completed"
    );
    Ok(InvokeResponse::from_sync_report(report, include_operations))
}

fn trim_process_allocator() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // Lambda enforces RSS, and glibc may keep freed ZIP/cache pages mapped
        // across warm invocations unless we explicitly return them.
        unsafe {
            libc::malloc_trim(0);
        }
    }
}

impl InvokeResponse {
    fn from_sync_report(report: SyncReport, include_operations: bool) -> Self {
        Self {
            source: report.source,
            destination: report.destination,
            summary: report.summary,
            diagnostics: report.diagnostics,
            operations: include_operations.then_some(report.operations),
        }
    }
}

impl InvokePayload {
    fn into_options(self, concurrency: usize) -> s3_unspool::Result<SyncOptions> {
        let mut options = SyncOptions::new(
            S3Object::parse(self.source)?,
            S3Prefix::parse(self.destination_prefix)?,
        );
        options.delete_extra = self.delete_extra;
        options.collect_diagnostics = self.diagnostics;
        options.concurrency = concurrency;
        options.ignore_embedded_catalog = self.ignore_embedded_catalog;
        options.collect_operations = self.include_operations;
        Ok(options)
    }
}

fn adaptive_lambda_concurrency(memory_mb: u64) -> usize {
    let memory_ratio = memory_mb as f64 / LAMBDA_BASE_MEMORY_MB;
    let workers = (LAMBDA_BASE_CONCURRENCY * memory_ratio.sqrt()).round() as usize;
    clamp_lambda_concurrency(workers)
}

fn clamp_lambda_concurrency(workers: usize) -> usize {
    workers.clamp(MIN_LAMBDA_CONCURRENCY, MAX_LAMBDA_CONCURRENCY)
}

fn adaptive_lambda_put_concurrency(entry_workers: usize, source_get_concurrency: usize) -> usize {
    entry_workers.min(source_get_concurrency.max(2)).clamp(1, 8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3_unspool::adaptive_source_window_capacity;

    #[test]
    fn parses_minimal_camel_case_payload() {
        let payload: InvokePayload = serde_json::from_str(
            r#"{
                "source": "s3://test-bucket/source/archive.zip",
                "destinationPrefix": "s3://test-bucket/dest/"
            }"#,
        )
        .unwrap();

        assert_eq!(payload.source, "s3://test-bucket/source/archive.zip");
        assert_eq!(payload.destination_prefix, "s3://test-bucket/dest/");
        assert!(!payload.delete_extra);
        assert!(!payload.diagnostics);
        assert_eq!(payload.concurrency, None);
        assert!(!payload.ignore_embedded_catalog);
        assert!(!payload.include_operations);
    }

    #[test]
    fn converts_payload_to_sync_options() {
        let payload: InvokePayload = serde_json::from_str(
            r#"{
                "source": "s3://test-bucket/source/archive.zip",
                "destination_prefix": "s3://test-bucket/dest",
                "deleteExtra": true,
                "collectDiagnostics": true,
                "concurrency": 8,
                "ignoreCatalog": true,
                "includeOperations": true
            }"#,
        )
        .unwrap();

        assert!(payload.include_operations);
        let concurrency = payload
            .concurrency
            .unwrap_or_else(|| adaptive_lambda_concurrency(256));
        let options = payload.into_options(concurrency).unwrap();

        assert_eq!(options.source.bucket, "test-bucket");
        assert_eq!(options.source.key, "source/archive.zip");
        assert_eq!(options.destination.bucket, "test-bucket");
        assert_eq!(options.destination.prefix, "dest");
        assert!(options.delete_extra);
        assert!(options.collect_diagnostics);
        assert_eq!(options.concurrency, 8);
        assert!(options.ignore_embedded_catalog);
        assert!(!options.fail_on_conditional_conflict);
        assert!(options.collect_operations);
        assert_eq!(options.adaptive_source_window_memory_mb, None);
    }

    #[test]
    fn parses_ignore_embedded_catalog_alias() {
        let payload: InvokePayload = serde_json::from_str(
            r#"{
                "source": "s3://test-bucket/source/archive.zip",
                "destinationPrefix": "s3://test-bucket/dest/",
                "ignoreEmbeddedCatalog": true
            }"#,
        )
        .unwrap();

        assert!(payload.ignore_embedded_catalog);
    }

    #[test]
    fn lambda_response_omits_operations_by_default() {
        let report = SyncReport {
            source: S3Object::parse("s3://bucket/source.zip").unwrap(),
            destination: S3Prefix::parse("s3://bucket/dest/").unwrap(),
            summary: SyncSummary {
                zip_files: 1,
                ..SyncSummary::default()
            },
            diagnostics: None,
            operations: vec![ObjectReport {
                status: s3_unspool::OperationStatus::SkippedUnchanged,
                key: "dest/file.txt".to_string(),
                zip_path: Some("file.txt".to_string()),
                size: Some(1),
                md5: None,
                destination_etag: None,
                message: None,
            }],
        };

        let response = InvokeResponse::from_sync_report(report, false);
        let json = serde_json::to_value(response).unwrap();

        assert!(json.get("operations").is_none());
        assert_eq!(json["summary"]["zip_files"], 1);
    }

    #[test]
    fn lambda_concurrency_defaults_follow_assigned_memory() {
        assert_eq!(adaptive_lambda_concurrency(0), 4);
        assert_eq!(adaptive_lambda_concurrency(128), 4);
        assert_eq!(adaptive_lambda_concurrency(256), 6);
        assert_eq!(adaptive_lambda_concurrency(512), 8);
        assert_eq!(adaptive_lambda_concurrency(1024), 11);
        assert_eq!(adaptive_lambda_concurrency(2048), 16);
        assert_eq!(adaptive_lambda_concurrency(3008), 16);
    }

    #[test]
    fn explicit_lambda_concurrency_is_clamped() {
        assert_eq!(clamp_lambda_concurrency(0), MIN_LAMBDA_CONCURRENCY);
        assert_eq!(clamp_lambda_concurrency(8), 8);
        assert_eq!(clamp_lambda_concurrency(64), MAX_LAMBDA_CONCURRENCY);
    }

    #[test]
    fn lambda_put_concurrency_follows_source_and_entry_limits() {
        assert_eq!(adaptive_lambda_put_concurrency(4, 1), 2);
        assert_eq!(adaptive_lambda_put_concurrency(6, 1), 2);
        assert_eq!(adaptive_lambda_put_concurrency(8, 4), 4);
        assert_eq!(adaptive_lambda_put_concurrency(16, 8), 8);
        assert_eq!(adaptive_lambda_put_concurrency(4, 8), 4);
    }

    #[test]
    fn adaptive_cache_accounts_for_workers_and_file_count() {
        let low_memory_concurrency = adaptive_lambda_concurrency(256);
        let small_source_get_concurrency = adaptive_source_get_concurrency(256);
        assert_eq!(small_source_get_concurrency, 1);
        assert_eq!(
            adaptive_source_window_capacity(
                256,
                423_545,
                low_memory_concurrency,
                10,
                8 * 1024 * 1024,
                small_source_get_concurrency,
            ),
            423_545
        );
        assert_eq!(
            adaptive_source_window_capacity(
                256,
                3 * 1024 * 1024 * 1024,
                low_memory_concurrency,
                49_152,
                8 * 1024 * 1024,
                small_source_get_concurrency
            ),
            16 * 1024 * 1024
        );
        assert_eq!(
            adaptive_source_window_capacity(
                256,
                3 * 1024 * 1024 * 1024,
                low_memory_concurrency,
                65_536,
                8 * 1024 * 1024,
                small_source_get_concurrency
            ),
            0
        );

        let large_memory_concurrency = adaptive_lambda_concurrency(2048);
        let large_source_get_concurrency = adaptive_source_get_concurrency(2048);
        assert_eq!(large_source_get_concurrency, 8);
        assert_eq!(
            adaptive_source_window_capacity(
                1024,
                3 * 1024 * 1024 * 1024,
                adaptive_lambda_concurrency(1024),
                49_152,
                8 * 1024 * 1024,
                adaptive_source_get_concurrency(1024)
            ),
            316 * 1024 * 1024
        );
        assert_eq!(
            adaptive_source_window_capacity(
                2048,
                3 * 1024 * 1024 * 1024,
                large_memory_concurrency,
                49_152,
                8 * 1024 * 1024,
                large_source_get_concurrency
            ),
            512 * 1024 * 1024
        );
    }
}
