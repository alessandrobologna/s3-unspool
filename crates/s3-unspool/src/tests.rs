use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_zip::Compression;
use aws_sdk_s3::config::{Credentials, Region};
use futures_lite::io::AsyncReadExt as FuturesAsyncReadExt;
use tokio::io::AsyncReadExt as TokioAsyncReadExt;

use crate::catalog::{EmbeddedCatalog, EmbeddedCatalogEntry, catalog_md5_by_path};
use crate::constants::{
    EMBEDDED_CATALOG_MAX_BYTES, EMBEDDED_CATALOG_PATH, EMBEDDED_CATALOG_VERSION,
    MAX_BODY_CHUNK_SIZE, MAX_PIPE_CAPACITY,
};
use crate::entry_reader::entry_reader;
use crate::extract::{
    DestinationObject, catalog_skip_report, comparable_destination_md5, conditional_conflict_error,
    normalized_list_prefix, resolve_source_window_capacity, validate_extracted_size,
    validate_options, validate_source_range_options,
};
use crate::range::{
    BlockRangeReader, BlockStore, SourceClient, SourceDiagnosticsCollector, SourcePlan,
    SourceRange, plan_source_blocks,
};
use crate::report::summarize;
use crate::s3_uri::{join_destination_key, normalize_etag};
use crate::upload::{UploadEntry, collect_upload_entries, upload_zip_path, write_upload_zip};
use crate::zip_manifest::{
    ManifestBuild, ManifestEntry, apply_embedded_catalog, build_manifest_entries,
    count_zip_file_entries, normalize_zip_file_path,
};
use crate::{
    Error, ObjectReport, OperationStatus, PutRetryPolicy, S3Object, S3Prefix, SyncOptions,
    SyncReport, SyncSummary, UploadProgress, UploadProgressHandler,
};

#[test]
fn parses_s3_object_uri() {
    let uri = S3Object::parse("s3://bucket/path/archive.zip").unwrap();
    assert_eq!(uri.bucket, "bucket");
    assert_eq!(uri.key, "path/archive.zip");
}

#[test]
fn parses_s3_object_uri_without_leading_slashes() {
    let uri = S3Object::parse("s3://bucket//path/archive.zip").unwrap();
    assert_eq!(uri.bucket, "bucket");
    assert_eq!(uri.key, "path/archive.zip");
    assert_eq!(uri.uri(), "s3://bucket/path/archive.zip");
}

#[test]
fn rejects_source_without_key() {
    let err = S3Object::parse("s3://bucket").unwrap_err();
    assert!(err.to_string().contains("missing object key"));
}

#[test]
fn rejects_s3_object_uri_with_only_repeated_slashes() {
    let err = S3Object::parse("s3://bucket//").unwrap_err();
    assert!(err.to_string().contains("missing object key"));
}

#[test]
fn parses_empty_destination_prefix() {
    let prefix = S3Prefix::parse("s3://bucket").unwrap();
    assert_eq!(prefix.bucket, "bucket");
    assert_eq!(prefix.prefix, "");
    assert_eq!(prefix.join_key("a/b.txt"), "a/b.txt");
}

#[test]
fn parses_destination_prefix_without_leading_slashes() {
    let prefix = S3Prefix::parse("s3://bucket//destination/").unwrap();
    assert_eq!(prefix.bucket, "bucket");
    assert_eq!(prefix.prefix, "destination/");
    assert_eq!(prefix.join_key("a/b.txt"), "destination/a/b.txt");
    assert_eq!(prefix.uri(), "s3://bucket/destination/");
}

#[test]
fn sync_options_use_embedded_catalog_by_default() {
    let options = SyncOptions::new(
        S3Object::parse("s3://bucket/source.zip").unwrap(),
        S3Prefix::parse("s3://bucket/destination/").unwrap(),
    );

    assert!(!options.ignore_embedded_catalog);
    assert!(!options.fail_on_conditional_conflict);
    assert!(options.collect_operations);
}

#[test]
fn sync_options_reject_delete_extra_at_bucket_root() {
    let mut options = SyncOptions::new(
        S3Object::parse("s3://bucket/source.zip").unwrap(),
        S3Prefix::parse("s3://bucket").unwrap(),
    );
    options.delete_extra = true;

    let err = validate_options(&options).unwrap_err();

    assert!(err.to_string().contains("delete_extra"));
}

#[test]
fn sync_options_reject_oversized_stream_buffers() {
    let mut options = SyncOptions::new(
        S3Object::parse("s3://bucket/source.zip").unwrap(),
        S3Prefix::parse("s3://bucket/destination/").unwrap(),
    );
    options.body_chunk_size = MAX_BODY_CHUNK_SIZE + 1;
    assert!(validate_options(&options).is_err());

    options.body_chunk_size = MAX_BODY_CHUNK_SIZE;
    options.pipe_capacity = MAX_PIPE_CAPACITY + 1;
    assert!(validate_options(&options).is_err());
}

#[test]
fn sync_options_reject_invalid_put_retry_settings() {
    let mut options = SyncOptions::new(
        S3Object::parse("s3://bucket/source.zip").unwrap(),
        S3Prefix::parse("s3://bucket/destination/").unwrap(),
    );

    options.put_concurrency = 0;
    assert!(
        validate_options(&options)
            .unwrap_err()
            .to_string()
            .contains("put_concurrency")
    );

    options.put_concurrency = 1;
    options.put_retry_policy.max_attempts = 0;
    assert!(
        validate_options(&options)
            .unwrap_err()
            .to_string()
            .contains("max_attempts")
    );

    options.put_retry_policy = PutRetryPolicy {
        base_delay: Duration::from_secs(2),
        max_delay: Duration::from_secs(1),
        ..PutRetryPolicy::default()
    };
    assert!(
        validate_options(&options)
            .unwrap_err()
            .to_string()
            .contains("max_delay")
    );

    options.put_retry_policy = PutRetryPolicy {
        slowdown_base_delay: Duration::from_secs(2),
        slowdown_max_delay: Duration::from_secs(1),
        ..PutRetryPolicy::default()
    };
    assert!(
        validate_options(&options)
            .unwrap_err()
            .to_string()
            .contains("slowdown_max_delay")
    );
}

#[test]
fn sync_options_reject_source_blocks_larger_than_window() {
    let mut options = SyncOptions::new(
        S3Object::parse("s3://bucket/source.zip").unwrap(),
        S3Prefix::parse("s3://bucket/destination/").unwrap(),
    );
    options.source_window_capacity = 8;
    options.source_block_size = 16;

    let err = validate_source_range_options(&options, 64).unwrap_err();

    assert!(err.to_string().contains("source_block_size"));
}

#[test]
fn sync_options_allow_large_source_block_for_small_source_zip() {
    let mut options = SyncOptions::new(
        S3Object::parse("s3://bucket/source.zip").unwrap(),
        S3Prefix::parse("s3://bucket/destination/").unwrap(),
    );
    options.source_window_capacity = 8;
    options.source_block_size = 16;

    validate_source_range_options(&options, 8).unwrap();
}

#[test]
fn adaptive_source_window_resolves_after_manifest_count_is_known() {
    let mut options = SyncOptions::new(
        S3Object::parse("s3://bucket/source.zip").unwrap(),
        S3Prefix::parse("s3://bucket/destination/").unwrap(),
    );
    options.source_window_capacity = 123;
    options.adaptive_source_window_memory_mb = Some(256);
    options.concurrency = 6;
    options.source_get_concurrency = 1;
    options.source_block_size = 8 * 1024 * 1024;

    resolve_source_window_capacity(&mut options, 3 * 1024 * 1024 * 1024, 49_152);

    assert_eq!(options.source_window_capacity, 16 * 1024 * 1024);
}

#[test]
fn joins_destination_prefixes() {
    assert_eq!(join_destination_key("", "a.txt"), "a.txt");
    assert_eq!(join_destination_key("prefix", "a.txt"), "prefix/a.txt");
    assert_eq!(join_destination_key("prefix/", "a.txt"), "prefix/a.txt");
}

#[test]
fn normalizes_destination_list_prefixes() {
    assert_eq!(normalized_list_prefix(""), "");
    assert_eq!(normalized_list_prefix("prefix"), "prefix/");
    assert_eq!(normalized_list_prefix("prefix/"), "prefix/");
}

#[test]
fn normalizes_safe_zip_file_paths() {
    assert_eq!(
        normalize_zip_file_path("a/b.txt").unwrap(),
        Some("a/b.txt".to_string())
    );
    assert_eq!(normalize_zip_file_path("dir/").unwrap(), None);
}

#[test]
fn rejects_unsafe_zip_file_paths() {
    for path in [
        "/abs.txt",
        "../escape.txt",
        "a/../escape.txt",
        "a//b.txt",
        "C:/x.txt",
        "a\\b.txt",
    ] {
        assert!(normalize_zip_file_path(path).is_err(), "{path}");
    }
}

#[tokio::test]
async fn collects_and_writes_upload_zip_entries() {
    let root = unique_temp_dir("zip-upload");
    tokio::fs::create_dir_all(root.join("nested"))
        .await
        .unwrap();
    tokio::fs::write(root.join("b.txt"), b"bravo")
        .await
        .unwrap();
    tokio::fs::write(root.join("nested").join("a.txt"), b"alpha")
        .await
        .unwrap();

    let entries = collect_upload_entries(&root).await.unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.zip_path.as_str())
            .collect::<Vec<_>>(),
        vec!["b.txt", "nested/a.txt"]
    );

    let mut data = Vec::new();
    let catalog_entries = write_upload_zip(&mut data, &entries, true, None)
        .await
        .unwrap();
    assert_eq!(
        catalog_entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry.md5.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("b.txt", "fd9ab41e47a9ef4f6477a8a000bf404f"),
            ("nested/a.txt", "2c1743a391305fbf367df8e4f069f9f9")
        ]
    );

    let zip = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();
    let zip_entries = zip.file().entries().to_vec();
    assert_eq!(zip_entries.len(), 3);
    assert_eq!(zip_entries[0].filename().as_str().unwrap(), "b.txt");
    assert_eq!(zip_entries[1].filename().as_str().unwrap(), "nested/a.txt");
    assert_eq!(
        zip_entries[2].filename().as_str().unwrap(),
        EMBEDDED_CATALOG_PATH
    );

    let mut nested = zip.reader_with_entry(1).await.unwrap();
    let mut nested_bytes = Vec::new();
    FuturesAsyncReadExt::read_to_end(&mut nested, &mut nested_bytes)
        .await
        .unwrap();
    assert_eq!(nested_bytes, b"alpha");

    let mut catalog_reader = zip.reader_with_entry(2).await.unwrap();
    let mut catalog_bytes = Vec::new();
    FuturesAsyncReadExt::read_to_end(&mut catalog_reader, &mut catalog_bytes)
        .await
        .unwrap();
    let catalog = serde_json::from_slice::<EmbeddedCatalog>(&catalog_bytes).unwrap();
    assert_eq!(catalog.version, EMBEDDED_CATALOG_VERSION);
    assert_eq!(catalog.entries, catalog_entries);

    tokio::fs::remove_dir_all(root).await.unwrap();
}

#[tokio::test]
async fn upload_zip_rejects_uncompressed_size_overflow() {
    let entries = vec![
        UploadEntry {
            path: PathBuf::from("missing-a.txt"),
            zip_path: "a.txt".to_string(),
            size: u64::MAX,
        },
        UploadEntry {
            path: PathBuf::from("missing-b.txt"),
            zip_path: "b.txt".to_string(),
            size: 1,
        },
    ];
    let mut data = Vec::new();

    let err = write_upload_zip(&mut data, &entries, false, None)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("exceeds u64::MAX"));
}

#[tokio::test]
async fn upload_zip_emits_file_progress_events() {
    let root = unique_temp_dir("zip-upload-progress");
    tokio::fs::create_dir_all(&root).await.unwrap();
    tokio::fs::write(root.join("a.txt"), b"alpha")
        .await
        .unwrap();
    tokio::fs::write(root.join("b.txt"), b"bravo")
        .await
        .unwrap();

    let entries = collect_upload_entries(&root).await.unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let event_sink = Arc::clone(&events);
    let progress = UploadProgressHandler::new(move |event| {
        event_sink.lock().unwrap().push(event);
    });

    let mut data = Vec::new();
    write_upload_zip(&mut data, &entries, true, Some(progress))
        .await
        .unwrap();

    let events = events.lock().unwrap().clone();
    assert_eq!(
        events,
        vec![
            UploadProgress::FileStarted {
                current_file: 1,
                total_files: 2,
                processed_files: 0,
                processed_bytes: 0,
                total_bytes: 10,
                path: "a.txt".to_string(),
            },
            UploadProgress::FileFinished {
                processed_files: 1,
                total_files: 2,
                processed_bytes: 5,
                total_bytes: 10,
                path: "a.txt".to_string(),
            },
            UploadProgress::FileStarted {
                current_file: 2,
                total_files: 2,
                processed_files: 1,
                processed_bytes: 5,
                total_bytes: 10,
                path: "b.txt".to_string(),
            },
            UploadProgress::FileFinished {
                processed_files: 2,
                total_files: 2,
                processed_bytes: 10,
                total_bytes: 10,
                path: "b.txt".to_string(),
            },
        ]
    );

    tokio::fs::remove_dir_all(root).await.unwrap();
}

#[tokio::test]
async fn upload_zip_progress_uses_measured_file_bytes() {
    let root = unique_temp_dir("zip-upload-progress-measured");
    tokio::fs::create_dir_all(&root).await.unwrap();
    tokio::fs::write(root.join("a.txt"), b"alpha")
        .await
        .unwrap();

    let mut entries = collect_upload_entries(&root).await.unwrap();
    entries[0].size = 1;
    let events = Arc::new(Mutex::new(Vec::new()));
    let event_sink = Arc::clone(&events);
    let progress = UploadProgressHandler::new(move |event| {
        event_sink.lock().unwrap().push(event);
    });

    let mut data = Vec::new();
    write_upload_zip(&mut data, &entries, false, Some(progress))
        .await
        .unwrap();

    let events = events.lock().unwrap().clone();
    assert!(events.iter().any(|event| matches!(
        event,
        UploadProgress::FileFinished {
            processed_bytes: 5,
            ..
        }
    )));

    tokio::fs::remove_dir_all(root).await.unwrap();
}

#[tokio::test]
async fn upload_rejects_reserved_catalog_path() {
    let root = unique_temp_dir("zip-upload-reserved");
    tokio::fs::create_dir_all(root.join(".s3-unspool"))
        .await
        .unwrap();
    tokio::fs::write(root.join(".s3-unspool").join("catalog.v1.json"), b"{}")
        .await
        .unwrap();

    let err = collect_upload_entries(&root).await.unwrap_err();

    assert!(err.to_string().contains("reserved"));
    tokio::fs::remove_dir_all(root).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn upload_rejects_symlinked_root_directory() {
    let target = unique_temp_dir("zip-upload-symlink-target");
    let link = unique_temp_dir("zip-upload-symlink-link");
    tokio::fs::create_dir_all(&target).await.unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let err = collect_upload_entries(&link).await.unwrap_err();

    assert!(err.to_string().contains("symbolic links"));
    tokio::fs::remove_file(link).await.unwrap();
    tokio::fs::remove_dir_all(target).await.unwrap();
}

#[test]
fn upload_zip_path_rejects_backslashes() {
    let err = upload_zip_path(Path::new("/base"), Path::new("/base/a\\b.txt")).unwrap_err();
    assert!(err.to_string().contains("backslashes"));
}

#[test]
fn normalizes_s3_etags() {
    assert_eq!(
        normalize_etag("\"D41D8CD98F00B204E9800998ECF8427E\""),
        Some("d41d8cd98f00b204e9800998ecf8427e".to_string())
    );
    assert_eq!(normalize_etag("\"abc-2\""), None);
    assert_eq!(normalize_etag("not-md5"), None);
}

#[test]
fn comparable_destination_md5_uses_listed_size_as_fast_filter() {
    let entry = manifest_entry("file.txt", 10);
    let destination = DestinationObject {
        etag: Some("\"d41d8cd98f00b204e9800998ecf8427e\"".to_string()),
        size: Some(11),
    };

    assert_eq!(
        comparable_destination_md5(&destination, destination.etag.as_deref().unwrap(), &entry),
        None
    );
}

#[test]
fn comparable_destination_md5_accepts_same_size_single_part_etag() {
    let entry = manifest_entry("file.txt", 10);
    let destination = DestinationObject {
        etag: Some("\"D41D8CD98F00B204E9800998ECF8427E\"".to_string()),
        size: Some(10),
    };

    assert_eq!(
        comparable_destination_md5(&destination, destination.etag.as_deref().unwrap(), &entry),
        Some("d41d8cd98f00b204e9800998ecf8427e".to_string())
    );
}

#[test]
fn comparable_destination_md5_rejects_non_md5_etag() {
    let entry = manifest_entry("file.txt", 10);
    let destination = DestinationObject {
        etag: Some("\"abc-2\"".to_string()),
        size: Some(10),
    };

    assert_eq!(
        comparable_destination_md5(&destination, destination.etag.as_deref().unwrap(), &entry),
        None
    );
}

#[tokio::test]
async fn source_client_rejects_invalid_ranges_before_fetch() {
    let source = SourceClient {
        client: dummy_s3_client(),
        bucket: "bucket".to_string(),
        key: "source.zip".to_string(),
        len: 10,
        etag: None,
        diagnostics: None,
    };

    let err = source.get_range(9, 0).await.unwrap_err();

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("start 9"));
}

#[test]
fn extracted_size_must_match_manifest_entry_size() {
    let entry = manifest_entry("a.txt", 5);

    validate_extracted_size(&entry, 5).unwrap();
    let short = validate_extracted_size(&entry, 4).unwrap_err();
    let long = validate_extracted_size(&entry, 6).unwrap_err();

    assert!(short.to_string().contains("produced 4 bytes"));
    assert!(long.to_string().contains("produced 6 bytes"));
}

#[tokio::test]
async fn block_store_waiter_observes_fetch_completion() {
    let store = BlockStore::new(
        SourcePlan {
            planned_entries: 1,
            blocks: vec![SourceRange { start: 0, end: 7 }],
        },
        8,
        None,
    );
    let entry = manifest_entry_with_span("a.txt", 0, 8);
    store.retain_entry(&entry);
    assert_eq!(
        store.reserve_fetch(0).await.unwrap(),
        SourceRange { start: 0, end: 7 }
    );

    let waiting = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.slice_from(2, 5).await.unwrap().bytes })
    };
    tokio::task::yield_now().await;
    store.finish_fetch(0, Ok(bytes::Bytes::from_static(b"abcdefgh")));

    let bytes = waiting.await.unwrap();
    assert_eq!(bytes, bytes::Bytes::from_static(b"cde"));
}

#[tokio::test]
async fn block_store_releases_ready_blocks_after_final_claim() {
    let entry = manifest_entry_with_span("a.txt", 0, 8);
    let store = BlockStore::new(
        SourcePlan {
            planned_entries: 1,
            blocks: vec![SourceRange { start: 0, end: 7 }],
        },
        8,
        None,
    );
    store.retain_entry(&entry);
    assert_eq!(
        store.reserve_fetch(0).await.unwrap(),
        SourceRange { start: 0, end: 7 }
    );
    store.finish_fetch(0, Ok(bytes::Bytes::from_static(b"abcdefgh")));

    let mut reader = BlockRangeReader::new(Arc::clone(&store), 0, 8).unwrap();
    let mut output = Vec::new();
    TokioAsyncReadExt::read_to_end(&mut reader, &mut output)
        .await
        .unwrap();
    drop(reader);

    assert_eq!(output, b"abcdefgh");
    assert!(!store.is_block_ready(0));
    assert_eq!(store.resident_bytes(), 0);
}

#[tokio::test]
async fn block_store_memory_pressure_waits_for_claim_release() {
    let diagnostics = Arc::new(SourceDiagnosticsCollector::new(16));
    let store = BlockStore::new(
        SourcePlan {
            planned_entries: 2,
            blocks: vec![
                SourceRange { start: 0, end: 7 },
                SourceRange { start: 8, end: 15 },
            ],
        },
        8,
        Some(Arc::clone(&diagnostics)),
    );
    let first_entry = manifest_entry_with_span("a.txt", 0, 8);
    let second_entry = manifest_entry_with_span("b.txt", 8, 16);
    store.retain_entry(&first_entry);
    store.retain_entry(&second_entry);
    assert_eq!(
        store.reserve_fetch(0).await.unwrap(),
        SourceRange { start: 0, end: 7 }
    );
    store.finish_fetch(0, Ok(bytes::Bytes::from_static(b"abcdefgh")));
    assert!(store.is_block_ready(0));

    let waiting = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.reserve_fetch(1).await.unwrap() })
    };
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());

    let mut reader = BlockRangeReader::new(Arc::clone(&store), 0, 8).unwrap();
    let mut output = Vec::new();
    TokioAsyncReadExt::read_to_end(&mut reader, &mut output)
        .await
        .unwrap();
    drop(reader);

    assert_eq!(waiting.await.unwrap(), SourceRange { start: 8, end: 15 });
    assert!(!store.is_block_ready(0));
    assert_eq!(store.resident_bytes(), 8);
    assert_eq!(diagnostics.snapshot().block_releases, 1);
}

#[tokio::test]
async fn released_block_is_not_refetched_without_replay_claim() {
    let diagnostics = Arc::new(SourceDiagnosticsCollector::new(16));
    let store = BlockStore::new(
        SourcePlan {
            planned_entries: 1,
            blocks: vec![SourceRange { start: 0, end: 7 }],
        },
        8,
        Some(Arc::clone(&diagnostics)),
    );
    let entry = manifest_entry_with_span("a.txt", 0, 8);
    store.retain_entry(&entry);
    let first = SourceRange { start: 0, end: 7 };
    assert_eq!(store.reserve_fetch(0).await.unwrap(), first);
    store.finish_fetch(0, Ok(bytes::Bytes::from_static(b"abcdefgh")));

    let mut reader = BlockRangeReader::new(Arc::clone(&store), 0, 8).unwrap();
    let mut output = Vec::new();
    TokioAsyncReadExt::read_to_end(&mut reader, &mut output)
        .await
        .unwrap();
    drop(reader);

    assert!(store.reserve_fetch(0).await.is_none());

    let snapshot = diagnostics.snapshot();
    assert_eq!(snapshot.block_releases, 1);
    assert_eq!(snapshot.block_refetches, 0);
}

#[tokio::test]
async fn explicit_replay_claim_allows_released_block_refetch() {
    let diagnostics = Arc::new(SourceDiagnosticsCollector::new(8));
    let store = BlockStore::new(
        SourcePlan {
            planned_entries: 1,
            blocks: vec![SourceRange { start: 0, end: 7 }],
        },
        8,
        Some(Arc::clone(&diagnostics)),
    );
    let entry = manifest_entry_with_span("a.txt", 0, 8);
    let first = SourceRange { start: 0, end: 7 };
    store.retain_entry(&entry);
    assert_eq!(store.reserve_fetch(0).await.unwrap(), first);
    store.finish_fetch(0, Ok(bytes::Bytes::from_static(b"abcdefgh")));

    let mut reader = BlockRangeReader::new(Arc::clone(&store), 0, 8).unwrap();
    let mut output = Vec::new();
    TokioAsyncReadExt::read_to_end(&mut reader, &mut output)
        .await
        .unwrap();
    drop(reader);

    store.add_replay_claim_for_test(&entry);
    assert_eq!(store.reserve_fetch(0).await.unwrap(), first);

    let snapshot = diagnostics.snapshot();
    assert_eq!(snapshot.block_releases, 1);
    assert_eq!(snapshot.block_refetches, 1);
}

#[tokio::test]
async fn explicit_replay_claim_allows_failed_block_refetch() {
    let diagnostics = Arc::new(SourceDiagnosticsCollector::new(8));
    let store = BlockStore::new(
        SourcePlan {
            planned_entries: 1,
            blocks: vec![SourceRange { start: 0, end: 7 }],
        },
        8,
        Some(Arc::clone(&diagnostics)),
    );
    let entry = manifest_entry_with_span("a.txt", 0, 8);
    let first = SourceRange { start: 0, end: 7 };

    store.retain_entry(&entry);
    assert_eq!(store.reserve_fetch(0).await.unwrap(), first);
    store.finish_fetch(0, Err(std::io::Error::other("temporary range failure")));
    assert!(store.reserve_fetch(0).await.is_none());

    store.add_replay_claim_for_test(&entry);
    assert_eq!(store.reserve_fetch(0).await.unwrap(), first);
    store.finish_fetch(0, Ok(bytes::Bytes::from_static(b"abcdefgh")));

    let mut reader = BlockRangeReader::new(Arc::clone(&store), 0, 8).unwrap();
    let mut output = Vec::new();
    TokioAsyncReadExt::read_to_end(&mut reader, &mut output)
        .await
        .unwrap();

    assert_eq!(output, b"abcdefgh");
}

#[test]
fn source_diagnostics_summarizes_unique_requested_bytes() {
    let diagnostics = SourceDiagnosticsCollector::new(20);
    diagnostics.record_get_attempt(0, 9, 1);
    diagnostics.record_get_attempt(5, 14, 1);
    diagnostics.record_get_attempt(0, 9, 2);
    diagnostics.record_get_success(0, 9);
    diagnostics.record_get_success(5, 14);
    diagnostics.record_get_success(0, 9);

    let snapshot = diagnostics.snapshot();

    assert_eq!(snapshot.source_get_attempts, 3);
    assert_eq!(snapshot.source_get_retries, 1);
    assert_eq!(snapshot.fetched_source_bytes, 30);
    assert_eq!(snapshot.unique_source_bytes, 15);
    assert_eq!(snapshot.source_amplification, 2.0);
}

#[test]
fn source_diagnostics_merges_adjacent_interval_chains() {
    let diagnostics = SourceDiagnosticsCollector::new(100);
    diagnostics.record_get_attempt(10, 20, 1);
    diagnostics.record_get_attempt(30, 40, 1);
    diagnostics.record_get_attempt(21, 29, 1);
    diagnostics.record_get_attempt(41, 50, 1);
    diagnostics.record_get_success(10, 20);
    diagnostics.record_get_success(30, 40);
    diagnostics.record_get_success(21, 29);
    diagnostics.record_get_success(41, 50);

    let snapshot = diagnostics.snapshot();

    assert_eq!(snapshot.fetched_source_bytes, 41);
    assert_eq!(snapshot.unique_source_bytes, 41);
    assert_eq!(snapshot.source_amplification, 1.0);
}

#[test]
fn summarizes_report_operations() {
    let mut report = SyncReport {
        source: S3Object::parse("s3://src/archive.zip").unwrap(),
        destination: S3Prefix::parse("s3://dst/out/").unwrap(),
        summary: SyncSummary {
            zip_files: 2,
            destination_objects: 1,
            ..SyncSummary::default()
        },
        diagnostics: None,
        operations: vec![
            ObjectReport {
                status: OperationStatus::UploadedNew,
                key: "out/a".to_string(),
                zip_path: Some("a".to_string()),
                size: Some(1),
                md5: None,
                destination_etag: None,
                message: None,
            },
            ObjectReport {
                status: OperationStatus::ConditionalConflict,
                key: "out/b".to_string(),
                zip_path: Some("b".to_string()),
                size: Some(1),
                md5: None,
                destination_etag: None,
                message: Some("precondition failed".to_string()),
            },
        ],
    };

    summarize(&mut report);

    assert_eq!(report.summary.uploaded_new, 1);
    assert_eq!(report.summary.conditional_conflicts, 1);
    assert!(!report.has_errors());
}

#[test]
fn conditional_conflict_error_respects_fail_fast_option() {
    let destination = S3Prefix::parse("s3://bucket/destination/").unwrap();
    let operation = ObjectReport {
        status: OperationStatus::ConditionalConflict,
        key: "destination/file.txt".to_string(),
        zip_path: Some("file.txt".to_string()),
        size: Some(12),
        md5: None,
        destination_etag: Some("old-etag".to_string()),
        message: Some("PreconditionFailed".to_string()),
    };

    assert!(conditional_conflict_error(&destination, &operation, false).is_none());
    let err = conditional_conflict_error(&destination, &operation, true).unwrap();

    assert_eq!(
        err.to_string(),
        "conditional write failed for s3://bucket/destination/file.txt: PreconditionFailed"
    );
}

#[tokio::test]
async fn builds_manifest_from_stored_and_deflated_entries() {
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        ("nested/b.txt", Compression::Deflate, b"bravo".as_slice()),
        ("nested/", Compression::Stored, b"".as_slice()),
    ])
    .await;
    let source_len = data.len() as u64;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();

    let manifest =
        build_manifest_entries(reader.file().entries(), &destination, source_len).unwrap();
    let catalog = EmbeddedCatalog {
        version: EMBEDDED_CATALOG_VERSION,
        entries: vec![EmbeddedCatalogEntry {
            path: "a.txt".to_string(),
            md5: "\"2C1743A391305FBF367DF8E4F069F9F9\"".to_string(),
        }],
    };
    let manifest = ManifestBuild {
        entries: apply_embedded_catalog(manifest.entries, catalog_md5_by_path(catalog)),
        catalog_index: manifest.catalog_index,
    };

    assert_eq!(manifest.entries.len(), 2);
    assert_eq!(manifest.entries[0].zip_path, "a.txt");
    assert_eq!(manifest.entries[0].key, "prefix/a.txt");
    assert_eq!(manifest.entries[0].size, 5);
    assert_eq!(
        manifest.entries[0].catalog_md5.as_deref(),
        Some("2c1743a391305fbf367df8e4f069f9f9")
    );
    assert_eq!(manifest.entries[1].zip_path, "nested/b.txt");
    assert_eq!(manifest.entries[1].key, "prefix/nested/b.txt");
    assert_eq!(manifest.entries[1].size, 5);
    assert_eq!(manifest.entries[1].catalog_md5, None);

    let stored = reader.file().entries();
    assert_eq!(
        manifest.entries[0].source_span_start,
        stored[0].header_offset()
    );
    assert_eq!(
        manifest.entries[0].source_span_end,
        stored[1].header_offset()
    );
    assert_eq!(
        manifest.entries[1].source_span_start,
        stored[1].header_offset()
    );
    assert_eq!(
        manifest.entries[1].source_span_end,
        stored[2].header_offset()
    );
    assert!(manifest.entries[0].source_span_start < manifest.entries[0].source_span_end);
    assert!(manifest.entries[1].source_span_start < manifest.entries[1].source_span_end);
}

#[tokio::test]
async fn manifest_uses_zip_length_as_last_entry_span_end() {
    let data = test_zip(&[("last.txt", Compression::Stored, b"alpha".as_slice())]).await;
    let source_len = data.len() as u64;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();

    let manifest =
        build_manifest_entries(reader.file().entries(), &destination, source_len).unwrap();

    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].source_span_end, source_len);
}

#[tokio::test]
async fn manifest_rejects_header_offsets_outside_source_length() {
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        ("b.txt", Compression::Stored, b"bravo".as_slice()),
    ])
    .await;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();
    let truncated_source_len = reader.file().entries()[1].header_offset();

    let err = build_manifest_entries(reader.file().entries(), &destination, truncated_source_len)
        .unwrap_err();

    assert!(matches!(
        err,
        Error::InvalidZipEntry { ref path, ref reason }
            if path == "b.txt" && reason.contains("outside source ZIP length")
    ));
}

#[tokio::test]
async fn counts_zip_files_excluding_directories_and_embedded_catalog() {
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        ("nested/b.txt", Compression::Deflate, b"bravo".as_slice()),
        ("nested/", Compression::Stored, b"".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Stored,
            br#"{"version":1,"entries":[]}"#.as_slice(),
        ),
    ])
    .await;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();

    assert_eq!(count_zip_file_entries(reader.file().entries()).unwrap(), 2);
}

#[tokio::test]
async fn entry_reader_streams_stored_and_deflated_entries_from_cached_ranges() {
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        ("nested/b.txt", Compression::Deflate, b"bravo".as_slice()),
    ])
    .await;
    let source_len = data.len() as u64;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data.clone())
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();
    let manifest =
        build_manifest_entries(reader.file().entries(), &destination, source_len).unwrap();
    let store = block_store_from_zip(data, &manifest.entries);

    let mut stored_reader = entry_reader(Arc::clone(&store), &manifest.entries[0])
        .await
        .unwrap();
    let mut stored = Vec::new();
    TokioAsyncReadExt::read_to_end(&mut stored_reader, &mut stored)
        .await
        .unwrap();
    assert_eq!(stored, b"alpha");

    let mut deflated_reader = entry_reader(store, &manifest.entries[1]).await.unwrap();
    let mut deflated = Vec::new();
    TokioAsyncReadExt::read_to_end(&mut deflated_reader, &mut deflated)
        .await
        .unwrap();
    assert_eq!(deflated, b"bravo");
}

#[tokio::test]
async fn entry_reader_rejects_local_header_name_mismatch() {
    let data = test_zip(&[("a.txt", Compression::Stored, b"alpha".as_slice())]).await;
    let data = replace_first_local_file_name(data, b"a.txt", b"b.txt");
    let source_len = data.len() as u64;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data.clone())
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();
    let manifest =
        build_manifest_entries(reader.file().entries(), &destination, source_len).unwrap();
    let store = block_store_from_zip(data, &manifest.entries);

    let err = match entry_reader(store, &manifest.entries[0]).await {
        Ok(_) => panic!("expected local header name mismatch to be rejected"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("local file name"));
}

#[tokio::test]
async fn entry_reader_rejects_local_header_lengths_outside_source_span() {
    let data = test_zip(&[("a.txt", Compression::Stored, b"alpha".as_slice())]).await;
    let source_len = data.len() as u64;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data.clone())
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();
    let mut manifest =
        build_manifest_entries(reader.file().entries(), &destination, source_len).unwrap();
    manifest.entries[0].source_span_end = manifest.entries[0].source_offset + 30;
    let store = block_store_from_zip(data, &manifest.entries);

    let err = match entry_reader(store, &manifest.entries[0]).await {
        Ok(_) => panic!("expected local header span overflow to be rejected"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        Error::InvalidZipEntry { ref path, ref reason }
            if path == "a.txt" && reason.contains("name or extra field")
    ));
}

#[tokio::test]
async fn manifest_ignores_embedded_catalog_entry() {
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Deflate,
            br#"{"version":1,"entries":[]}"#.as_slice(),
        ),
    ])
    .await;
    let source_len = data.len() as u64;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();

    let manifest =
        build_manifest_entries(reader.file().entries(), &destination, source_len).unwrap();

    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].zip_path, "a.txt");
    assert_eq!(manifest.catalog_index, Some(1));
}

#[tokio::test]
async fn embedded_catalog_loads_normalized_md5_values() {
    let catalog_json = br#"{"version":1,"entries":[{"path":"a.txt","md5":"\"2C1743A391305FBF367DF8E4F069F9F9\""}]}"#;
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Stored,
            catalog_json.as_slice(),
        ),
    ])
    .await;

    let catalog = load_catalog_from_zip(data.clone(), data).await;

    assert_eq!(
        catalog.get("a.txt").map(String::as_str),
        Some("2c1743a391305fbf367df8e4f069f9f9")
    );
}

#[tokio::test]
async fn embedded_catalog_falls_back_on_invalid_json() {
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Stored,
            b"not json".as_slice(),
        ),
    ])
    .await;

    let catalog = load_catalog_from_zip(data.clone(), data).await;

    assert!(catalog.is_empty());
}

#[tokio::test]
async fn embedded_catalog_falls_back_on_crc_mismatch() {
    let catalog_json =
        br#"{"version":1,"entries":[{"path":"a.txt","md5":"2c1743a391305fbf367df8e4f069f9f9"}]}"#;
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Stored,
            catalog_json.as_slice(),
        ),
    ])
    .await;
    let corrupted = corrupt_first_match(data.clone(), b"2c1743");

    let catalog = load_catalog_from_zip(data, corrupted).await;

    assert!(catalog.is_empty());
}

#[tokio::test]
async fn embedded_catalog_falls_back_on_read_error() {
    let catalog_json = br#"{"version":1,"entries":[]}"#;
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Stored,
            catalog_json.as_slice(),
        ),
    ])
    .await;
    let mut source_data = data.clone();
    let reader = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();
    let catalog_index = reader
        .file()
        .entries()
        .iter()
        .position(|entry| entry.filename().as_str().unwrap() == EMBEDDED_CATALOG_PATH)
        .unwrap();
    let header_offset = reader.file().entries()[catalog_index].header_offset() as usize;
    source_data[header_offset..header_offset + 4].copy_from_slice(b"BAD!");

    let catalog = load_catalog_from_zip(source_data.clone(), source_data).await;

    assert!(catalog.is_empty());
}

#[tokio::test]
async fn embedded_catalog_falls_back_when_entry_is_too_large() {
    let catalog_json = br#"{"version":1,"entries":[]}"#;
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Stored,
            catalog_json.as_slice(),
        ),
    ])
    .await;
    let oversized = set_catalog_central_uncompressed_size(
        data,
        u32::try_from(EMBEDDED_CATALOG_MAX_BYTES + 1).unwrap(),
    );

    let catalog = load_catalog_from_zip(oversized.clone(), oversized).await;

    assert!(catalog.is_empty());
}

#[tokio::test]
async fn embedded_catalog_falls_back_when_compressed_entry_is_too_large() {
    let catalog_json = br#"{"version":1,"entries":[]}"#;
    let data = test_zip(&[
        ("a.txt", Compression::Stored, b"alpha".as_slice()),
        (
            EMBEDDED_CATALOG_PATH,
            Compression::Stored,
            catalog_json.as_slice(),
        ),
    ])
    .await;
    let oversized = set_catalog_central_compressed_size(
        data,
        u32::try_from(EMBEDDED_CATALOG_MAX_BYTES + 1).unwrap(),
    );

    let catalog = load_catalog_from_zip(oversized.clone(), oversized).await;

    assert!(catalog.is_empty());
}

#[test]
fn empty_catalog_leaves_manifest_entries_without_md5() {
    let entry = manifest_entry("file.txt", 10);
    let entries = apply_embedded_catalog(vec![entry], Default::default());

    assert_eq!(entries[0].catalog_md5, None);
}

#[test]
fn catalog_md5_skips_matching_destination_without_size_check() {
    let mut entry = manifest_entry("file.txt", 10);
    entry.catalog_md5 = Some("d41d8cd98f00b204e9800998ecf8427e".to_string());
    let destination = DestinationObject {
        etag: Some("\"D41D8CD98F00B204E9800998ECF8427E\"".to_string()),
        size: Some(999),
    };

    let report = catalog_skip_report(&entry, Some(&destination)).unwrap();

    assert_eq!(report.status, OperationStatus::SkippedUnchanged);
    assert_eq!(report.md5.as_deref(), entry.catalog_md5.as_deref());
}

#[test]
fn source_block_planner_coalesces_close_spans() {
    let entries = vec![
        manifest_entry_with_span("0.txt", 0, 200),
        manifest_entry_with_span("7.txt", 250, 500),
        manifest_entry_with_span("6.txt", 900, 1_300),
        manifest_entry_with_span("1.txt", 1_200, 1_600),
    ];

    let plan = plan_source_blocks(&entries, 2_000, 1_000, 100);

    assert_eq!(
        plan.blocks,
        vec![
            SourceRange { start: 0, end: 499 },
            SourceRange {
                start: 900,
                end: 1_599
            }
        ]
    );
}

#[test]
fn source_block_planner_does_not_coalesce_large_gaps() {
    let entries = vec![
        manifest_entry_with_span("0.txt", 0, 200),
        manifest_entry_with_span("1.txt", 700, 900),
    ];

    let plan = plan_source_blocks(&entries, 2_000, 1_000, 100);

    assert_eq!(
        plan.blocks,
        vec![
            SourceRange { start: 0, end: 199 },
            SourceRange {
                start: 700,
                end: 899
            }
        ]
    );
}

#[test]
fn source_block_planner_splits_large_spans() {
    let entries = vec![manifest_entry_with_span("large.txt", 0, 2_500)];

    let plan = plan_source_blocks(&entries, 3_000, 1_000, 100);

    assert_eq!(
        plan.blocks,
        vec![
            SourceRange { start: 0, end: 999 },
            SourceRange {
                start: 1_000,
                end: 1_999
            },
            SourceRange {
                start: 2_000,
                end: 2_499
            },
        ]
    );
}

#[test]
fn source_block_planner_clamps_to_source_length() {
    let entries = vec![manifest_entry_with_span("last.txt", 1_700, 2_500)];

    let plan = plan_source_blocks(&entries, 2_100, 1_000, 100);

    assert_eq!(
        plan.blocks,
        vec![SourceRange {
            start: 1_700,
            end: 2_099
        }]
    );
}

#[tokio::test]
async fn rejects_duplicate_zip_paths_in_manifest() {
    let data = test_zip(&[
        ("same.txt", Compression::Stored, b"first".as_slice()),
        ("same.txt", Compression::Stored, b"second".as_slice()),
    ])
    .await;
    let source_len = data.len() as u64;
    let reader = async_zip::base::read::mem::ZipFileReader::new(data)
        .await
        .unwrap();
    let destination = S3Prefix::parse("s3://bucket/prefix/").unwrap();

    let err =
        build_manifest_entries(reader.file().entries(), &destination, source_len).unwrap_err();

    assert!(matches!(err, Error::DuplicateZipPath(path) if path == "same.txt"));
}

async fn test_zip(entries: &[(&str, Compression, &[u8])]) -> Vec<u8> {
    let mut writer = async_zip::base::write::ZipFileWriter::new(Vec::new());
    for (path, compression, data) in entries {
        let entry = async_zip::ZipEntryBuilder::new((*path).to_string().into(), *compression);
        writer.write_entry_whole(entry, data).await.unwrap();
    }
    writer.close().await.unwrap()
}

async fn load_catalog_from_zip(
    zip_directory_data: Vec<u8>,
    source_data: Vec<u8>,
) -> std::collections::HashMap<String, String> {
    let directory_reader = async_zip::base::read::mem::ZipFileReader::new(zip_directory_data)
        .await
        .unwrap();
    let Some(catalog_index) = directory_reader
        .file()
        .entries()
        .iter()
        .position(|entry| entry.filename().as_str().unwrap() == EMBEDDED_CATALOG_PATH)
    else {
        return Default::default();
    };
    let catalog_entry = &directory_reader.file().entries()[catalog_index];
    if catalog_entry.uncompressed_size() > EMBEDDED_CATALOG_MAX_BYTES
        || catalog_entry.compressed_size() > EMBEDDED_CATALOG_MAX_BYTES
    {
        return Default::default();
    }

    let reader = match async_zip::base::read::mem::ZipFileReader::new(source_data).await {
        Ok(reader) => reader,
        Err(_) => return Default::default(),
    };
    let mut catalog_reader = match reader.reader_with_entry(catalog_index).await {
        Ok(reader) => reader,
        Err(_) => return Default::default(),
    };
    let mut bytes = Vec::new();
    if FuturesAsyncReadExt::read_to_end(&mut catalog_reader, &mut bytes)
        .await
        .is_err()
    {
        return Default::default();
    }
    let Ok(catalog) = serde_json::from_slice::<EmbeddedCatalog>(&bytes) else {
        return Default::default();
    };
    catalog_md5_by_path(catalog)
}

fn corrupt_first_match(mut data: Vec<u8>, needle: &[u8]) -> Vec<u8> {
    let index = data
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("test fixture should contain bytes to corrupt");
    data[index] ^= 0xff;
    data
}

fn replace_first_local_file_name(
    mut data: Vec<u8>,
    expected: &[u8],
    replacement: &[u8],
) -> Vec<u8> {
    assert_eq!(expected.len(), replacement.len());

    let index = data
        .windows(4)
        .position(|window| window == [0x50, 0x4b, 0x03, 0x04])
        .expect("test fixture should contain a local file header");
    let name_len = u16::from_le_bytes([data[index + 26], data[index + 27]]) as usize;
    let name_start = index + 30;
    let name_end = name_start + name_len;

    assert_eq!(&data[name_start..name_end], expected);
    data[name_start..name_end].copy_from_slice(replacement);
    data
}

fn set_catalog_central_uncompressed_size(mut data: Vec<u8>, size: u32) -> Vec<u8> {
    set_catalog_central_size(&mut data, 24, size);
    data
}

fn set_catalog_central_compressed_size(mut data: Vec<u8>, size: u32) -> Vec<u8> {
    set_catalog_central_size(&mut data, 20, size);
    data
}

fn set_catalog_central_size(data: &mut [u8], field_offset: usize, size: u32) {
    let mut index = 0;
    while index + 46 <= data.len() {
        if data[index..index + 4] == [0x50, 0x4b, 0x01, 0x02] {
            let name_len = u16::from_le_bytes([data[index + 28], data[index + 29]]) as usize;
            let extra_len = u16::from_le_bytes([data[index + 30], data[index + 31]]) as usize;
            let comment_len = u16::from_le_bytes([data[index + 32], data[index + 33]]) as usize;
            let name_start = index + 46;
            let name_end = name_start + name_len;
            if name_end <= data.len()
                && &data[name_start..name_end] == EMBEDDED_CATALOG_PATH.as_bytes()
            {
                data[index + field_offset..index + field_offset + 4]
                    .copy_from_slice(&size.to_le_bytes());
                return;
            }
            index = name_end + extra_len + comment_len;
        } else {
            index += 1;
        }
    }
    panic!("catalog central directory entry not found");
}

fn manifest_entry(path: &str, size: u64) -> ManifestEntry {
    ManifestEntry {
        source_offset: 0,
        source_span_start: 0,
        source_span_end: 10,
        zip_path: path.to_string(),
        key: format!("prefix/{path}"),
        size,
        compressed_size: size,
        compression: Compression::Stored,
        crc32: 0,
        catalog_md5: None,
    }
}

fn block_store_from_zip(data: Vec<u8>, entries: &[ManifestEntry]) -> Arc<BlockStore> {
    let len = data.len() as u64;
    let plan = plan_source_blocks(entries, len, len.max(1) as usize, 0);
    let store = BlockStore::new(plan, len as usize, None);
    for entry in entries {
        store.retain_entry(entry);
    }
    for index in 0..store.block_count() {
        let range = store.block_range(index).unwrap();
        let start = range.start as usize;
        let end = range.end as usize + 1;
        store.finish_fetch(index, Ok(bytes::Bytes::copy_from_slice(&data[start..end])));
    }
    store
}

fn dummy_s3_client() -> aws_sdk_s3::Client {
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
    aws_sdk_s3::Client::from_conf(config)
}

fn manifest_entry_with_span(
    path: &str,
    source_span_start: u64,
    source_span_end: u64,
) -> ManifestEntry {
    ManifestEntry {
        source_offset: source_span_start,
        source_span_start,
        source_span_end,
        zip_path: path.to_string(),
        key: format!("prefix/{path}"),
        size: source_span_end.saturating_sub(source_span_start),
        compressed_size: source_span_end.saturating_sub(source_span_start),
        compression: Compression::Stored,
        crc32: 0,
        catalog_md5: None,
    }
}

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("s3-unspool-{name}-{}-{nanos}", std::process::id()))
}
