# Library Reference

The published `s3-unspool` crate exposes extraction and ZIP creation helpers for
local and S3 endpoints.

## Core Types

| Type | Purpose |
| --- | --- |
| `S3Object` | Parsed `s3://bucket/key` object location. |
| `S3Prefix` | Parsed `s3://bucket/prefix/` destination or source prefix. |
| `SyncOptions` | Extraction options for syncing a ZIP into a destination. |
| `UploadOptions` | Options for uploading a local directory as a ZIP. |
| `LocalZipOptions` | Options for writing a local directory to a local ZIP. |
| `S3PrefixUploadOptions` | Options for uploading an S3 prefix as a ZIP. |
| `S3PrefixLocalZipOptions` | Options for writing an S3 prefix to a local ZIP. |
| `LocalZipSyncOptions` | Options for extracting a local ZIP into an S3 prefix. |
| `S3ZipLocalUnzipOptions` | Options for extracting an S3 ZIP into a local directory. |
| `LocalUnzipOptions` | Options for extracting a local ZIP into a local directory. |
| `UnzipSelection` | Include/exclude patterns for selective extraction. |
| `ZipCompression` | Compression method for generated ZIP entries. |
| `DestinationCleanup` | Cleanup policy for unzip-to-S3 operations. |
| `ComparisonMode` | Catalog/hash comparison policy for unzip operations. |
| `ConflictPolicy` | Conditional write conflict handling policy. |
| `AdaptiveSourceWindow` | Named inputs for Lambda-style source window sizing. |

## Extraction Functions

| Function | Use it when |
| --- | --- |
| `sync_zip_to_s3` | An S3 ZIP should be extracted into an S3 prefix with one S3 client. |
| `sync_zip_to_s3_with_clients` | An S3 ZIP should be extracted into S3 with separate source and destination clients. |
| `unzip_file_to_s3` | A local ZIP should be extracted into an S3 prefix. |
| `unzip_s3_zip_to_local` | An S3 ZIP should be extracted into a local directory. |
| `unzip_file_to_local` | A local ZIP should be extracted into a local directory. |
| `inspect_s3_zip` | You need source ZIP size and file count before choosing runtime settings. |

## Extraction Dry-Run Functions

| Function | Use it when |
| --- | --- |
| `dry_run_sync_zip_to_s3` | Preview an S3 ZIP to S3-prefix extract. |
| `dry_run_sync_zip_to_s3_with_clients` | Preview an S3 ZIP to S3-prefix extract with separate clients. |
| `dry_run_unzip_file_to_s3` | Preview a local ZIP to S3-prefix extract. |
| `dry_run_unzip_s3_zip_to_local` | Preview an S3 ZIP to local-directory extract. |
| `dry_run_unzip_file_to_local` | Preview a local ZIP to local-directory extract. |

## ZIP Creation Functions

| Function | Use it when |
| --- | --- |
| `upload_directory_zip_to_s3` | A local directory should become a cataloged S3 ZIP. |
| `zip_directory_to_file` | A local directory should become a local ZIP. |
| `zip_s3_prefix_to_s3` | An existing S3 prefix should become a cataloged S3 ZIP. |
| `zip_s3_prefix_to_file` | An existing S3 prefix should become a local ZIP. |

## ZIP Creation Dry-Run Functions

| Function | Use it when |
| --- | --- |
| `dry_run_upload_directory_zip_to_s3` | Preview a local directory to S3 ZIP upload. |
| `dry_run_zip_directory_to_file` | Preview a local directory to local ZIP write. |
| `dry_run_zip_s3_prefix_to_s3` | Preview an S3 prefix to S3 ZIP upload. |
| `dry_run_zip_s3_prefix_to_file` | Preview an S3 prefix to local ZIP write. |

## Options That Affect Incremental Extraction

| Builder or Policy | Default | Meaning |
| --- | ---: | --- |
| `with_comparison_mode(ComparisonMode::CatalogThenHash)` | enabled | Use the embedded catalog when present, then fall back to entry hashing. |
| `force_hash_comparison()` | disabled | Ignore the embedded catalog and force the extract-and-hash comparison path. |
| `with_selection(...)` | none | Restrict extraction to include/exclude patterns. |
| `delete_extra_objects()` | disabled | Delete destination objects that are not in the ZIP. Not allowed with selection. |
| `with_conflict_policy(ConflictPolicy::ReportAndContinue)` | enabled | Record destination write conflicts and continue. |
| `fail_on_conflict()` | disabled | Return an error on the first destination write conflict. |
| `without_operations()` | disabled | Omit per-object operation records from the returned report. |

## Scheduler Tuning Options

| Builder | Default | Use it to control |
| --- | ---: | --- |
| `with_concurrency(...)` | `64` | ZIP entries processed concurrently. |
| `with_put_concurrency(...)` | `16` | Destination `PutObject` requests in flight. |
| `with_source_block_size(...)` | 8 MiB | Maximum planned source range size. |
| `with_source_block_merge_gap(...)` | 256 KiB | Nearby ZIP spans coalesced into one source range. |
| `with_source_get_concurrency(...)` | `4` | Ranged source `GetObject` requests in flight. |
| `with_source_window_capacity(...)` | 64 MiB | Resident source block window. |
| `with_source_window_memory_budget_mb(...)` | unset | Derive the resident source block window from available memory. |
| `with_put_retry_policy(...)` | 6 attempts | Destination PUT retry and `SlowDown` backoff behavior. |

`SyncOptions` has read-only accessors for these tuning knobs. For adaptive
window sizing, the accessor returns the configured value; collect diagnostics
and read `SyncDiagnostics::source_window_capacity` to inspect the effective
post-manifest capacity used by a run.

## See Also

- [Use the Rust Library](../how-to/use-rust-library.md)
- [Extract Selected Entries](../how-to/extract-selected-entries.md)
- [Performance and Lambda](../explanation/performance-and-lambda.md)
