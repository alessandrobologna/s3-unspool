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
| `S3PrefixUploadOptions` | Options for uploading an S3 prefix as a ZIP. |
| `UnzipSelection` | Include/exclude patterns for selective extraction. |
| `ZipCompression` | Compression method for generated ZIP entries. |

## Extraction Functions

| Function | Use it when |
| --- | --- |
| `sync_zip_to_s3` | One S3 client can be used for source reads and destination writes. |
| `sync_zip_to_s3_with_clients` | Source reads and destination writes need different S3 client configuration. |
| `inspect_s3_zip` | You need source ZIP size and file count before choosing runtime settings. |

## ZIP Creation Functions

| Function | Use it when |
| --- | --- |
| `upload_directory_zip_to_s3` | A local directory should become a cataloged S3 ZIP. |
| `zip_s3_prefix_to_s3` | An existing S3 prefix should become a cataloged S3 ZIP. |
| `zip_s3_prefix_to_file` | An existing S3 prefix should become a local ZIP. |

## Options That Affect Incremental Extraction

| Option | Default | Meaning |
| --- | ---: | --- |
| `SyncOptions::ignore_embedded_catalog` | `false` | Ignore the embedded catalog and force the extract-and-hash comparison path. |
| `SyncOptions::selection` | none | Restrict extraction to include/exclude patterns. |
| `SyncOptions::delete_extra` | `false` | Delete destination objects that are not in the ZIP. Not allowed with selection. |
| `SyncOptions::fail_on_conditional_conflict` | `false` | Return an error on the first destination write conflict. |

## Scheduler Tuning Options

| Option | Default | Use it to control |
| --- | ---: | --- |
| `SyncOptions::concurrency` | `64` | ZIP entries processed concurrently. |
| `SyncOptions::put_concurrency` | `16` | Destination `PutObject` requests in flight. |
| `SyncOptions::source_block_size` | 8 MiB | Maximum planned source range size. |
| `SyncOptions::source_block_merge_gap` | 256 KiB | Nearby ZIP spans coalesced into one source range. |
| `SyncOptions::source_get_concurrency` | `4` | Ranged source `GetObject` requests in flight. |
| `SyncOptions::source_window_capacity` | 64 MiB | Resident source block window. |
| `SyncOptions::put_retry_policy` | 6 attempts | Destination PUT retry and `SlowDown` backoff behavior. |

## See Also

- [Use the Rust Library](../how-to/use-rust-library.md)
- [Extract Selected Entries](../how-to/extract-selected-entries.md)
- [Performance and Lambda](../explanation/performance-and-lambda.md)
