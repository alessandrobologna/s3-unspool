# Diagnostics

Source and PUT diagnostics are collected only when
`SyncOptions::collect_diagnostics()`, CLI `--diagnostics`, or Lambda payload
`"diagnostics": true` is enabled.

Use these fields to explain a run after it completes. They are counters and
sizes collected from the source scheduler and destination PUT retry logic; they
are not per-file timing samples.

| Field | Meaning |
| --- | --- |
| `source_zip_bytes` | Source ZIP object size in bytes. |
| `planned_entries` | ZIP entries included in source plans. Catalog-skipped entries are excluded. |
| `planned_blocks` | Coalesced source blocks scheduled for hash or upload phases. |
| `fetched_blocks` | Planned source blocks fetched successfully. |
| `source_get_attempts` | Ranged S3 `GetObject` attempts, including retries. |
| `source_get_retries` | Ranged S3 `GetObject` retries after the first attempt. |
| `source_get_request_errors` | Ranged S3 `GetObject` request failures. |
| `source_get_body_errors` | Errors while reading ranged S3 `GetObject` response bodies. |
| `source_get_short_body_errors` | Ranged S3 `GetObject` responses that ended before all requested bytes were read. |
| `source_get_errors` | Source block fetches that failed after all retry attempts. |
| `planned_source_bytes` | Sum of planned source block sizes. |
| `fetched_source_bytes` | Sum of fetched source block sizes, including refetches. |
| `unique_source_bytes` | Unique source bytes covered by fetched ranges. |
| `source_amplification` | Fetched source bytes divided by unique fetched source bytes. Higher values mean retry or metadata-read overlap. |
| `block_hits` | Reader requests served from ready planned blocks. |
| `block_waits` | Reader requests that waited for a scheduled block. |
| `block_releases` | Ready source blocks released from the resident window after all planned claims consumed them. |
| `block_misses` | Reader cache misses; this should remain zero because readers cannot fetch. |
| `block_refetches` | Explicit replay fetches for blocks that had already been released. |
| `active_gets_high_water` | Highest number of concurrent ranged S3 GET requests observed. |
| `put.failed_attempts` | Failed destination `PutObject` attempts, including retryable attempts that later succeeded. |
| `put.failures_by_error_code` | Failed destination `PutObject` attempts grouped by AWS error code, or by SDK failure kind when no AWS service code exists. |
| `put.retry_attempts` | Application-level destination PUT retries scheduled after failed attempts. |
| `put.throttled_attempts` | Failed destination PUT attempts classified as throttling. |
| `put.throttle_waits` | Waits on the shared destination PUT cooldown. |
| `put.throttle_wait_millis` | Total milliseconds spent waiting on the shared destination PUT cooldown. |

The benchmark tables report medians across successful samples. Their source
columns are fetch and scheduler counts, not file counts or upload times.

## See Also

- [Reports](reports.md)
- [Architecture](../explanation/architecture.md)
