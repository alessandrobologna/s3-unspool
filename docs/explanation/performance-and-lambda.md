# Performance and Lambda

`s3-unspool` is designed to keep memory bounded while extracting source ZIPs
that may be much larger than local scratch space or Lambda memory.

## Source Range Planning

Extraction starts by reading the ZIP central directory and listing the
destination prefix. Entries that match the embedded MD5 catalog are skipped
before any source file data is fetched. The remaining entries are converted into
a source-ordered block plan, with nearby byte spans coalesced so workers can
share ranged `GetObject` responses.

The most important tuning knobs are listed in the
[Library Reference](../reference/library.md).

## Lambda Defaults

The library defaults are conservative and tunable through `SyncOptions`.

The Lambda binary uses adaptive settings because Lambda memory controls both
available RAM and CPU. The current policy is:

| Lambda memory | Entry workers | Source block | Source GETs | PUTs |
| ---: | ---: | ---: | ---: | ---: |
| Lambda 128 MB | 4 | 8 MiB | 1 | 4 |
| Lambda 256 MB | 6 | 8 MiB | 1 | 6 |
| Lambda 512 MB | 8 | 8 MiB | 2 | 8 |
| Lambda 1024 MB | 11 | 8 MiB | 4 | 8 |
| Lambda 2048 MB | 16 | 8 MiB | 8 | 8 |

The default worker count grows with the square root of memory:

```text
workers = clamp(round(4 * sqrt(lambda_memory_mb / 128)), 4, 16)
puts = min(workers, 8)
```

## Source Block Window

After the ZIP manifest is loaded, the library resolves the source block window
from the Lambda memory budget and the real file count. The window uses otherwise
idle memory after reserving fixed runtime overhead, worker overhead, per-file
metadata overhead, and in-flight source blocks:

```text
window = M - 64 MiB - 12 MiB * workers - 2 KiB * zip_files - in_flight
in_flight = source_get_concurrency * source_block_size
if window > 512 MiB, window = window - 384 MiB
window = clamp(window, one source block, 512 MiB)
```

The window is capped by the source ZIP size. If the computed window is smaller
than one source block, the scheduler still allows one block in flight so the run
can make progress with minimal memory.

Large window budgets reserve an extra 384 MiB of RSS slack for allocator
behavior, ZIP/catalog metadata, SDK HTTP buffers, and destination PUT streams
that linger during long uploads. This is intentionally larger than the live Rust
block window because Lambda enforces RSS rather than the live Rust object graph.

## Lambda Client Behavior

The Lambda does not pre-inspect the ZIP to discover its file count. Instead, it
passes the assigned memory budget into the library, and extraction resolves the
adaptive window immediately after the ZIP manifest has been loaded.

The Lambda asks glibc to return freed pages at invocation boundaries. Warm
execution environments can otherwise retain ZIP catalog/block pages in RSS after
Rust values are dropped.

The Lambda creates separate source and destination S3 clients inside each
invocation. Separate clients keep ranged `GetObject` and streaming `PutObject`
traffic on independent HTTP pools. The destination client disables AWS SDK
upload stalled-stream protection while leaving download protection enabled,
because a streaming PUT body can legitimately pause while it waits for the
source scheduler to fetch the next planned ZIP block.

## See Also

- [Benchmark Snapshots](../reference/benchmark-snapshots.md)
- [Diagnostics](../reference/diagnostics.md)
- [Architecture](architecture.md)
