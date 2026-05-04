# s3-unspool Documentation

These docs are organized by what you are trying to do.

| Section | Start here when |
| --- | --- |
| [Tutorials](tutorials/README.md) | You want a guided first run. |
| [How-to guides](how-to/README.md) | You already have a task and need steps. |
| [Reference](reference/README.md) | You need exact commands, API behavior, limits, or benchmark numbers. |
| [Explanation](explanation/README.md) | You want to understand the design, tradeoffs, or economics. |

If you are evaluating the project quickly, read the repository
[README](../README.md), then try the [first extract tutorial](tutorials/first-extract.md).

## Choose Your Path

- I want the fastest proof on my machine: start with
  [First Extract](tutorials/first-extract.md).
- I want to restore from an S3 ZIP into S3: use
  [Extract an S3 ZIP to S3](how-to/extract-s3-zip-to-s3.md).
- I want to decide whether compressed archive storage is worthwhile: read
  [Economics](explanation/economics.md).
- I want to understand selective extraction and bounded memory: read
  [Architecture](explanation/architecture.md), then
  [Incremental Extraction](explanation/incremental-extraction.md).
- I want measured Lambda behavior: read
  [Benchmark Snapshots](reference/benchmark-snapshots.md).

## Common Paths

- Install and try the CLI: [Install the CLI](how-to/install-cli.md)
- Embed the library in Rust: [Use the Rust Library](how-to/use-rust-library.md)
- Restore an S3 ZIP into S3: [Extract an S3 ZIP to S3](how-to/extract-s3-zip-to-s3.md)
- Restore only matching entries: [Extract Selected Entries](how-to/extract-selected-entries.md)
- Understand the architecture: [Architecture](explanation/architecture.md)
- Compare benchmark results: [Benchmark Snapshots](reference/benchmark-snapshots.md)
- Evaluate storage tradeoffs: [Economics](explanation/economics.md)
