# Reports

The CLI can emit human-readable summaries and JSON reports for zip and unzip
operations.

## Interactive Output

Interactive zip and unzip commands show a single-line spinner with elapsed time
and progress where available:

```text
• Zipping 00:03 [█████▍            ] 30% 18 MiB/512 MiB file 42/1000
```

The spinner is written to stderr, clears itself before the final summary, and is
disabled by `--quiet` or non-interactive output.

## Human-Readable Reports

Use bare `--report` to expand the final transcript:

```sh
s3-unspool unzip \
  --diagnostics \
  --report \
  s3://my-bucket/releases/site.zip \
  s3://my-bucket/www/
```

## JSON Reports

Use `--report=PATH` when you want JSON for automation:

```sh
s3-unspool unzip \
  --report=report.json \
  s3://my-bucket/releases/site.zip \
  s3://my-bucket/www/
```

Formatted zip reports contain the source tree, destination ZIP, file and
directory counts, uncompressed bytes, ZIP bytes, wall time, and zip speed in
MiB/s.

Unzip reports contain:

- `summary`: totals for uploaded, skipped, conflicted, deleted, and errored
  objects.
- `operations`: one record per relevant object.
- `diagnostics`: optional source scheduler and block cache counters when
  diagnostics are enabled for `s3://` ZIP sources, plus failed/retried
  `PutObject` counters when the destination is S3.

Example unzip summary:

```json
{
  "zip_files": 1000,
  "destination_objects": 1000,
  "uploaded_new": 0,
  "uploaded_changed": 100,
  "skipped_unchanged": 900,
  "conditional_conflicts": 0,
  "deleted_extra": 0,
  "errors": 0
}
```

## See Also

- [CLI Reference](cli.md)
- [Diagnostics](diagnostics.md)
