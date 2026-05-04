# Incremental Extraction

Incremental extraction is the main reason `s3-unspool` creates cataloged ZIPs.
The catalog lets a later extract skip unchanged files before decompression.

## Catalog Skip Path

Cataloged ZIPs include:

```text
.s3-unspool/catalog.v1.json
```

The catalog records each file path and MD5 digest. During extraction,
`s3-unspool` lists the destination prefix once, compares catalog MD5s with
listed destination ETags, and skips entries that already match.

This skip happens before source entry extraction. For large update runs, that is
the important optimization: unchanged entries do not need to be decompressed or
uploaded.

## Fallback Hash Path

External ZIP files are still supported. If the embedded catalog is missing or
ignored, existing destination files with comparable single-part ETags are
handled in a hash phase. The extractor reads those entries, computes MD5, and
adds only changed entries to the later upload phase.

Use `SyncOptions::ignore_embedded_catalog = true`, CLI `--ignore-catalog`, or
Lambda payload `"ignoreCatalog": true` to force this fallback path. This is
useful for measuring the catalog benefit against the same source ZIP.

## Selective Extraction

Selection patterns are applied before source range planning. That means a
partial restore still uses the same central-directory metadata and ranged-read
planner as a full restore, but only selected entries contribute source spans.

Selected extracts cannot be combined with destination cleanup. The CLI rejects
`--delete-extra` with `--include` or `--exclude` because unselected destination
objects are outside the restore scope.

## Reserved Catalog Path

The embedded catalog file is reserved. It is never extracted to the destination,
and upload sources cannot contain a file at that path.

## See Also

- [Extract Selected Entries](../how-to/extract-selected-entries.md)
- [Create a Cataloged ZIP](../how-to/create-cataloged-zip.md)
- [Reports](../reference/reports.md)
