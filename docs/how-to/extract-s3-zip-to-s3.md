# Extract an S3 ZIP to S3

Use this guide when you have a ZIP object in S3 and want to materialize all or
part of it into an S3 prefix. This is the main production path for incremental
and selective extraction.

## Before You Begin

You need:

- the `s3-unspool` CLI installed; see [Install the CLI](install-cli.md)
- AWS credentials for the target account
- a source ZIP object in S3, preferably created by `s3-unspool zip` so it
  includes `.s3-unspool/catalog.v1.json`
- destination permissions for list, write, and conditional overwrite behavior;
  see [S3 Permissions](../reference/permissions.md)

Costs are workload dependent. This workflow issues S3 `GetObject` requests
against the source ZIP, one `ListObjectsV2` pass against the destination prefix,
and `PutObject` requests for new or changed destination objects.

## Set the Source and Destination

```sh
SOURCE=s3://my-bucket/releases/site.zip
DESTINATION=s3://my-bucket/www/
```

Use a non-empty destination prefix. `--delete-extra` is rejected at the bucket
root because it would make cleanup too broad.

## Preview the Extract

Run a dry run first:

```sh
s3-unspool unzip \
  --dry-run \
  --report \
  "$SOURCE" \
  "$DESTINATION"
```

The report shows what would be created, replaced, skipped, or deleted without
writing any destination objects.

## Extract with a JSON Report

```sh
s3-unspool unzip \
  --diagnostics \
  --report=extract-report.json \
  "$SOURCE" \
  "$DESTINATION"
```

On an empty destination, the summary should show new uploads. A later extract of
the same cataloged ZIP should be up to date:

```text
✓ Up to date
  └ 2 entries unchanged
    Destination: 2 objects listed
```

The JSON report summary for an unchanged rerun has the same shape as:

```json
{
  "zip_files": 2,
  "destination_objects": 2,
  "uploaded_new": 0,
  "uploaded_changed": 0,
  "skipped_unchanged": 2,
  "conditional_conflicts": 0,
  "deleted_extra": 0,
  "errors": 0
}
```

The important field is `skipped_unchanged`: those entries matched the embedded
catalog and destination ETags, so they did not need to be decompressed and
uploaded again.

## Extract Only Matching Entries

Use include and exclude patterns to restore a subset of the ZIP:

```sh
s3-unspool unzip \
  --report=docs-report.json \
  "$SOURCE" \
  "$DESTINATION" \
  --include 'docs/**/*.md' \
  --include 'crates/**/README.md' \
  --exclude 'docs/drafts/**'
```

Patterns match normalized ZIP paths. Selection is applied before source range
planning, so only selected entries contribute to the source byte ranges fetched
from S3.

## Use Cleanup Carefully

Use `--delete-extra` only for full-prefix syncs where the ZIP is intended to be
the complete desired state of the destination prefix:

```sh
s3-unspool unzip \
  --delete-extra \
  --report=sync-report.json \
  "$SOURCE" \
  "$DESTINATION"
```

Do not use `--delete-extra` for partial restores. The CLI rejects
`--delete-extra` with `--include` or `--exclude` because unselected destination
objects are outside the restore scope.

## Production Safeguards

`s3-unspool` lists the destination prefix once, then uses conditional writes:

- missing files are written with `If-None-Match: *`
- changed files are written with `If-Match: <listed destination ETag>`
- conditional conflicts are reported instead of silently overwriting concurrent
  changes

Destination ETags must be comparable single-part MD5 ETags for the fastest
unchanged-skip behavior. Destination objects that do not expose comparable MD5
ETags, such as multipart or SSE-C objects, cannot use the direct catalog-to-ETag
comparison. See [Assumptions and Limits](../reference/assumptions-and-limits.md).

## See Also

- [Create a Cataloged ZIP](create-cataloged-zip.md)
- [Extract Selected Entries](extract-selected-entries.md)
- [Reports](../reference/reports.md)
- [Diagnostics](../reference/diagnostics.md)
