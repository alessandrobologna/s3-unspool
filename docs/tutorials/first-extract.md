# First Extract

This tutorial walks through a local ZIP round trip. It does not require AWS, so
it is the fastest way to learn the `s3-unspool` workflow.

You will:

1. Create a small source directory.
2. Build a cataloged ZIP.
3. Extract that ZIP into a new directory.
4. Preview what a repeat local extract would replace.

## Prerequisites

- Rust and Cargo installed.
- A checkout of this repository.

## Create a Source Directory

```sh
mkdir -p /tmp/s3-unspool-demo/site/docs
printf '# Home\n' > /tmp/s3-unspool-demo/site/index.md
printf '# Guide\n' > /tmp/s3-unspool-demo/site/docs/guide.md
```

## Create a Cataloged ZIP

From the repository root, run:

```sh
cargo run -p s3-unspool-cli -- \
  zip /tmp/s3-unspool-demo/site /tmp/s3-unspool-demo/site.zip
```

The ZIP includes an embedded catalog at `.s3-unspool/catalog.v1.json`. S3
destination extracts can use that catalog to skip unchanged files before
decompressing them.

## Extract the ZIP

```sh
cargo run -p s3-unspool-cli -- \
  unzip /tmp/s3-unspool-demo/site.zip /tmp/s3-unspool-demo/out
```

Check the extracted files:

```sh
find /tmp/s3-unspool-demo/out -type f | sort
```

Expected files:

```text
/tmp/s3-unspool-demo/out/docs/guide.md
/tmp/s3-unspool-demo/out/index.md
```

## Preview a Repeat Extract

```sh
cargo run -p s3-unspool-cli -- \
  unzip --dry-run --report /tmp/s3-unspool-demo/site.zip /tmp/s3-unspool-demo/out
```

Expected output includes:

```text
✓ Unzip dry run complete
  └ 2 entries: 0 would create, 2 would replace, 0 unchanged
    Report:
      Source: /tmp/s3-unspool-demo/site.zip
      Destination: /tmp/s3-unspool-demo/out
      ZIP entries: 2 entries
      Operations: 0 would create, 2 would replace, 0 unchanged, 0 would delete
```

Local extraction is deliberately simple: it writes the selected ZIP entries to
local files. The catalog skip path is most visible for S3 destinations, where
`s3-unspool` can compare catalog MD5s with destination ETags before
decompressing unchanged entries.

## Next Steps

- Use real S3 endpoints with [Create a Cataloged ZIP](../how-to/create-cataloged-zip.md).
- See catalog-based skipping with [Extract an S3 ZIP to S3](../how-to/extract-s3-zip-to-s3.md).
- Restore only part of an archive with [Extract Selected Entries](../how-to/extract-selected-entries.md).
- Look up CLI flags in the [CLI Reference](../reference/cli.md).
