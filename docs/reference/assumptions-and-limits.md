# Assumptions and Limits

`s3-unspool` is optimized for predictable ZIP extraction and S3 synchronization,
not for every archive or object-storage workflow.

- The crate is built for Rust 1.95 and edition 2024.
- ZIP extraction supports Stored, Deflate, and Zstandard method 93 entries when
  default features are enabled. Build with `default-features = false` to omit
  Zstd support.
- Local zip sources must be local directories and include regular files plus
  empty directories recursively.
- S3-prefix zip sources include regular objects and zero-byte trailing-slash
  directory marker objects recursively.
- Symbolic links and other special files are rejected.
- Zip source paths must be UTF-8 and cannot contain backslashes.
- ZIP entry paths must be relative UTF-8 paths with no absolute roots, `..`,
  empty components, Windows drive prefixes, or backslashes.
- Zip sources cannot contain `.s3-unspool/catalog.v1.json`.
- S3-prefix zip rejects nonzero objects whose keys end in `/`.
- S3 ZIP destinations use S3 multipart upload so the archive can be streamed once
  without precomputing its final compressed size.
- Destination objects are assumed to be written by this tool or by equivalent
  single-part `PutObject` writes.
- Destination ETags are assumed to be MD5 hashes of object content. SSE-C and
  multipart destination objects are out of scope for ETag comparison.
- Destination writes use single `PutObject` requests, not multipart upload.
  Objects larger than the S3 single-PUT limit are rejected or fail.
- IAM policies for conditional overwrites must allow `s3:GetObject` on
  destination objects as well as `s3:PutObject`.
- Source reads are pinned to the source object ETag observed at the start of the
  run. If the source ZIP changes mid-run, extraction fails or reports errors
  instead of mixing old and new source bytes.

## See Also

- [S3 Permissions](permissions.md)
- [Architecture](../explanation/architecture.md)
