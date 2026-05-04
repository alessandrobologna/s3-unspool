# S3 Permissions

This page lists the S3 permissions needed by extraction and S3-prefix ZIP
creation.

## Extraction

| Scope | Permission | Why |
| --- | --- | --- |
| Source ZIP object | `s3:GetObject` | Read ZIP metadata and ranged source bytes. |
| Destination bucket | `s3:ListBucket` | List destination keys and ETags once. |
| Destination prefix | `s3:PutObject` | Write missing and changed objects. |
| Destination prefix | `s3:GetObject` | Authorize conditional overwrites with `If-Match`. |
| Destination prefix | `s3:DeleteObject` | Only needed for CLI `--delete-extra` or library `delete_extra_objects()`. |

The destination `s3:GetObject` permission is required even though `s3-unspool`
does not issue per-file destination `HeadObject` requests or read destination
object bodies. S3 authorizes `PutObject` requests with `If-Match: <etag>`
against object-read permission; without destination `s3:GetObject`, changed
files are rejected with `AccessDenied`.

## S3-Prefix ZIP Creation

| Scope | Permission | Why |
| --- | --- | --- |
| Source bucket | `s3:ListBucket` | List source keys, sizes, and ETags. |
| Source prefix | `s3:GetObject` | Stream each source object into the ZIP. |
| Destination ZIP object | `s3:PutObject`, `s3:AbortMultipartUpload` | Write the generated ZIP with multipart upload and clean up failed uploads. |

## See Also

- [Assumptions and Limits](assumptions-and-limits.md)
- [Run Live S3 Tests](../how-to/run-live-s3-tests.md)
