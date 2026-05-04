# Run Live S3 Tests

The live S3 test is skipped unless `S3_UNSPOOL_LIVE_BUCKET` is set.

```sh
S3_UNSPOOL_LIVE_BUCKET=your-test-bucket \
  cargo test -p s3-unspool --test live_s3 -- --nocapture
```

The test creates a temporary prefix, exercises upload, skip, overwrite, and
delete behavior, verifies destination object contents, and deletes the
temporary objects at the end of the run.

Use a disposable test bucket. The test writes and deletes objects under a
temporary prefix.

## See Also

- [S3 Permissions](../reference/permissions.md)
- [Assumptions and Limits](../reference/assumptions-and-limits.md)
