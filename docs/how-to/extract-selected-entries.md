# Extract Selected Entries

Use include and exclude patterns when you only want to restore part of an
archive. Patterns use gitignore-style syntax and match normalized ZIP paths.

## Use the CLI

```sh
s3-unspool unzip \
  s3://my-bucket/releases/site.zip \
  s3://my-bucket/restore/ \
  --include 'docs/**/*.md' \
  --include 'crates/**/README.md' \
  --exclude 'docs/drafts/**'
```

Exclude-only selections restore every non-excluded ZIP entry:

```sh
s3-unspool unzip \
  s3://my-bucket/releases/site.zip \
  ./restore \
  --exclude 'docs/drafts/**'
```

Selected extracts cannot be combined with `--delete-extra`, because unselected
destination objects are outside the restore scope.

## Use Rust

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use s3_unspool::{S3Object, S3Prefix, SyncOptions, UnzipSelection, sync_zip_to_s3};

#[tokio::main]
async fn main() -> s3_unspool::Result<()> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let extract = SyncOptions::new(
        S3Object::parse("s3://my-bucket/releases/site.zip")?,
        S3Prefix::parse("s3://my-bucket/restore/")?,
    )
    .with_selection(
        UnzipSelection::new()
            .include("docs/**/*.md")
            .include("crates/**/README.md")
            .exclude("docs/drafts/**"),
    );

    sync_zip_to_s3(&client, extract).await?;
    Ok(())
}
```

## See Also

- [CLI Reference](../reference/cli.md)
- [Library Reference](../reference/library.md)
- [Incremental Extraction](../explanation/incremental-extraction.md)
