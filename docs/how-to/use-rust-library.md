# Use the Rust Library

Use the `s3-unspool` crate when you want to run ZIP extraction or ZIP creation
inside your own Rust application.

## Install

```sh
cargo add s3-unspool aws-config aws-sdk-s3
cargo add tokio --features macros,rt-multi-thread
```

## Extract an S3 ZIP to an S3 Prefix

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use s3_unspool::{S3Object, S3Prefix, SyncOptions, sync_zip_to_s3};

#[tokio::main]
async fn main() -> s3_unspool::Result<()> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let mut extract = SyncOptions::new(
        S3Object::parse("s3://my-bucket/releases/site.zip")?,
        S3Prefix::parse("s3://my-bucket/www/")?,
    );
    extract.collect_diagnostics = true;

    let report = sync_zip_to_s3(&client, extract).await?;
    println!("changed files: {}", report.summary.uploaded_changed);

    Ok(())
}
```

## Use Separate Source and Destination Clients

For high-concurrency extraction, use separate clients so ranged source reads and
streaming destination writes use independent HTTP pools.

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::StalledStreamProtectionConfig;
use aws_sdk_s3::Client;
use s3_unspool::{S3Object, S3Prefix, SyncOptions, sync_zip_to_s3_with_clients};

#[tokio::main]
async fn main() -> s3_unspool::Result<()> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let source_client = Client::new(&config);
    let destination_client = Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .upload_enabled(false)
                    .download_enabled(true)
                    .build(),
            )
            .build(),
    );

    let extract = SyncOptions::new(
        S3Object::parse("s3://my-bucket/releases/site.zip")?,
        S3Prefix::parse("s3://my-bucket/www/")?,
    );

    sync_zip_to_s3_with_clients(&source_client, &destination_client, extract).await?;
    Ok(())
}
```

## See Also

- [Library Reference](../reference/library.md)
- [S3 Permissions](../reference/permissions.md)
- [Performance and Lambda](../explanation/performance-and-lambda.md)
