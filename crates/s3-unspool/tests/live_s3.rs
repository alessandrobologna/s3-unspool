use std::env;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use async_zip::{Compression, ZipEntryBuilder};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use s3_unspool::{
    LocalZipOptions, OperationStatus, S3Object, S3Prefix, S3PrefixLocalZipOptions,
    S3PrefixUploadOptions, S3ZipLocalUnzipOptions, SyncOptions, UploadOptions, sync_zip_to_s3,
    unzip_s3_zip_to_local, upload_directory_zip_to_s3, zip_directory_to_file,
    zip_s3_prefix_to_file, zip_s3_prefix_to_s3,
};

type BoxError = Box<dyn Error + Send + Sync>;

#[tokio::test]
async fn live_s3_sync_uploads_skips_changes_and_deletes() -> Result<(), BoxError> {
    let Ok(bucket) = env::var("S3_UNSPOOL_LIVE_BUCKET") else {
        eprintln!(
            "skipping live_s3_sync_uploads_skips_changes_and_deletes: \
             S3_UNSPOOL_LIVE_BUCKET is not set"
        );
        return Ok(());
    };

    let prefix =
        env::var("S3_UNSPOOL_LIVE_PREFIX").unwrap_or_else(|_| "s3-unspool-live".to_string());
    let trimmed_prefix = prefix.trim_matches('/');
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = if trimmed_prefix.is_empty() {
        run_id.to_string()
    } else {
        format!("{trimmed_prefix}/{run_id}")
    };

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let test_result = run_live_sync_case(&client, &bucket, &root).await;
    let cleanup_result = cleanup_prefix(&client, &bucket, &root).await;

    match (test_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(test_err), Ok(())) => Err(test_err),
        (Ok(()), Err(cleanup_err)) => Err(cleanup_err),
        (Err(test_err), Err(cleanup_err)) => {
            eprintln!("cleanup failed after test failure for prefix '{root}': {cleanup_err}");
            Err(test_err)
        }
    }
}

async fn run_live_sync_case(client: &Client, bucket: &str, root: &str) -> Result<(), BoxError> {
    let source_key = format!("{root}/source/archive.zip");
    let destination_prefix = format!("{root}/dest/");

    let zip = build_zip()
        .await
        .map_err(|err| format!("failed to build ZIP fixture: {err}"))?;

    put_bytes(client, bucket, &source_key, zip).await?;
    put_bytes(
        client,
        bucket,
        &format!("{destination_prefix}unchanged.txt"),
        b"already current".to_vec(),
    )
    .await?;
    put_bytes(
        client,
        bucket,
        &format!("{destination_prefix}changed.txt"),
        b"old content".to_vec(),
    )
    .await?;
    put_bytes(
        client,
        bucket,
        &format!("{destination_prefix}extra.txt"),
        b"delete me".to_vec(),
    )
    .await?;

    let options = SyncOptions::new(
        S3Object::parse(format!("s3://{bucket}/{source_key}"))?,
        S3Prefix::parse(format!("s3://{bucket}/{destination_prefix}"))?,
    )
    .delete_extra_objects()
    .with_concurrency(4);

    let report = sync_zip_to_s3(client, options).await?;

    ensure_eq(report.summary.zip_files, 4, "zip entry count")?;
    ensure_eq(
        report.summary.destination_objects,
        3,
        "destination object count",
    )?;
    ensure_eq(report.summary.uploaded_new, 2, "new uploads")?;
    ensure_eq(report.summary.uploaded_changed, 1, "changed uploads")?;
    ensure_eq(report.summary.skipped_unchanged, 1, "unchanged skips")?;
    ensure_eq(report.summary.deleted_extra, 1, "extra deletes")?;
    ensure_eq(
        report.summary.conditional_conflicts,
        0,
        "conditional conflicts",
    )?;
    ensure_eq(report.summary.errors, 0, "errors")?;

    ensure_status(
        &report.operations,
        "unchanged.txt",
        OperationStatus::SkippedUnchanged,
    )?;
    ensure_status(
        &report.operations,
        "changed.txt",
        OperationStatus::UploadedChanged,
    )?;
    ensure_status(
        &report.operations,
        "nested/new.txt",
        OperationStatus::UploadedNew,
    )?;
    ensure_status(&report.operations, "empty/", OperationStatus::UploadedNew)?;
    ensure_status(
        &report.operations,
        "extra.txt",
        OperationStatus::DeletedExtra,
    )?;

    ensure_eq(
        get_bytes(
            client,
            bucket,
            &format!("{destination_prefix}unchanged.txt"),
        )
        .await?,
        b"already current".to_vec(),
        "unchanged object content",
    )?;
    ensure_eq(
        get_bytes(client, bucket, &format!("{destination_prefix}changed.txt")).await?,
        b"new content".to_vec(),
        "changed object content",
    )?;
    ensure_eq(
        get_bytes(
            client,
            bucket,
            &format!("{destination_prefix}nested/new.txt"),
        )
        .await?,
        b"brand new".to_vec(),
        "new object content",
    )?;
    ensure_eq(
        get_bytes(client, bucket, &format!("{destination_prefix}empty/")).await?,
        Vec::new(),
        "directory marker content",
    )?;

    let keys = list_keys(client, bucket, &destination_prefix).await?;
    if keys.contains(&format!("{destination_prefix}extra.txt")) {
        return Err("extra destination object still exists after delete-extra".into());
    }

    let roundtrip_key = format!("{root}/roundtrip/archive.zip");
    let roundtrip_report = zip_s3_prefix_to_s3(
        client,
        S3PrefixUploadOptions::new(
            S3Prefix::parse(format!("s3://{bucket}/{destination_prefix}"))?,
            S3Object::parse(format!("s3://{bucket}/{roundtrip_key}"))?,
        ),
    )
    .await?;
    ensure_eq(roundtrip_report.files, 3, "roundtrip file count")?;
    ensure_eq(roundtrip_report.directories, 1, "roundtrip directory count")?;
    ensure_eq(roundtrip_report.entries, 4, "roundtrip entry count")?;

    let roundtrip_zip = get_bytes(client, bucket, &roundtrip_key).await?;
    ensure_zip_contains(&roundtrip_zip, "empty/").await?;

    let local_unzip_dir = env::temp_dir().join(format!(
        "s3-unspool-live-unzip-{root_run}",
        root_run = root.replace('/', "-")
    ));
    let local_zip_dir = env::temp_dir().join(format!(
        "s3-unspool-live-zip-{root_run}",
        root_run = root.replace('/', "-")
    ));
    let local_source_dir = env::temp_dir().join(format!(
        "s3-unspool-live-source-{root_run}",
        root_run = root.replace('/', "-")
    ));
    let local_zip_path = local_zip_dir.join("prefix.zip");

    unzip_s3_zip_to_local(
        client,
        S3ZipLocalUnzipOptions::new(
            S3Object::parse(format!("s3://{bucket}/{roundtrip_key}"))?,
            &local_unzip_dir,
        ),
    )
    .await?;
    ensure_eq(
        tokio::fs::read(local_unzip_dir.join("nested").join("new.txt")).await?,
        b"brand new".to_vec(),
        "s3 zip to local file content",
    )?;
    ensure_eq(
        tokio::fs::metadata(local_unzip_dir.join("empty"))
            .await?
            .is_dir(),
        true,
        "s3 zip to local empty directory",
    )?;

    tokio::fs::create_dir_all(&local_zip_dir).await?;
    zip_s3_prefix_to_file(
        client,
        S3PrefixLocalZipOptions::new(
            S3Prefix::parse(format!("s3://{bucket}/{destination_prefix}"))?,
            &local_zip_path,
        ),
    )
    .await?;
    let local_prefix_zip = tokio::fs::read(&local_zip_path).await?;
    ensure_zip_contains(&local_prefix_zip, "empty/").await?;

    tokio::fs::create_dir_all(local_source_dir.join("empty")).await?;
    tokio::fs::write(local_source_dir.join("local.txt"), b"from local").await?;
    let local_source_zip_key = format!("{root}/local-source/archive.zip");
    upload_directory_zip_to_s3(
        client,
        UploadOptions::new(
            &local_source_dir,
            S3Object::parse(format!("s3://{bucket}/{local_source_zip_key}"))?,
        ),
    )
    .await?;
    let local_source_extract_prefix = format!("{root}/local-source-dest/");
    sync_zip_to_s3(
        client,
        SyncOptions::new(
            S3Object::parse(format!("s3://{bucket}/{local_source_zip_key}"))?,
            S3Prefix::parse(format!("s3://{bucket}/{local_source_extract_prefix}"))?,
        ),
    )
    .await?;
    ensure_eq(
        get_bytes(
            client,
            bucket,
            &format!("{local_source_extract_prefix}local.txt"),
        )
        .await?,
        b"from local".to_vec(),
        "local dir to s3 zip to s3 prefix file content",
    )?;
    ensure_eq(
        get_bytes(
            client,
            bucket,
            &format!("{local_source_extract_prefix}empty/"),
        )
        .await?,
        Vec::new(),
        "local dir to s3 zip to s3 prefix marker",
    )?;

    let local_only_zip_path = local_zip_dir.join("local.zip");
    zip_directory_to_file(LocalZipOptions::new(
        &local_source_dir,
        &local_only_zip_path,
    ))
    .await?;
    ensure_zip_contains(&tokio::fs::read(&local_only_zip_path).await?, "empty/").await?;

    let _ = tokio::fs::remove_dir_all(local_unzip_dir).await;
    let _ = tokio::fs::remove_dir_all(local_zip_dir).await;
    let _ = tokio::fs::remove_dir_all(local_source_dir).await;

    Ok(())
}

async fn build_zip() -> Result<Vec<u8>, async_zip::error::ZipError> {
    let mut writer = async_zip::base::write::ZipFileWriter::new(Vec::new());
    writer
        .write_entry_whole(
            ZipEntryBuilder::new("unchanged.txt".to_string().into(), Compression::Stored),
            b"already current",
        )
        .await?;
    writer
        .write_entry_whole(
            ZipEntryBuilder::new("changed.txt".to_string().into(), Compression::Deflate),
            b"new content",
        )
        .await?;
    writer
        .write_entry_whole(
            ZipEntryBuilder::new("nested/new.txt".to_string().into(), Compression::Deflate),
            b"brand new",
        )
        .await?;
    writer
        .write_entry_whole(
            ZipEntryBuilder::new("empty/".to_string().into(), Compression::Stored),
            b"",
        )
        .await?;
    writer.close().await
}

async fn ensure_zip_contains(data: &[u8], path: &str) -> Result<(), BoxError> {
    let reader = async_zip::base::read::mem::ZipFileReader::new(data.to_vec()).await?;
    if reader
        .file()
        .entries()
        .iter()
        .any(|entry| entry.filename().as_str().ok() == Some(path))
    {
        Ok(())
    } else {
        Err(format!("roundtrip ZIP is missing {path}").into())
    }
}

async fn put_bytes(
    client: &Client,
    bucket: &str,
    key: &str,
    bytes: Vec<u8>,
) -> Result<(), BoxError> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(bytes))
        .send()
        .await?;
    Ok(())
}

async fn get_bytes(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>, BoxError> {
    let output = client.get_object().bucket(bucket).key(key).send().await?;
    Ok(output.body.collect().await?.into_bytes().to_vec())
}

async fn list_keys(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<String>, BoxError> {
    let mut keys = Vec::new();
    let mut continuation = None::<String>;

    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(token) = continuation.take() {
            request = request.continuation_token(token);
        }

        let output = request.send().await?;
        keys.extend(
            output
                .contents()
                .iter()
                .filter_map(|object| object.key().map(str::to_string)),
        );

        if output.is_truncated().unwrap_or(false) {
            continuation = Some(
                output
                    .next_continuation_token()
                    .map(str::to_string)
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "S3 list_objects_v2 for bucket {bucket:?} and prefix {prefix:?} was truncated but did not include next_continuation_token"
                        ))
                    })?,
            );
        } else {
            break;
        }
    }

    Ok(keys)
}

async fn cleanup_prefix(client: &Client, bucket: &str, root: &str) -> Result<(), BoxError> {
    let prefix = format!("{}/", root.trim_end_matches('/'));
    let keys = list_keys(client, bucket, &prefix).await?;

    for chunk in keys.chunks(1000) {
        let objects = chunk
            .iter()
            .map(|key| ObjectIdentifier::builder().key(key).build())
            .collect::<Result<Vec<_>, _>>()?;
        if objects.is_empty() {
            continue;
        }

        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(true)
            .build()?;

        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;
    }

    Ok(())
}

fn ensure_status(
    operations: &[s3_unspool::ObjectReport],
    suffix: &str,
    expected: OperationStatus,
) -> Result<(), BoxError> {
    let Some(operation) = operations
        .iter()
        .find(|operation| operation_matches(operation, suffix))
    else {
        return Err(format!("missing operation ending with {suffix}").into());
    };

    ensure_eq(operation.status.clone(), expected, suffix)
}

fn operation_matches(operation: &s3_unspool::ObjectReport, suffix: &str) -> bool {
    if operation.zip_path.as_deref() == Some(suffix) {
        return true;
    }

    operation.zip_path.is_none()
        && operation
            .key
            .rsplit_once('/')
            .map(|(_, basename)| basename == suffix)
            .unwrap_or_else(|| operation.key == suffix)
}

fn ensure_eq<T>(actual: T, expected: T, label: &str) -> Result<(), BoxError>
where
    T: std::fmt::Debug + PartialEq,
{
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{label}: expected {expected:?}, got {actual:?}").into())
    }
}
