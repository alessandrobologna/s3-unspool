use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use async_zip::base::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::BytesMut;
use futures_lite::io::{AsyncWrite, AsyncWriteExt as FuturesAsyncWriteExt};
use md5::{Digest, Md5};
use serde::Serialize;
use tokio::io::{AsyncReadExt as TokioAsyncReadExt, DuplexStream};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::catalog::EmbeddedCatalogEntry;
use crate::constants::{
    EMBEDDED_CATALOG_PATH, EMBEDDED_CATALOG_VERSION, MAX_BODY_CHUNK_SIZE, MAX_PIPE_CAPACITY,
    S3_SINGLE_PUT_LIMIT,
};
use crate::error::{Error, Result, aws_error_message};
use crate::extract::normalized_list_prefix;
use crate::options::{
    LocalZipOptions, S3PrefixLocalZipOptions, S3PrefixUploadOptions, UploadOptions, UploadProgress,
    UploadProgressHandler,
};
use crate::report::{LocalZipReport, S3PrefixUploadReport, UploadReport};
use crate::s3_uri::{S3Object, S3Prefix};
use crate::zip_manifest::normalize_zip_entry_path;

const UPLOAD_PROGRESS_BYTE_GRANULARITY: u64 = 8 * 1024 * 1024;
const UPLOAD_MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
const S3_MULTIPART_MAX_PARTS: i32 = 10_000;
const UPLOAD_MULTIPART_MAX_ZIP_BYTES: u64 =
    UPLOAD_MULTIPART_PART_SIZE as u64 * S3_MULTIPART_MAX_PARTS as u64;

#[derive(Clone, Debug)]
pub(crate) struct UploadEntry {
    pub(crate) zip_path: String,
    pub(crate) size: u64,
    pub(crate) source: UploadEntrySource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UploadEntryKind {
    File,
    Directory,
}

#[derive(Clone, Debug)]
pub(crate) enum UploadEntrySource {
    LocalFile(PathBuf),
    S3Object {
        bucket: String,
        key: String,
        etag: Option<String>,
    },
    Directory,
}

impl UploadEntry {
    fn is_file(&self) -> bool {
        !self.is_directory()
    }

    fn is_directory(&self) -> bool {
        matches!(self.source, UploadEntrySource::Directory)
    }
}

#[derive(Serialize)]
struct EmbeddedCatalogRef<'a> {
    version: u32,
    entries: &'a [EmbeddedCatalogEntry],
}

/// Zips a local directory and uploads the archive to S3.
///
/// The ZIP body is streamed directly into an S3 multipart upload; no local
/// archive file is created and no preflight ZIP sizing pass is required. By
/// default, the archive includes an embedded catalog that accelerates later
/// incremental extracts. Multipart uploads use fixed 16 MiB parts, so the
/// generated ZIP archive can contain up to about 160 GiB before reaching S3's
/// 10,000-part completion limit.
pub async fn upload_directory_zip_to_s3(
    client: &Client,
    options: UploadOptions,
) -> Result<UploadReport> {
    validate_upload_options(&options)?;

    let entries = collect_upload_entries(&options.source_dir).await?;
    let files = entries.iter().filter(|entry| entry.is_file()).count();
    let directories = entries.len().saturating_sub(files);
    let total_entries = entries.len();
    let uncompressed_bytes = upload_entries_uncompressed_bytes(&entries)?;
    if let Some(progress) = &options.progress {
        progress.emit(UploadProgress::Planned {
            total_files: total_entries,
            total_bytes: uncompressed_bytes,
        });
    }

    let upload_id = create_upload(client, &options.destination).await?;
    let (writer, reader) = tokio::io::duplex(options.pipe_capacity);
    let include_catalog = options.include_catalog;
    let progress = options.progress.clone();
    let finish_progress = progress.clone();
    let total_bytes = uncompressed_bytes;
    let producer = tokio::spawn(async move {
        write_upload_zip(writer.compat_write(), &entries, include_catalog, progress)
            .await
            .inspect(|_| {
                if let Some(progress) = &finish_progress {
                    progress.emit(UploadProgress::Finished {
                        total_files: total_entries,
                        total_bytes,
                    });
                }
            })
    });

    let upload_result = upload_parts(
        client,
        &options.destination,
        &upload_id,
        reader,
        options.body_chunk_size,
    )
    .await;
    let (parts, zip_bytes) = match upload_result {
        Ok(result) => result,
        Err(err) => {
            abort_producer(producer).await;
            return Err(
                abort_upload_after_error(client, &options.destination, &upload_id, err).await,
            );
        }
    };

    match producer.await {
        Ok(Ok(_catalog)) => {}
        Ok(Err(err)) => {
            return Err(
                abort_upload_after_error(client, &options.destination, &upload_id, err).await,
            );
        }
        Err(err) => {
            return Err(abort_upload_after_error(
                client,
                &options.destination,
                &upload_id,
                err.into(),
            )
            .await);
        }
    }

    if let Err(err) = validate_multipart_upload_has_data(&parts, zip_bytes) {
        return Err(abort_upload_after_error(client, &options.destination, &upload_id, err).await);
    }

    if let Err(err) = complete_upload(client, &options.destination, &upload_id, parts).await {
        return Err(abort_upload_after_error(client, &options.destination, &upload_id, err).await);
    }

    Ok(UploadReport {
        source_dir: options.source_dir.display().to_string(),
        destination: options.destination,
        files,
        directories,
        uncompressed_bytes,
        zip_bytes,
    })
}

/// Zips a local directory into a local ZIP file.
///
/// The destination is written to a temporary sibling file first and renamed into
/// place only after ZIP generation succeeds.
pub async fn zip_directory_to_file(options: LocalZipOptions) -> Result<LocalZipReport> {
    validate_local_zip_options(&options).await?;

    let entries = collect_upload_entries_with_size_limit(&options.source_dir, None).await?;
    let files = entries.iter().filter(|entry| entry.is_file()).count();
    let directories = entries.len().saturating_sub(files);
    let uncompressed_bytes = upload_entries_uncompressed_bytes(&entries)?;
    if let Some(progress) = &options.progress {
        progress.emit(UploadProgress::Planned {
            total_files: entries.len(),
            total_bytes: uncompressed_bytes,
        });
    }

    let zip_bytes = write_upload_zip_to_local_file(
        &options.destination_zip,
        &entries,
        options.include_catalog,
        options.progress.clone(),
        None,
    )
    .await?;

    if let Some(progress) = &options.progress {
        progress.emit(UploadProgress::Finished {
            total_files: entries.len(),
            total_bytes: uncompressed_bytes,
        });
    }

    Ok(LocalZipReport {
        source: options.source_dir.display().to_string(),
        destination_zip: options.destination_zip.display().to_string(),
        files,
        directories,
        entries: entries.len(),
        uncompressed_bytes,
        zip_bytes,
    })
}

/// Zips an S3 prefix and uploads the archive to S3.
///
/// The source objects are listed with `ListObjectsV2` and streamed into the ZIP
/// with `GetObject`; no local archive or source object body is written to local
/// storage. Zero-byte source keys ending in `/` are preserved as ZIP directory
/// entries, while regular zero-byte objects are preserved as file entries.
pub async fn zip_s3_prefix_to_s3(
    client: &Client,
    options: S3PrefixUploadOptions,
) -> Result<S3PrefixUploadReport> {
    validate_s3_prefix_upload_options(&options)?;

    let entries = collect_s3_prefix_upload_entries(client, &options).await?;
    let files = entries.iter().filter(|entry| entry.is_file()).count();
    let directories = entries.len().saturating_sub(files);
    let uncompressed_bytes = upload_entries_uncompressed_bytes(&entries)?;
    if let Some(progress) = &options.progress {
        progress.emit(UploadProgress::Planned {
            total_files: entries.len(),
            total_bytes: uncompressed_bytes,
        });
    }

    let upload_id = create_upload(client, &options.destination).await?;
    let (writer, reader) = tokio::io::duplex(options.pipe_capacity);
    let include_catalog = options.include_catalog;
    let progress = options.progress.clone();
    let finish_progress = progress.clone();
    let total_entries = entries.len();
    let total_bytes = uncompressed_bytes;
    let producer_client = client.clone();
    let producer = tokio::spawn(async move {
        write_upload_zip_with_s3_client(
            writer.compat_write(),
            &entries,
            include_catalog,
            progress,
            &producer_client,
        )
        .await
        .inspect(|_| {
            if let Some(progress) = &finish_progress {
                progress.emit(UploadProgress::Finished {
                    total_files: total_entries,
                    total_bytes,
                });
            }
        })
    });

    let upload_result = upload_parts(
        client,
        &options.destination,
        &upload_id,
        reader,
        options.body_chunk_size,
    )
    .await;
    let (parts, zip_bytes) = match upload_result {
        Ok(result) => result,
        Err(err) => {
            abort_producer(producer).await;
            return Err(
                abort_upload_after_error(client, &options.destination, &upload_id, err).await,
            );
        }
    };

    match producer.await {
        Ok(Ok(_catalog)) => {}
        Ok(Err(err)) => {
            return Err(
                abort_upload_after_error(client, &options.destination, &upload_id, err).await,
            );
        }
        Err(err) => {
            return Err(abort_upload_after_error(
                client,
                &options.destination,
                &upload_id,
                err.into(),
            )
            .await);
        }
    }

    if let Err(err) = validate_multipart_upload_has_data(&parts, zip_bytes) {
        return Err(abort_upload_after_error(client, &options.destination, &upload_id, err).await);
    }

    if let Err(err) = complete_upload(client, &options.destination, &upload_id, parts).await {
        return Err(abort_upload_after_error(client, &options.destination, &upload_id, err).await);
    }

    Ok(S3PrefixUploadReport {
        source: options.source,
        destination: options.destination,
        files,
        directories,
        entries: total_entries,
        uncompressed_bytes,
        zip_bytes,
    })
}

/// Zips an S3 prefix into a local ZIP file.
///
/// Source objects are listed and streamed from S3 with `GetObject`. Zero-byte
/// trailing-slash marker objects are preserved as ZIP directory entries.
pub async fn zip_s3_prefix_to_file(
    client: &Client,
    options: S3PrefixLocalZipOptions,
) -> Result<LocalZipReport> {
    validate_local_zip_destination_path(&options.destination_zip).await?;

    let entries = collect_s3_prefix_upload_entries_with_size_limit(
        client,
        &S3PrefixUploadOptions {
            source: options.source.clone(),
            destination: S3Object {
                bucket: "__local__".to_string(),
                key: options.destination_zip.display().to_string(),
            },
            include_catalog: options.include_catalog,
            body_chunk_size: MAX_BODY_CHUNK_SIZE.min(64 * 1024),
            pipe_capacity: 64 * 1024,
            progress: options.progress.clone(),
        },
        None,
    )
    .await?;
    let files = entries.iter().filter(|entry| entry.is_file()).count();
    let directories = entries.len().saturating_sub(files);
    let uncompressed_bytes = upload_entries_uncompressed_bytes(&entries)?;
    if let Some(progress) = &options.progress {
        progress.emit(UploadProgress::Planned {
            total_files: entries.len(),
            total_bytes: uncompressed_bytes,
        });
    }

    let zip_bytes = write_upload_zip_to_local_file(
        &options.destination_zip,
        &entries,
        options.include_catalog,
        options.progress.clone(),
        Some(client),
    )
    .await?;

    if let Some(progress) = &options.progress {
        progress.emit(UploadProgress::Finished {
            total_files: entries.len(),
            total_bytes: uncompressed_bytes,
        });
    }

    Ok(LocalZipReport {
        source: options.source.uri(),
        destination_zip: options.destination_zip.display().to_string(),
        files,
        directories,
        entries: entries.len(),
        uncompressed_bytes,
        zip_bytes,
    })
}

fn upload_entries_uncompressed_bytes(entries: &[UploadEntry]) -> Result<u64> {
    entries
        .iter()
        .filter(|entry| entry.is_file())
        .map(|entry| entry.size)
        .try_fold(0_u64, |total, size| {
            total.checked_add(size).ok_or_else(|| {
                Error::InvalidOption("total upload size exceeds u64::MAX".to_string())
            })
        })
}

async fn write_upload_zip_to_local_file(
    destination: &Path,
    entries: &[UploadEntry],
    include_catalog: bool,
    progress: Option<UploadProgressHandler>,
    s3_client: Option<&Client>,
) -> Result<u64> {
    let temp_path = temp_sibling_path(destination)?;
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await
        .map_err(|err| invalid_local_path(&temp_path, format!("cannot create ZIP file: {err}")))?;

    let write_result = write_upload_zip_entries(
        file.compat_write(),
        entries,
        include_catalog,
        progress,
        s3_client,
    )
    .await;
    if let Err(err) = write_result {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(err);
    }

    let zip_bytes = tokio::fs::metadata(&temp_path)
        .await
        .map_err(|err| {
            invalid_local_path(&temp_path, format!("cannot read ZIP file metadata: {err}"))
        })?
        .len();
    if let Err(err) = replace_temp_file(&temp_path, destination, "completed ZIP").await {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(err);
    }
    Ok(zip_bytes)
}

async fn create_upload(client: &Client, destination: &S3Object) -> Result<String> {
    let output = client
        .create_multipart_upload()
        .bucket(&destination.bucket)
        .key(&destination.key)
        .send()
        .await
        .map_err(|err| Error::S3 {
            operation: "CreateMultipartUpload",
            bucket: destination.bucket.clone(),
            key: destination.key.clone(),
            message: aws_error_message(&err),
        })?;

    output.upload_id().map(ToOwned::to_owned).ok_or_else(|| {
        Error::Build("CreateMultipartUpload response did not include an upload id".to_string())
    })
}

async fn upload_parts(
    client: &Client,
    destination: &S3Object,
    upload_id: &str,
    mut reader: DuplexStream,
    body_chunk_size: usize,
) -> Result<(Vec<CompletedPart>, u64)> {
    let mut read_buffer = vec![0_u8; body_chunk_size];
    let mut part_buffer = BytesMut::with_capacity(UPLOAD_MULTIPART_PART_SIZE + body_chunk_size);
    let mut parts = Vec::new();
    let mut part_number = 1_i32;
    let mut zip_bytes = 0_u64;

    loop {
        let read = reader.read(&mut read_buffer).await?;
        if read == 0 {
            break;
        }

        zip_bytes = zip_bytes.checked_add(read as u64).ok_or_else(|| {
            Error::Build("zip byte count overflowed during multipart upload".to_string())
        })?;
        part_buffer.extend_from_slice(&read_buffer[..read]);

        while let Some((current_part_number, bytes)) =
            take_ready_multipart_part(&mut part_buffer, &mut part_number)?
        {
            let part =
                upload_part(client, destination, upload_id, current_part_number, bytes).await?;
            parts.push(part);
        }
    }

    if let Some((current_part_number, bytes)) = take_final_multipart_part(part_buffer, part_number)?
    {
        let part = upload_part(client, destination, upload_id, current_part_number, bytes).await?;
        parts.push(part);
    }

    Ok((parts, zip_bytes))
}

fn validate_multipart_upload_has_data(parts: &[CompletedPart], zip_bytes: u64) -> Result<()> {
    if parts.is_empty() || zip_bytes == 0 {
        Err(Error::Build(
            "multipart upload produced no ZIP data".to_string(),
        ))
    } else {
        Ok(())
    }
}

async fn upload_part(
    client: &Client,
    destination: &S3Object,
    upload_id: &str,
    part_number: i32,
    bytes: bytes::Bytes,
) -> Result<CompletedPart> {
    let content_length = i64::try_from(bytes.len())
        .map_err(|_| Error::Build("multipart part size does not fit i64".to_string()))?;
    let output = client
        .upload_part()
        .bucket(&destination.bucket)
        .key(&destination.key)
        .upload_id(upload_id)
        .part_number(part_number)
        .content_length(content_length)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(|err| Error::S3 {
            operation: "UploadPart",
            bucket: destination.bucket.clone(),
            key: destination.key.clone(),
            message: aws_error_message(&err),
        })?;

    let etag = output.e_tag().map(ToOwned::to_owned).ok_or_else(|| {
        Error::Build(format!(
            "UploadPart response for part {part_number} did not include an ETag"
        ))
    })?;

    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(etag)
        .build())
}

fn validate_part_number(part_number: i32) -> Result<()> {
    if (1..=S3_MULTIPART_MAX_PARTS).contains(&part_number) {
        return Ok(());
    }

    Err(Error::InvalidOption(format!(
        "multipart upload exceeded the S3 limit of {S3_MULTIPART_MAX_PARTS} parts; with the fixed {UPLOAD_MULTIPART_PART_SIZE} byte part size this supports ZIP archives up to {UPLOAD_MULTIPART_MAX_ZIP_BYTES} bytes"
    )))
}

fn take_ready_multipart_part(
    part_buffer: &mut BytesMut,
    part_number: &mut i32,
) -> Result<Option<(i32, bytes::Bytes)>> {
    if part_buffer.len() < UPLOAD_MULTIPART_PART_SIZE {
        return Ok(None);
    }

    validate_part_number(*part_number)?;
    let current_part_number = *part_number;
    let bytes = part_buffer.split_to(UPLOAD_MULTIPART_PART_SIZE).freeze();
    *part_number = next_part_number(*part_number)?;
    Ok(Some((current_part_number, bytes)))
}

fn take_final_multipart_part(
    part_buffer: BytesMut,
    part_number: i32,
) -> Result<Option<(i32, bytes::Bytes)>> {
    if part_buffer.is_empty() {
        return Ok(None);
    }

    validate_part_number(part_number)?;
    Ok(Some((part_number, part_buffer.freeze())))
}

fn next_part_number(part_number: i32) -> Result<i32> {
    part_number
        .checked_add(1)
        .ok_or_else(|| Error::InvalidOption("multipart part number overflow".to_string()))
}

async fn complete_upload(
    client: &Client,
    destination: &S3Object,
    upload_id: &str,
    mut parts: Vec<CompletedPart>,
) -> Result<()> {
    parts.sort_by_key(|part| part.part_number());
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(&destination.bucket)
        .key(&destination.key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await
        .map(|_| ())
        .map_err(|err| Error::S3 {
            operation: "CompleteMultipartUpload",
            bucket: destination.bucket.clone(),
            key: destination.key.clone(),
            message: aws_error_message(&err),
        })
}

async fn abort_upload(client: &Client, destination: &S3Object, upload_id: &str) -> Result<()> {
    client
        .abort_multipart_upload()
        .bucket(&destination.bucket)
        .key(&destination.key)
        .upload_id(upload_id)
        .send()
        .await
        .map(|_| ())
        .map_err(|err| Error::S3 {
            operation: "AbortMultipartUpload",
            bucket: destination.bucket.clone(),
            key: destination.key.clone(),
            message: aws_error_message(&err),
        })
}

async fn abort_upload_after_error(
    client: &Client,
    destination: &S3Object,
    upload_id: &str,
    original: Error,
) -> Error {
    match abort_upload(client, destination, upload_id).await {
        Ok(()) => original,
        Err(abort) => attach_abort_error(original, abort),
    }
}

fn attach_abort_error(original: Error, abort: Error) -> Error {
    Error::MultipartAbort {
        original: Box::new(original),
        abort: Box::new(abort),
    }
}

async fn abort_producer(producer: tokio::task::JoinHandle<Result<Vec<EmbeddedCatalogEntry>>>) {
    if !producer.is_finished() {
        producer.abort();
    }
    let _ = producer.await;
}

fn validate_upload_options(options: &UploadOptions) -> Result<()> {
    validate_upload_stream_options(options.body_chunk_size, options.pipe_capacity)
}

fn validate_s3_prefix_upload_options(options: &S3PrefixUploadOptions) -> Result<()> {
    validate_upload_stream_options(options.body_chunk_size, options.pipe_capacity)?;

    if options.source.bucket == options.destination.bucket {
        let list_prefix = normalized_list_prefix(&options.source.prefix);
        if list_prefix.is_empty() || options.destination.key.starts_with(&list_prefix) {
            return Err(Error::InvalidOption(format!(
                "destination {} is inside source prefix {}",
                options.destination.uri(),
                options.source.uri()
            )));
        }
    }

    Ok(())
}

async fn validate_local_zip_options(options: &LocalZipOptions) -> Result<()> {
    reject_local_zip_destination_inside_source(&options.source_dir, &options.destination_zip)
        .await?;
    validate_local_zip_destination_path(&options.destination_zip).await
}

async fn reject_local_zip_destination_inside_source(
    source_dir: &Path,
    destination_zip: &Path,
) -> Result<()> {
    let source = tokio::fs::canonicalize(source_dir).await.map_err(|err| {
        invalid_local_path(source_dir, format!("cannot canonicalize directory: {err}"))
    })?;
    let destination_parent = destination_zip
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let destination_parent = tokio::fs::canonicalize(destination_parent)
        .await
        .map_err(|err| {
            invalid_local_path(
                destination_parent,
                format!("cannot canonicalize destination directory: {err}"),
            )
        })?;

    if destination_parent == source || destination_parent.starts_with(&source) {
        return Err(invalid_local_path(
            destination_zip,
            format!(
                "destination ZIP must not be inside source directory {}",
                source.display()
            ),
        ));
    }

    Ok(())
}

async fn validate_local_zip_destination_path(destination_zip: &Path) -> Result<()> {
    reject_local_zip_destination_parent_symlink_chain(destination_zip).await?;
    reject_existing_local_zip_destination_symlink_or_directory(destination_zip).await
}

async fn reject_local_zip_destination_parent_symlink_chain(destination_zip: &Path) -> Result<()> {
    let parent = destination_zip
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut current = parent;

    loop {
        if current.as_os_str().is_empty() {
            break;
        }

        match tokio::fs::symlink_metadata(current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_local_path(
                    current,
                    "destination path component cannot be a symbolic link".to_string(),
                ));
            }
            Ok(metadata) if metadata.is_dir() => {
                if current == parent {
                    return Ok(());
                }
                return Err(invalid_local_path(
                    parent,
                    "destination directory does not exist".to_string(),
                ));
            }
            Ok(_) => {
                return Err(invalid_local_path(
                    current,
                    "destination path component exists and is not a directory".to_string(),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(invalid_local_path(
                    current,
                    format!("cannot inspect destination path component: {err}"),
                ));
            }
        }

        let Some(parent) = current.parent() else {
            break;
        };
        current = parent;
    }

    Err(invalid_local_path(
        parent,
        "destination directory does not exist".to_string(),
    ))
}

async fn reject_existing_local_zip_destination_symlink_or_directory(
    destination_zip: &Path,
) -> Result<()> {
    match tokio::fs::symlink_metadata(destination_zip).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_local_path(
            destination_zip,
            "destination ZIP cannot be a symbolic link".to_string(),
        )),
        Ok(metadata) if metadata.is_dir() => Err(invalid_local_path(
            destination_zip,
            "destination ZIP path is a directory".to_string(),
        )),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(invalid_local_path(
            destination_zip,
            format!("cannot inspect destination ZIP: {err}"),
        )),
    }
}

fn validate_upload_stream_options(body_chunk_size: usize, pipe_capacity: usize) -> Result<()> {
    if body_chunk_size == 0 {
        return Err(Error::InvalidOption(
            "body_chunk_size must be greater than zero".to_string(),
        ));
    }
    if body_chunk_size > MAX_BODY_CHUNK_SIZE {
        return Err(Error::InvalidOption(format!(
            "body_chunk_size must be less than or equal to {MAX_BODY_CHUNK_SIZE}"
        )));
    }
    if pipe_capacity == 0 {
        return Err(Error::InvalidOption(
            "pipe_capacity must be greater than zero".to_string(),
        ));
    }
    if pipe_capacity > MAX_PIPE_CAPACITY {
        return Err(Error::InvalidOption(format!(
            "pipe_capacity must be less than or equal to {MAX_PIPE_CAPACITY}"
        )));
    }
    Ok(())
}

pub(crate) async fn collect_upload_entries(source_dir: &Path) -> Result<Vec<UploadEntry>> {
    collect_upload_entries_with_size_limit(source_dir, Some(S3_SINGLE_PUT_LIMIT)).await
}

async fn collect_upload_entries_with_size_limit(
    source_dir: &Path,
    max_file_size: Option<u64>,
) -> Result<Vec<UploadEntry>> {
    let metadata = tokio::fs::symlink_metadata(source_dir)
        .await
        .map_err(|err| {
            invalid_local_path(source_dir, format!("cannot read directory metadata: {err}"))
        })?;
    if metadata.file_type().is_symlink() {
        return Err(invalid_local_path(
            source_dir,
            "symbolic links are not supported".to_string(),
        ));
    }
    if !metadata.is_dir() {
        return Err(invalid_local_path(
            source_dir,
            "source must be a directory".to_string(),
        ));
    }

    let source_dir = tokio::fs::canonicalize(source_dir).await.map_err(|err| {
        invalid_local_path(source_dir, format!("cannot canonicalize directory: {err}"))
    })?;
    let metadata = tokio::fs::metadata(&source_dir).await.map_err(|err| {
        invalid_local_path(
            &source_dir,
            format!("cannot read directory metadata: {err}"),
        )
    })?;

    if !metadata.is_dir() {
        return Err(invalid_local_path(
            &source_dir,
            "source must be a directory".to_string(),
        ));
    }

    let mut dirs = vec![source_dir.clone()];
    let mut files = Vec::new();

    while let Some(dir) = dirs.pop() {
        let mut read_dir = tokio::fs::read_dir(&dir)
            .await
            .map_err(|err| invalid_local_path(&dir, format!("cannot read directory: {err}")))?;
        let mut has_child = false;

        while let Some(entry) = read_dir.next_entry().await.map_err(|err| {
            invalid_local_path(&dir, format!("cannot read directory entry: {err}"))
        })? {
            has_child = true;
            let path = entry.path();
            let file_type = entry.file_type().await.map_err(|err| {
                invalid_local_path(&path, format!("cannot read file type: {err}"))
            })?;

            if file_type.is_symlink() {
                return Err(invalid_local_path(
                    &path,
                    "symbolic links are not supported".to_string(),
                ));
            }

            if file_type.is_dir() {
                dirs.push(path);
                continue;
            }

            if !file_type.is_file() {
                return Err(invalid_local_path(
                    &path,
                    "only regular files are supported".to_string(),
                ));
            }

            let metadata = entry.metadata().await.map_err(|err| {
                invalid_local_path(&path, format!("cannot read file metadata: {err}"))
            })?;
            if let Some(max_file_size) = max_file_size
                && metadata.len() > max_file_size
            {
                return Err(invalid_local_path(
                    &path,
                    format!(
                        "file is {} bytes, larger than the S3 single PutObject limit",
                        metadata.len()
                    ),
                ));
            }

            let zip_path = upload_zip_path(&source_dir, &path)?;
            if zip_path == EMBEDDED_CATALOG_PATH {
                return Err(invalid_local_path(
                    &path,
                    format!("{EMBEDDED_CATALOG_PATH} is reserved for the embedded catalog"),
                ));
            }
            files.push(UploadEntry {
                zip_path,
                size: metadata.len(),
                source: UploadEntrySource::LocalFile(path),
            });
        }

        if !has_child && dir != source_dir {
            let zip_path = format!("{}/", upload_zip_path(&source_dir, &dir)?);
            files.push(UploadEntry {
                zip_path,
                size: 0,
                source: UploadEntrySource::Directory,
            });
        }
    }

    files.sort_unstable_by(|left, right| left.zip_path.cmp(&right.zip_path));
    Ok(files)
}

pub(crate) async fn collect_s3_prefix_upload_entries(
    client: &Client,
    options: &S3PrefixUploadOptions,
) -> Result<Vec<UploadEntry>> {
    collect_s3_prefix_upload_entries_with_size_limit(client, options, Some(S3_SINGLE_PUT_LIMIT))
        .await
}

async fn collect_s3_prefix_upload_entries_with_size_limit(
    client: &Client,
    options: &S3PrefixUploadOptions,
    max_file_size: Option<u64>,
) -> Result<Vec<UploadEntry>> {
    let mut entries = Vec::new();
    let mut seen_zip_paths = HashSet::new();
    let mut continuation = None::<String>;
    let list_prefix = normalized_list_prefix(&options.source.prefix);

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(&options.source.bucket)
            .prefix(&list_prefix);

        if let Some(token) = continuation.take() {
            request = request.continuation_token(token);
        }

        let output = request.send().await.map_err(|err| Error::S3 {
            operation: "ListObjectsV2",
            bucket: options.source.bucket.clone(),
            key: list_prefix.clone(),
            message: aws_error_message(&err),
        })?;

        for object in output.contents() {
            let key = object.key().ok_or_else(|| Error::S3 {
                operation: "ListObjectsV2",
                bucket: options.source.bucket.clone(),
                key: list_prefix.clone(),
                message: "listed object did not include a key".to_string(),
            })?;
            let size = object.size().ok_or_else(|| Error::S3 {
                operation: "ListObjectsV2",
                bucket: options.source.bucket.clone(),
                key: key.to_string(),
                message: "listed object did not include a size".to_string(),
            })?;
            let size = u64::try_from(size).map_err(|_| Error::S3 {
                operation: "ListObjectsV2",
                bucket: options.source.bucket.clone(),
                key: key.to_string(),
                message: format!("listed object had negative size {size}"),
            })?;

            if let Some(entry) = s3_prefix_upload_entry(
                &options.source,
                key,
                size,
                object.e_tag(),
                &mut seen_zip_paths,
                max_file_size,
            )? {
                entries.push(entry);
            }
        }

        if output.is_truncated().unwrap_or(false) {
            continuation = output.next_continuation_token().map(str::to_string);
            if continuation.is_none() {
                return Err(Error::S3 {
                    operation: "ListObjectsV2",
                    bucket: options.source.bucket.clone(),
                    key: list_prefix.clone(),
                    message: "response was truncated without a continuation token".to_string(),
                });
            }
        } else {
            break;
        }
    }

    entries.sort_unstable_by(|left, right| left.zip_path.cmp(&right.zip_path));
    Ok(entries)
}

fn s3_prefix_upload_entry(
    source: &S3Prefix,
    key: &str,
    size: u64,
    etag: Option<&str>,
    seen_zip_paths: &mut HashSet<String>,
    max_file_size: Option<u64>,
) -> Result<Option<UploadEntry>> {
    let Some((zip_path, kind)) = s3_prefix_zip_path(source, key, size)? else {
        return Ok(None);
    };

    record_upload_zip_path(seen_zip_paths, &zip_path)?;
    if kind == UploadEntryKind::File && zip_path == EMBEDDED_CATALOG_PATH {
        return Err(Error::InvalidZipEntry {
            path: zip_path,
            reason: format!("{EMBEDDED_CATALOG_PATH} is reserved for the embedded catalog"),
        });
    }
    if kind == UploadEntryKind::File
        && let Some(max_file_size) = max_file_size
        && size > max_file_size
    {
        return Err(Error::EntryTooLarge {
            path: zip_path,
            size,
        });
    }

    Ok(Some(UploadEntry {
        zip_path,
        size,
        source: match kind {
            UploadEntryKind::File => UploadEntrySource::S3Object {
                bucket: source.bucket.clone(),
                key: key.to_string(),
                etag: etag.map(str::to_string),
            },
            UploadEntryKind::Directory => UploadEntrySource::Directory,
        },
    }))
}

fn record_upload_zip_path(seen: &mut HashSet<String>, zip_path: &str) -> Result<()> {
    if seen.insert(zip_path.to_string()) {
        Ok(())
    } else {
        Err(Error::DuplicateZipPath(zip_path.to_string()))
    }
}

pub(crate) fn s3_prefix_zip_path(
    source: &S3Prefix,
    key: &str,
    size: u64,
) -> Result<Option<(String, UploadEntryKind)>> {
    let list_prefix = normalized_list_prefix(&source.prefix);
    let relative = if list_prefix.is_empty() {
        key
    } else {
        key.strip_prefix(&list_prefix)
            .ok_or_else(|| Error::InvalidZipEntry {
                path: key.to_string(),
                reason: format!("key is outside source prefix {}", source.uri()),
            })?
    };

    if relative.is_empty() {
        if size == 0 {
            return Ok(None);
        }
        return Err(Error::InvalidZipEntry {
            path: key.to_string(),
            reason: "source prefix root object cannot be represented as a ZIP entry".to_string(),
        });
    }

    let zip_path = normalize_zip_entry_path(relative)?;
    if zip_path.is_directory {
        if size == 0 {
            Ok(Some((zip_path.path, UploadEntryKind::Directory)))
        } else {
            Err(Error::InvalidZipEntry {
                path: relative.to_string(),
                reason: "trailing-slash S3 objects must be zero-byte directory markers".to_string(),
            })
        }
    } else {
        Ok(Some((zip_path.path, UploadEntryKind::File)))
    }
}

pub(crate) async fn write_upload_zip<W>(
    writer: W,
    entries: &[UploadEntry],
    include_catalog: bool,
    progress: Option<UploadProgressHandler>,
) -> Result<Vec<EmbeddedCatalogEntry>>
where
    W: AsyncWrite + Unpin,
{
    write_upload_zip_entries(writer, entries, include_catalog, progress, None).await
}

async fn write_upload_zip_with_s3_client<W>(
    writer: W,
    entries: &[UploadEntry],
    include_catalog: bool,
    progress: Option<UploadProgressHandler>,
    client: &Client,
) -> Result<Vec<EmbeddedCatalogEntry>>
where
    W: AsyncWrite + Unpin,
{
    write_upload_zip_entries(writer, entries, include_catalog, progress, Some(client)).await
}

async fn write_upload_zip_entries<W>(
    writer: W,
    entries: &[UploadEntry],
    include_catalog: bool,
    progress: Option<UploadProgressHandler>,
    s3_client: Option<&Client>,
) -> Result<Vec<EmbeddedCatalogEntry>>
where
    W: AsyncWrite + Unpin,
{
    let mut zip_writer = ZipFileWriter::new(writer);
    let mut catalog_entries = Vec::with_capacity(entries.len());
    let total_files = entries.len();
    let total_bytes = upload_entries_uncompressed_bytes(entries)?;
    let mut processed_bytes = 0_u64;

    for (index, entry) in entries.iter().enumerate() {
        let current_file = index + 1;
        if let Some(progress) = &progress {
            progress.emit(UploadProgress::FileStarted {
                current_file,
                total_files,
                processed_files: index,
                processed_bytes,
                total_bytes,
                path: entry.zip_path.clone(),
            });
        }

        let (file_bytes, digest) = write_zip_entry(
            &mut zip_writer,
            entry,
            include_catalog,
            &progress,
            UploadProgressContext {
                current_file,
                total_files,
                processed_files: index,
                processed_bytes,
                total_bytes,
            },
            s3_client,
        )
        .await?;
        let processed_files = current_file;
        processed_bytes = processed_bytes.saturating_add(file_bytes);
        if let Some(progress) = &progress {
            progress.emit(UploadProgress::FileFinished {
                processed_files,
                total_files,
                processed_bytes,
                total_bytes,
                path: entry.zip_path.clone(),
            });
        }
        if let Some(digest) = digest {
            catalog_entries.push(EmbeddedCatalogEntry {
                path: entry.zip_path.clone(),
                md5: digest,
            });
        }
    }

    if include_catalog {
        let catalog = EmbeddedCatalogRef {
            version: EMBEDDED_CATALOG_VERSION,
            entries: &catalog_entries,
        };
        let catalog = serde_json::to_vec(&catalog)
            .map_err(|err| Error::Build(format!("cannot serialize embedded catalog: {err}")))?;
        let builder = ZipEntryBuilder::new(
            EMBEDDED_CATALOG_PATH.to_string().into(),
            Compression::Deflate,
        );
        zip_writer.write_entry_whole(builder, &catalog).await?;
    }

    zip_writer.close().await?;
    Ok(catalog_entries)
}

#[derive(Clone, Copy)]
struct UploadProgressContext {
    current_file: usize,
    total_files: usize,
    processed_files: usize,
    processed_bytes: u64,
    total_bytes: u64,
}

async fn write_zip_entry<W>(
    zip_writer: &mut ZipFileWriter<W>,
    entry: &UploadEntry,
    hash_entry: bool,
    progress: &Option<UploadProgressHandler>,
    progress_context: UploadProgressContext,
    s3_client: Option<&Client>,
) -> Result<(u64, Option<String>)>
where
    W: AsyncWrite + Unpin,
{
    if entry.is_directory() {
        let builder = ZipEntryBuilder::new(entry.zip_path.clone().into(), Compression::Stored);
        zip_writer.write_entry_whole(builder, &[]).await?;
        return Ok((0, None));
    }

    let builder = ZipEntryBuilder::new(entry.zip_path.clone().into(), Compression::Deflate);
    let mut entry_writer = zip_writer.write_entry_stream(builder).await?;
    let mut state = UploadEntryWriteState {
        hasher: hash_entry.then(Md5::new),
        file_bytes: 0,
        next_progress_bytes: progress_context
            .processed_bytes
            .saturating_add(UPLOAD_PROGRESS_BYTE_GRANULARITY),
    };

    match &entry.source {
        UploadEntrySource::LocalFile(path) => {
            let mut buffer = vec![0_u8; 64 * 1024];
            let mut file = tokio::fs::File::open(path)
                .await
                .map_err(|err| invalid_local_path(path, format!("cannot open file: {err}")))?;

            loop {
                let read = TokioAsyncReadExt::read(&mut file, &mut buffer)
                    .await
                    .map_err(|err| invalid_local_path(path, format!("cannot read file: {err}")))?;
                if read == 0 {
                    break;
                }
                write_upload_chunk(
                    &mut entry_writer,
                    &buffer[..read],
                    &mut state,
                    entry,
                    progress,
                    progress_context,
                )
                .await?;
            }
        }
        UploadEntrySource::S3Object { bucket, key, etag } => {
            let client = s3_client.ok_or_else(|| {
                Error::Build("S3 upload entries require an S3 client".to_string())
            })?;
            let mut request = client.get_object().bucket(bucket).key(key);
            if let Some(etag) = etag {
                request = request.if_match(etag);
            }
            let output = request.send().await.map_err(|err| Error::S3 {
                operation: "GetObject",
                bucket: bucket.clone(),
                key: key.clone(),
                message: aws_error_message(&err),
            })?;

            let mut body = output.body;
            while let Some(bytes) = body.try_next().await.map_err(|err| Error::S3 {
                operation: "GetObject",
                bucket: bucket.clone(),
                key: key.clone(),
                message: format!("S3 body read failed: {err}"),
            })? {
                write_upload_chunk(
                    &mut entry_writer,
                    &bytes,
                    &mut state,
                    entry,
                    progress,
                    progress_context,
                )
                .await?;
            }

            if state.file_bytes != entry.size {
                return Err(Error::S3 {
                    operation: "GetObject",
                    bucket: bucket.clone(),
                    key: key.clone(),
                    message: format!(
                        "object body returned {} bytes but listing declared {} bytes",
                        state.file_bytes, entry.size
                    ),
                });
            }
        }
        UploadEntrySource::Directory => {
            return Err(Error::Build(format!(
                "directory entry {} reached file writer",
                entry.zip_path
            )));
        }
    }

    entry_writer.close().await?;

    Ok((
        state.file_bytes,
        state.hasher.map(|hasher| hex::encode(hasher.finalize())),
    ))
}

struct UploadEntryWriteState {
    hasher: Option<Md5>,
    file_bytes: u64,
    next_progress_bytes: u64,
}

async fn write_upload_chunk<W>(
    entry_writer: &mut W,
    bytes: &[u8],
    state: &mut UploadEntryWriteState,
    entry: &UploadEntry,
    progress: &Option<UploadProgressHandler>,
    progress_context: UploadProgressContext,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if let Some(hasher) = &mut state.hasher {
        hasher.update(bytes);
    }
    FuturesAsyncWriteExt::write_all(entry_writer, bytes).await?;
    state.file_bytes = state.file_bytes.saturating_add(bytes.len() as u64);
    let current_processed_bytes = progress_context
        .processed_bytes
        .saturating_add(state.file_bytes);
    if current_processed_bytes >= state.next_progress_bytes {
        if let Some(progress) = progress {
            progress.emit(UploadProgress::FileProgress {
                current_file: progress_context.current_file,
                total_files: progress_context.total_files,
                processed_files: progress_context.processed_files,
                processed_bytes: current_processed_bytes,
                total_bytes: progress_context.total_bytes,
                path: entry.zip_path.clone(),
            });
        }
        while state.next_progress_bytes <= current_processed_bytes {
            state.next_progress_bytes = state
                .next_progress_bytes
                .saturating_add(UPLOAD_PROGRESS_BYTE_GRANULARITY);
        }
    }
    Ok(())
}

pub(crate) fn upload_zip_path(source_dir: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(source_dir).map_err(|err| {
        invalid_local_path(path, format!("path is outside source directory: {err}"))
    })?;
    let mut parts = Vec::new();

    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    invalid_local_path(path, "path is not valid UTF-8".to_string())
                })?;
                if part.contains('\\') {
                    return Err(invalid_local_path(
                        path,
                        "path components cannot contain backslashes".to_string(),
                    ));
                }
                parts.push(part);
            }
            _ => {
                return Err(invalid_local_path(
                    path,
                    "path must be relative and normalized".to_string(),
                ));
            }
        }
    }

    if parts.is_empty() {
        return Err(invalid_local_path(
            path,
            "path does not name a file under the source directory".to_string(),
        ));
    }

    Ok(parts.join("/"))
}

pub(crate) fn invalid_local_path(path: &Path, reason: String) -> Error {
    Error::InvalidLocalPath {
        path: path.display().to_string(),
        reason,
    }
}

pub(crate) fn temp_sibling_path(destination: &Path) -> Result<PathBuf> {
    let file_name = destination.file_name().ok_or_else(|| {
        invalid_local_path(
            destination,
            "destination must include a file name".to_string(),
        )
    })?;
    let mut temp_name = OsString::from(".");
    temp_name.push(file_name);
    temp_name.push(format!(
        ".s3-unspool-{}-{}.tmp",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));

    Ok(destination
        .parent()
        .map(|parent| parent.join(&temp_name))
        .unwrap_or_else(|| PathBuf::from(temp_name)))
}

pub(crate) async fn replace_temp_file(
    temp_path: &Path,
    destination: &Path,
    description: &str,
) -> Result<()> {
    match tokio::fs::rename(temp_path, destination).await {
        Ok(()) => Ok(()),
        Err(err) => {
            replace_temp_file_after_rename_error(temp_path, destination, description, err).await
        }
    }
}

#[cfg(not(windows))]
async fn replace_temp_file_after_rename_error(
    _temp_path: &Path,
    destination: &Path,
    description: &str,
    err: std::io::Error,
) -> Result<()> {
    Err(invalid_local_path(
        destination,
        format!("cannot move {description} into place: {err}"),
    ))
}

#[cfg(windows)]
async fn replace_temp_file_after_rename_error(
    temp_path: &Path,
    destination: &Path,
    description: &str,
    err: std::io::Error,
) -> Result<()> {
    if err.kind() != std::io::ErrorKind::AlreadyExists {
        return Err(invalid_local_path(
            destination,
            format!("cannot move {description} into place: {err}"),
        ));
    }

    match tokio::fs::symlink_metadata(destination).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(invalid_local_path(
                destination,
                format!("{description} destination cannot be a symbolic link"),
            ));
        }
        Ok(metadata) if metadata.is_dir() => {
            return Err(invalid_local_path(
                destination,
                format!("{description} destination is a directory"),
            ));
        }
        Ok(_) => tokio::fs::remove_file(destination).await.map_err(|remove_err| {
            invalid_local_path(
                destination,
                format!("cannot remove existing destination before moving {description}: {remove_err}"),
            )
        })?,
        Err(metadata_err) if metadata_err.kind() == std::io::ErrorKind::NotFound => {}
        Err(metadata_err) => {
            return Err(invalid_local_path(
                destination,
                format!("cannot inspect existing destination before moving {description}: {metadata_err}"),
            ));
        }
    }

    tokio::fs::rename(temp_path, destination)
        .await
        .map_err(|rename_err| {
            invalid_local_path(
                destination,
                format!("cannot move {description} into place after removing existing destination: {rename_err}"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipart_part_assembly_emits_exact_multiple_without_empty_final() {
        let mut buffer = BytesMut::new();
        buffer.resize(UPLOAD_MULTIPART_PART_SIZE * 2, 7);
        let mut part_number = 1;

        let first = take_ready_multipart_part(&mut buffer, &mut part_number)
            .unwrap()
            .unwrap();
        let second = take_ready_multipart_part(&mut buffer, &mut part_number)
            .unwrap()
            .unwrap();

        assert_eq!(first.0, 1);
        assert_eq!(first.1.len(), UPLOAD_MULTIPART_PART_SIZE);
        assert_eq!(second.0, 2);
        assert_eq!(second.1.len(), UPLOAD_MULTIPART_PART_SIZE);
        assert_eq!(part_number, 3);
        assert!(buffer.is_empty());
        assert!(
            take_final_multipart_part(buffer, part_number)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn multipart_part_assembly_flushes_small_final_part() {
        let mut buffer = BytesMut::new();
        buffer.resize(UPLOAD_MULTIPART_PART_SIZE + 3, 9);
        let mut part_number = 1;

        let first = take_ready_multipart_part(&mut buffer, &mut part_number)
            .unwrap()
            .unwrap();
        let final_part = take_final_multipart_part(buffer, part_number)
            .unwrap()
            .unwrap();

        assert_eq!(first.0, 1);
        assert_eq!(first.1.len(), UPLOAD_MULTIPART_PART_SIZE);
        assert_eq!(final_part.0, 2);
        assert_eq!(final_part.1.len(), 3);
    }

    #[test]
    fn multipart_part_number_validation_enforces_s3_limits() {
        assert!(validate_part_number(0).is_err());
        assert!(validate_part_number(S3_MULTIPART_MAX_PARTS).is_ok());
        assert!(validate_part_number(S3_MULTIPART_MAX_PARTS + 1).is_err());

        let mut buffer = BytesMut::new();
        buffer.resize(UPLOAD_MULTIPART_PART_SIZE, 1);
        let mut part_number = S3_MULTIPART_MAX_PARTS + 1;

        assert!(take_ready_multipart_part(&mut buffer, &mut part_number).is_err());
    }

    #[test]
    fn multipart_upload_requires_at_least_one_part_and_byte() {
        assert!(validate_multipart_upload_has_data(&[], 0).is_err());
        assert!(validate_multipart_upload_has_data(&[], 1).is_err());

        let part = CompletedPart::builder()
            .part_number(1)
            .e_tag("etag")
            .build();
        assert!(validate_multipart_upload_has_data(&[part], 1).is_ok());
    }

    #[test]
    fn upload_options_reject_oversized_stream_buffers() {
        let mut options = UploadOptions::new(
            ".",
            crate::s3_uri::S3Object::parse("s3://bucket/archive.zip").unwrap(),
        );
        options.body_chunk_size = MAX_BODY_CHUNK_SIZE + 1;
        assert!(validate_upload_options(&options).is_err());

        options.body_chunk_size = MAX_BODY_CHUNK_SIZE;
        options.pipe_capacity = MAX_PIPE_CAPACITY + 1;
        assert!(validate_upload_options(&options).is_err());
    }

    #[test]
    fn s3_prefix_upload_rejects_destination_inside_source_prefix() {
        let options = S3PrefixUploadOptions::new(
            crate::s3_uri::S3Prefix::parse("s3://bucket/source/").unwrap(),
            crate::s3_uri::S3Object::parse("s3://bucket/source/archive.zip").unwrap(),
        );

        assert!(validate_s3_prefix_upload_options(&options).is_err());
    }

    #[test]
    fn s3_prefix_upload_rejects_duplicate_normalized_paths() {
        let source = crate::s3_uri::S3Prefix::parse("s3://bucket/source/").unwrap();
        let (first, first_kind) = s3_prefix_zip_path(&source, "source/foo/", 0)
            .unwrap()
            .unwrap();
        let (second, second_kind) = s3_prefix_zip_path(&source, "source/foo//", 0)
            .unwrap()
            .unwrap();
        let mut seen = HashSet::new();

        assert_eq!(first, "foo/");
        assert_eq!(second, first);
        assert_eq!(first_kind, UploadEntryKind::Directory);
        assert_eq!(second_kind, UploadEntryKind::Directory);
        record_upload_zip_path(&mut seen, &first).unwrap();

        let err = record_upload_zip_path(&mut seen, &second).unwrap_err();

        assert!(matches!(err, Error::DuplicateZipPath(path) if path == "foo/"));
    }

    #[tokio::test]
    async fn local_zip_entry_collection_allows_files_above_s3_single_put_limit() {
        let root = unique_upload_test_dir("large-local-zip-entry");
        let path = root.join("large.bin");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(S3_SINGLE_PUT_LIMIT + 1).unwrap();
        drop(file);

        let entries = collect_upload_entries_with_size_limit(&root, None)
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].zip_path, "large.bin");
        assert_eq!(entries[0].size, S3_SINGLE_PUT_LIMIT + 1);

        let err = collect_upload_entries(&root).await.unwrap_err();
        assert!(err.to_string().contains("S3 single PutObject limit"));
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[test]
    fn s3_prefix_local_zip_entry_allows_objects_above_s3_single_put_limit() {
        let source = crate::s3_uri::S3Prefix::parse("s3://bucket/source/").unwrap();
        let mut seen = HashSet::new();
        let entry = s3_prefix_upload_entry(
            &source,
            "source/large.bin",
            S3_SINGLE_PUT_LIMIT + 1,
            Some("\"etag\""),
            &mut seen,
            None,
        )
        .unwrap()
        .unwrap();

        assert_eq!(entry.zip_path, "large.bin");
        assert_eq!(entry.size, S3_SINGLE_PUT_LIMIT + 1);
        assert!(matches!(
            entry.source,
            UploadEntrySource::S3Object { ref bucket, ref key, ref etag }
                if bucket == "bucket"
                    && key == "source/large.bin"
                    && etag.as_deref() == Some("\"etag\"")
        ));

        let mut seen = HashSet::new();
        let err = s3_prefix_upload_entry(
            &source,
            "source/large.bin",
            S3_SINGLE_PUT_LIMIT + 1,
            Some("\"etag\""),
            &mut seen,
            Some(S3_SINGLE_PUT_LIMIT),
        )
        .unwrap_err();

        assert!(
            matches!(err, Error::EntryTooLarge { ref path, size } if path == "large.bin" && size == S3_SINGLE_PUT_LIMIT + 1)
        );
    }

    fn unique_upload_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "s3-unspool-upload-{name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
