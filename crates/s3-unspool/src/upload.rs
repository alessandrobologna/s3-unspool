use std::path::{Component, Path, PathBuf};

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
use crate::options::{UploadOptions, UploadProgress, UploadProgressHandler};
use crate::report::UploadReport;

const UPLOAD_PROGRESS_BYTE_GRANULARITY: u64 = 8 * 1024 * 1024;
const UPLOAD_MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
const S3_MULTIPART_MAX_PARTS: i32 = 10_000;
const UPLOAD_MULTIPART_MAX_ZIP_BYTES: u64 =
    UPLOAD_MULTIPART_PART_SIZE as u64 * S3_MULTIPART_MAX_PARTS as u64;

#[derive(Clone, Debug)]
pub(crate) struct UploadEntry {
    pub(crate) path: PathBuf,
    pub(crate) zip_path: String,
    pub(crate) size: u64,
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
    let files = entries.len();
    let uncompressed_bytes =
        entries
            .iter()
            .map(|entry| entry.size)
            .try_fold(0_u64, |total, size| {
                total
                    .checked_add(size)
                    .ok_or_else(|| Error::InvalidOption("upload size overflow".to_string()))
            })?;
    if let Some(progress) = &options.progress {
        progress.emit(UploadProgress::Planned {
            total_files: files,
            total_bytes: uncompressed_bytes,
        });
    }

    let upload_id = create_upload(client, &options).await?;
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
                        total_files: files,
                        total_bytes,
                    });
                }
            })
    });

    let upload_result = upload_parts(
        client,
        &options,
        &upload_id,
        reader,
        options.body_chunk_size,
    )
    .await;
    let (parts, zip_bytes) = match upload_result {
        Ok(result) => result,
        Err(err) => {
            abort_producer(producer).await;
            return Err(abort_upload_after_error(client, &options, &upload_id, err).await);
        }
    };

    match producer.await {
        Ok(Ok(_catalog)) => {}
        Ok(Err(err)) => {
            return Err(abort_upload_after_error(client, &options, &upload_id, err).await);
        }
        Err(err) => {
            return Err(abort_upload_after_error(client, &options, &upload_id, err.into()).await);
        }
    }

    if let Err(err) = validate_multipart_upload_has_data(&parts, zip_bytes) {
        return Err(abort_upload_after_error(client, &options, &upload_id, err).await);
    }

    if let Err(err) = complete_upload(client, &options, &upload_id, parts).await {
        return Err(abort_upload_after_error(client, &options, &upload_id, err).await);
    }

    Ok(UploadReport {
        source_dir: options.source_dir.display().to_string(),
        destination: options.destination,
        files,
        uncompressed_bytes,
        zip_bytes,
    })
}

async fn create_upload(client: &Client, options: &UploadOptions) -> Result<String> {
    let output = client
        .create_multipart_upload()
        .bucket(&options.destination.bucket)
        .key(&options.destination.key)
        .send()
        .await
        .map_err(|err| Error::S3 {
            operation: "CreateMultipartUpload",
            bucket: options.destination.bucket.clone(),
            key: options.destination.key.clone(),
            message: aws_error_message(&err),
        })?;

    output.upload_id().map(ToOwned::to_owned).ok_or_else(|| {
        Error::Build("CreateMultipartUpload response did not include an upload id".to_string())
    })
}

async fn upload_parts(
    client: &Client,
    options: &UploadOptions,
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
            let part = upload_part(client, options, upload_id, current_part_number, bytes).await?;
            parts.push(part);
        }
    }

    if let Some((current_part_number, bytes)) = take_final_multipart_part(part_buffer, part_number)?
    {
        let part = upload_part(client, options, upload_id, current_part_number, bytes).await?;
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
    options: &UploadOptions,
    upload_id: &str,
    part_number: i32,
    bytes: bytes::Bytes,
) -> Result<CompletedPart> {
    let content_length = i64::try_from(bytes.len())
        .map_err(|_| Error::Build("multipart part size does not fit i64".to_string()))?;
    let output = client
        .upload_part()
        .bucket(&options.destination.bucket)
        .key(&options.destination.key)
        .upload_id(upload_id)
        .part_number(part_number)
        .content_length(content_length)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(|err| Error::S3 {
            operation: "UploadPart",
            bucket: options.destination.bucket.clone(),
            key: options.destination.key.clone(),
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
    options: &UploadOptions,
    upload_id: &str,
    mut parts: Vec<CompletedPart>,
) -> Result<()> {
    parts.sort_by_key(|part| part.part_number());
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(&options.destination.bucket)
        .key(&options.destination.key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await
        .map(|_| ())
        .map_err(|err| Error::S3 {
            operation: "CompleteMultipartUpload",
            bucket: options.destination.bucket.clone(),
            key: options.destination.key.clone(),
            message: aws_error_message(&err),
        })
}

async fn abort_upload(client: &Client, options: &UploadOptions, upload_id: &str) -> Result<()> {
    client
        .abort_multipart_upload()
        .bucket(&options.destination.bucket)
        .key(&options.destination.key)
        .upload_id(upload_id)
        .send()
        .await
        .map(|_| ())
        .map_err(|err| Error::S3 {
            operation: "AbortMultipartUpload",
            bucket: options.destination.bucket.clone(),
            key: options.destination.key.clone(),
            message: aws_error_message(&err),
        })
}

async fn abort_upload_after_error(
    client: &Client,
    options: &UploadOptions,
    upload_id: &str,
    original: Error,
) -> Error {
    match abort_upload(client, options, upload_id).await {
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
    if options.body_chunk_size == 0 {
        return Err(Error::InvalidOption(
            "body_chunk_size must be greater than zero".to_string(),
        ));
    }
    if options.body_chunk_size > MAX_BODY_CHUNK_SIZE {
        return Err(Error::InvalidOption(format!(
            "body_chunk_size must be less than or equal to {MAX_BODY_CHUNK_SIZE}"
        )));
    }
    if options.pipe_capacity == 0 {
        return Err(Error::InvalidOption(
            "pipe_capacity must be greater than zero".to_string(),
        ));
    }
    if options.pipe_capacity > MAX_PIPE_CAPACITY {
        return Err(Error::InvalidOption(format!(
            "pipe_capacity must be less than or equal to {MAX_PIPE_CAPACITY}"
        )));
    }
    Ok(())
}

pub(crate) async fn collect_upload_entries(source_dir: &Path) -> Result<Vec<UploadEntry>> {
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

        while let Some(entry) = read_dir.next_entry().await.map_err(|err| {
            invalid_local_path(&dir, format!("cannot read directory entry: {err}"))
        })? {
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
            if metadata.len() > S3_SINGLE_PUT_LIMIT {
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
                path,
                zip_path,
                size: metadata.len(),
            });
        }
    }

    files.sort_unstable_by(|left, right| left.zip_path.cmp(&right.zip_path));
    Ok(files)
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
    let mut zip_writer = ZipFileWriter::new(writer);
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut catalog_entries = Vec::with_capacity(entries.len());
    let total_files = entries.len();
    let total_bytes = entries.iter().try_fold(0_u64, |total, entry| {
        total
            .checked_add(entry.size)
            .ok_or_else(|| Error::Build("total uncompressed upload size exceeds u64::MAX".into()))
    })?;
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

        let builder = ZipEntryBuilder::new(entry.zip_path.clone().into(), Compression::Deflate);
        let mut entry_writer = zip_writer.write_entry_stream(builder).await?;
        let mut file = tokio::fs::File::open(&entry.path)
            .await
            .map_err(|err| invalid_local_path(&entry.path, format!("cannot open file: {err}")))?;
        let mut hasher = include_catalog.then(Md5::new);
        let mut file_bytes = 0_u64;
        let mut next_progress_bytes =
            processed_bytes.saturating_add(UPLOAD_PROGRESS_BYTE_GRANULARITY);

        loop {
            let read = TokioAsyncReadExt::read(&mut file, &mut buffer)
                .await
                .map_err(|err| {
                    invalid_local_path(&entry.path, format!("cannot read file: {err}"))
                })?;
            if read == 0 {
                break;
            }
            if let Some(hasher) = &mut hasher {
                hasher.update(&buffer[..read]);
            }
            FuturesAsyncWriteExt::write_all(&mut entry_writer, &buffer[..read]).await?;
            file_bytes = file_bytes.saturating_add(read as u64);
            let current_processed_bytes = processed_bytes.saturating_add(file_bytes);
            if current_processed_bytes >= next_progress_bytes {
                if let Some(progress) = &progress {
                    progress.emit(UploadProgress::FileProgress {
                        current_file,
                        total_files,
                        processed_files: index,
                        processed_bytes: current_processed_bytes,
                        total_bytes,
                        path: entry.zip_path.clone(),
                    });
                }
                while next_progress_bytes <= current_processed_bytes {
                    next_progress_bytes =
                        next_progress_bytes.saturating_add(UPLOAD_PROGRESS_BYTE_GRANULARITY);
                }
            }
        }

        entry_writer.close().await?;
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
        if let Some(hasher) = hasher {
            catalog_entries.push(EmbeddedCatalogEntry {
                path: entry.zip_path.clone(),
                md5: hex::encode(hasher.finalize()),
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
}
