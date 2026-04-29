use std::sync::Arc;

use async_zip::tokio::read::seek::ZipFileReader;
use aws_sdk_s3::Client;

use crate::constants::DEFAULT_SOURCE_BLOCK_SIZE;
use crate::error::Result;
use crate::range::{S3RangeReader, SourceClient};
use crate::s3_uri::S3Object;
use crate::source::head_source;
use crate::zip_manifest::count_zip_file_entries;

/// Metadata about a source ZIP object stored in S3.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ZipInfo {
    /// Source ZIP object size in bytes.
    pub size: u64,
    /// Number of file entries in the ZIP, excluding directories and the
    /// embedded update catalog.
    pub file_count: usize,
}

/// Inspects an S3 ZIP object without downloading it to local storage.
///
/// The inspection performs a source `HeadObject` and ranged `GetObject` reads
/// needed by the ZIP central directory parser. It is useful when callers need
/// source size or file count before choosing extraction memory settings.
pub async fn inspect_s3_zip(client: &Client, source: &S3Object) -> Result<S3ZipInfo> {
    let head = head_source(client, source).await?;
    let size = head.len;
    let source = Arc::new(SourceClient {
        client: client.clone(),
        bucket: source.bucket.clone(),
        key: source.key.clone(),
        len: head.len,
        etag: head.etag,
        diagnostics: None,
    });
    let reader = S3RangeReader::new(source, DEFAULT_SOURCE_BLOCK_SIZE);
    let reader = ZipFileReader::with_tokio(reader).await?;
    let file_count = count_zip_file_entries(reader.file().entries())?;

    Ok(S3ZipInfo { size, file_count })
}
