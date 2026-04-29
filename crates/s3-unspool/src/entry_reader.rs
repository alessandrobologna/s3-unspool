use std::pin::Pin;
use std::sync::Arc;

use async_compression::tokio::bufread::DeflateDecoder;
use async_zip::Compression;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use crate::error::{Error, Result};
use crate::range::{BlockRangeReader, BlockStore};
use crate::zip_manifest::{ManifestEntry, normalize_zip_file_path};

const LOCAL_FILE_HEADER_SIGNATURE: u32 = 0x0403_4b50;
const LOCAL_FILE_HEADER_LEN: usize = 30;
const LOCAL_FILE_NAME_LEN_OFFSET: usize = 26;
const LOCAL_EXTRA_FIELD_LEN_OFFSET: usize = 28;
const LOCAL_COMPRESSION_OFFSET: usize = 8;

pub(crate) type EntryReader = Pin<Box<dyn AsyncRead + Send>>;

pub(crate) async fn entry_reader(
    store: Arc<BlockStore>,
    entry: &ManifestEntry,
) -> Result<EntryReader> {
    let mut reader = BlockRangeReader::new(store, entry.source_offset, entry.source_span_end)?;
    let local_header_end = entry
        .source_offset
        .checked_add(LOCAL_FILE_HEADER_LEN as u64)
        .ok_or_else(|| Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: "local file header offset overflowed".to_string(),
        })?;
    if local_header_end > entry.source_span_end {
        return Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: "local file header extends beyond the planned source span".to_string(),
        });
    }

    let mut header = [0_u8; LOCAL_FILE_HEADER_LEN];
    reader.read_exact(&mut header).await?;

    let signature = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if signature != LOCAL_FILE_HEADER_SIGNATURE {
        return Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: format!(
                "unexpected local file header signature {signature:#x} at offset {}",
                entry.source_offset
            ),
        });
    }

    let local_compression = u16::from_le_bytes([
        header[LOCAL_COMPRESSION_OFFSET],
        header[LOCAL_COMPRESSION_OFFSET + 1],
    ]);
    let expected_compression = u16::from(entry.compression);
    if local_compression != expected_compression {
        return Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: format!(
                "local compression method {local_compression} does not match central directory method {expected_compression}"
            ),
        });
    }

    let file_name_len = u16::from_le_bytes([
        header[LOCAL_FILE_NAME_LEN_OFFSET],
        header[LOCAL_FILE_NAME_LEN_OFFSET + 1],
    ]) as u64;
    let extra_field_len = u16::from_le_bytes([
        header[LOCAL_EXTRA_FIELD_LEN_OFFSET],
        header[LOCAL_EXTRA_FIELD_LEN_OFFSET + 1],
    ]) as u64;
    let data_offset = entry
        .source_offset
        .checked_add(LOCAL_FILE_HEADER_LEN as u64)
        .and_then(|offset| offset.checked_add(file_name_len))
        .and_then(|offset| offset.checked_add(extra_field_len))
        .ok_or_else(|| Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: "local file data offset overflowed".to_string(),
        })?;
    if data_offset > entry.source_span_end {
        return Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: "local header name or extra field extends beyond the planned source span"
                .to_string(),
        });
    }

    let mut local_file_name = vec![0_u8; file_name_len as usize];
    reader.read_exact(&mut local_file_name).await?;
    let local_file_name =
        std::str::from_utf8(&local_file_name).map_err(|err| Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: format!("local file name is not valid UTF-8: {err}"),
        })?;
    let Some(local_zip_path) = normalize_zip_file_path(local_file_name)? else {
        return Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: "local file header points to a directory entry".to_string(),
        });
    };
    if local_zip_path != entry.zip_path {
        return Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: format!(
                "local file name {local_zip_path:?} does not match central directory name {:?}",
                entry.zip_path
            ),
        });
    }

    let mut local_extra = vec![0_u8; extra_field_len as usize];
    reader.read_exact(&mut local_extra).await?;
    let compressed_data_end = data_offset
        .checked_add(entry.compressed_size)
        .ok_or_else(|| Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: "local file compressed data offset overflowed".to_string(),
        })?;
    if compressed_data_end > entry.source_span_end {
        return Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: "local file data extends beyond the planned source span".to_string(),
        });
    }

    let compressed = reader.take(entry.compressed_size);
    match entry.compression {
        Compression::Stored => Ok(Box::pin(compressed)),
        Compression::Deflate => Ok(Box::pin(DeflateDecoder::new(BufReader::new(compressed)))),
        other => Err(Error::InvalidZipEntry {
            path: entry.zip_path.clone(),
            reason: format!("unsupported compression method {other:?}"),
        }),
    }
}
