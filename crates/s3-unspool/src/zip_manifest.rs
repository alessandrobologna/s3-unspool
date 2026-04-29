use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_zip::base::read::{WithoutEntry, ZipEntryReader};
use async_zip::tokio::read::seek::ZipFileReader;
use async_zip::{Compression, StoredZipEntry, ZipFile};
use futures_lite::io::AsyncReadExt as FuturesAsyncReadExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::catalog::{EmbeddedCatalog, catalog_md5_by_path};
use crate::constants::{EMBEDDED_CATALOG_MAX_BYTES, EMBEDDED_CATALOG_PATH, S3_SINGLE_PUT_LIMIT};
use crate::error::{Error, Result};
use crate::range::{S3RangeReader, SourceClient};
use crate::s3_uri::S3Prefix;

#[derive(Clone, Debug)]
pub(crate) struct ManifestEntry {
    /// Local-file-header offset used to seek directly to this ZIP entry.
    pub(crate) source_offset: u64,
    /// Inclusive start of the source byte span used by the source scheduler.
    pub(crate) source_span_start: u64,
    /// Exclusive end of the source byte span used by the source scheduler.
    pub(crate) source_span_end: u64,
    pub(crate) zip_path: String,
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) compressed_size: u64,
    pub(crate) compression: Compression,
    pub(crate) crc32: u32,
    pub(crate) catalog_md5: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ZipManifest {
    pub(crate) entries: Vec<ManifestEntry>,
}

#[derive(Debug)]
pub(crate) struct ManifestBuild {
    pub(crate) entries: Vec<ManifestEntry>,
    pub(crate) catalog_index: Option<usize>,
}

pub(crate) async fn load_zip_manifest(
    source: Arc<SourceClient>,
    destination: &S3Prefix,
    ignore_embedded_catalog: bool,
    source_block_size: usize,
) -> Result<ZipManifest> {
    let reader = S3RangeReader::new(Arc::clone(&source), source_block_size);
    let reader = ZipFileReader::with_tokio(reader).await?;
    let zip_file = reader.file().clone();
    let build = build_manifest_entries(zip_file.entries(), destination, source.len)?;
    let catalog = if ignore_embedded_catalog {
        HashMap::new()
    } else {
        load_embedded_catalog(
            source,
            zip_file.clone(),
            build.catalog_index,
            source_block_size,
        )
        .await
    };
    let entries = apply_embedded_catalog(build.entries, catalog);
    Ok(ZipManifest { entries })
}

pub(crate) fn build_manifest_entries(
    entries: &[StoredZipEntry],
    destination: &S3Prefix,
    source_len: u64,
) -> Result<ManifestBuild> {
    let mut seen = HashSet::new();
    let mut manifest = Vec::new();
    let mut catalog_index = None;
    let mut source_offsets = entries
        .iter()
        .map(StoredZipEntry::header_offset)
        .collect::<Vec<_>>();
    source_offsets.sort_unstable();

    for (index, stored) in entries.iter().enumerate() {
        let Some(zip_path) = stored_zip_file_path(stored)? else {
            continue;
        };

        if zip_path == EMBEDDED_CATALOG_PATH {
            catalog_index = Some(index);
            continue;
        }

        validate_stored_file_entry(stored, &zip_path)?;
        if !seen.insert(zip_path.clone()) {
            return Err(Error::DuplicateZipPath(zip_path));
        }

        let size = stored.uncompressed_size();
        let key = destination.join_key(&zip_path);
        let source_span_start = stored.header_offset();
        let source_span_end = next_source_offset(&source_offsets, source_span_start)
            .unwrap_or(source_len)
            .min(source_len)
            .max(source_span_start);
        manifest.push(ManifestEntry {
            source_offset: source_span_start,
            source_span_start,
            source_span_end,
            zip_path,
            key,
            size,
            compressed_size: stored.compressed_size(),
            compression: stored.compression(),
            crc32: stored.crc32(),
            catalog_md5: None,
        });
    }

    manifest.sort_unstable_by_key(|entry| entry.source_offset);
    Ok(ManifestBuild {
        entries: manifest,
        catalog_index,
    })
}

pub(crate) fn count_zip_file_entries(entries: &[StoredZipEntry]) -> Result<usize> {
    let mut seen = HashSet::new();
    let mut count = 0_usize;

    for stored in entries {
        let Some(zip_path) = stored_zip_file_path(stored)? else {
            continue;
        };

        if zip_path == EMBEDDED_CATALOG_PATH {
            continue;
        }

        validate_stored_file_entry(stored, &zip_path)?;
        if !seen.insert(zip_path.clone()) {
            return Err(Error::DuplicateZipPath(zip_path));
        }
        count = count.saturating_add(1);
    }

    Ok(count)
}

fn stored_zip_file_path(stored: &StoredZipEntry) -> Result<Option<String>> {
    let raw_path = stored
        .filename()
        .as_str()
        .map_err(|err| Error::InvalidZipEntry {
            path: format!("{:?}", stored.filename().as_bytes()),
            reason: err.to_string(),
        })?;

    normalize_zip_file_path(raw_path)
}

fn validate_stored_file_entry(stored: &StoredZipEntry, zip_path: &str) -> Result<()> {
    match stored.compression() {
        Compression::Stored | Compression::Deflate => {}
        other => {
            return Err(Error::InvalidZipEntry {
                path: zip_path.to_string(),
                reason: format!("unsupported compression method {other:?}"),
            });
        }
    }

    let size = stored.uncompressed_size();
    if size > S3_SINGLE_PUT_LIMIT {
        return Err(Error::EntryTooLarge {
            path: zip_path.to_string(),
            size,
        });
    }

    Ok(())
}

pub(crate) async fn load_embedded_catalog(
    source: Arc<SourceClient>,
    zip_file: ZipFile,
    catalog_index: Option<usize>,
    source_block_size: usize,
) -> HashMap<String, String> {
    let Some(catalog_index) = catalog_index else {
        tracing::debug!("embedded catalog is not present");
        return HashMap::new();
    };
    let Some(entry) = zip_file.entries().get(catalog_index) else {
        tracing::debug!(
            catalog_index,
            "embedded catalog index is missing from ZIP entries"
        );
        return HashMap::new();
    };
    let catalog_size = entry.uncompressed_size();
    let catalog_compressed_size = entry.compressed_size();
    if catalog_size > EMBEDDED_CATALOG_MAX_BYTES
        || catalog_compressed_size > EMBEDDED_CATALOG_MAX_BYTES
    {
        tracing::debug!(
            catalog_size,
            catalog_compressed_size,
            max_bytes = EMBEDDED_CATALOG_MAX_BYTES,
            "embedded catalog is too large"
        );
        return HashMap::new();
    }
    let expected_crc32 = entry.crc32();

    let mut reader = match entry_reader_with_block_size(
        source,
        zip_file,
        catalog_index,
        source_block_size,
    )
    .await
    {
        Ok(reader) => reader,
        Err(err) => {
            tracing::debug!(error = %err, "embedded catalog could not be opened");
            return HashMap::new();
        }
    };
    let mut bytes = Vec::with_capacity(
        usize::try_from(catalog_size)
            .unwrap_or(0)
            .min(EMBEDDED_CATALOG_MAX_BYTES as usize),
    );
    let mut limited_reader = (&mut reader).take(EMBEDDED_CATALOG_MAX_BYTES + 1);
    if let Err(err) = FuturesAsyncReadExt::read_to_end(&mut limited_reader, &mut bytes).await {
        tracing::debug!(error = %err, "embedded catalog could not be read");
        return HashMap::new();
    }
    if bytes.len() > EMBEDDED_CATALOG_MAX_BYTES as usize {
        tracing::debug!(
            read_bytes = bytes.len(),
            max_bytes = EMBEDDED_CATALOG_MAX_BYTES,
            "embedded catalog read exceeded size limit"
        );
        return HashMap::new();
    }
    if let Err(err) = validate_crc32(expected_crc32, &mut reader) {
        tracing::debug!(error = %err, "embedded catalog CRC validation failed");
        return HashMap::new();
    }

    let catalog = match serde_json::from_slice::<EmbeddedCatalog>(&bytes) {
        Ok(catalog) => catalog,
        Err(err) => {
            tracing::debug!(error = %err, "embedded catalog JSON could not be parsed");
            return HashMap::new();
        }
    };
    catalog_md5_by_path(catalog)
}

pub(crate) fn apply_embedded_catalog(
    entries: Vec<ManifestEntry>,
    catalog: HashMap<String, String>,
) -> Vec<ManifestEntry> {
    entries
        .into_iter()
        .map(|mut entry| {
            entry.catalog_md5 = catalog.get(&entry.zip_path).cloned();
            entry
        })
        .collect()
}

pub(crate) fn next_source_offset(sorted_offsets: &[u64], offset: u64) -> Option<u64> {
    let index = sorted_offsets.partition_point(|candidate| *candidate <= offset);
    sorted_offsets.get(index).copied()
}

pub(crate) fn normalize_zip_file_path(raw_path: &str) -> Result<Option<String>> {
    if raw_path.is_empty() {
        return Err(Error::InvalidZipEntry {
            path: raw_path.to_string(),
            reason: "empty path".to_string(),
        });
    }
    if raw_path.contains('\0') {
        return Err(Error::InvalidZipEntry {
            path: raw_path.to_string(),
            reason: "NUL byte in path".to_string(),
        });
    }
    if raw_path.starts_with('/') || raw_path.starts_with('\\') {
        return Err(Error::InvalidZipEntry {
            path: raw_path.to_string(),
            reason: "absolute path".to_string(),
        });
    }
    if raw_path.contains('\\') {
        return Err(Error::InvalidZipEntry {
            path: raw_path.to_string(),
            reason: "backslash path separators are not supported".to_string(),
        });
    }

    let is_directory = raw_path.ends_with('/');
    let trimmed = raw_path.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(Error::InvalidZipEntry {
            path: raw_path.to_string(),
            reason: "root directory entry".to_string(),
        });
    }

    for (index, component) in trimmed.split('/').enumerate() {
        if component.is_empty() {
            return Err(Error::InvalidZipEntry {
                path: raw_path.to_string(),
                reason: "empty path component".to_string(),
            });
        }
        if component == "." || component == ".." {
            return Err(Error::InvalidZipEntry {
                path: raw_path.to_string(),
                reason: "relative path component".to_string(),
            });
        }
        if index == 0 && looks_like_windows_drive(component) {
            return Err(Error::InvalidZipEntry {
                path: raw_path.to_string(),
                reason: "Windows drive prefix".to_string(),
            });
        }
    }

    if is_directory {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

fn looks_like_windows_drive(component: &str) -> bool {
    let bytes = component.as_bytes();
    bytes.len() == 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic()
}

pub(crate) async fn entry_reader_with_block_size(
    source: Arc<SourceClient>,
    zip_file: ZipFile,
    index: usize,
    source_block_size: usize,
) -> Result<ZipEntryReader<'static, tokio_util::compat::Compat<S3RangeReader>, WithoutEntry>> {
    let reader = S3RangeReader::new(source, source_block_size).compat();
    let zip_reader = ZipFileReader::from_raw_parts(reader, zip_file);
    let entry_reader = zip_reader.into_entry(index).await?;
    Ok(entry_reader)
}

pub(crate) fn validate_crc32(
    expected: u32,
    reader: &mut ZipEntryReader<'_, tokio_util::compat::Compat<S3RangeReader>, WithoutEntry>,
) -> Result<()> {
    validate_crc32_value(expected, reader.compute_hash())
}

pub(crate) fn validate_crc32_value(expected: u32, actual: u32) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(async_zip::error::ZipError::CRC32CheckError.into())
    }
}
