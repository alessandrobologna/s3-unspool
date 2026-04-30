use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Parsed S3 object URI.
///
/// Use [`S3Object::parse`] to construct an object from an `s3://bucket/key`
/// string.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3Object {
    /// S3 bucket name.
    pub bucket: String,
    /// S3 object key.
    pub key: String,
}

impl S3Object {
    /// Parses an object URI such as `s3://my-bucket/path/archive.zip`.
    pub fn parse(uri: impl AsRef<str>) -> Result<Self> {
        let uri = uri.as_ref();
        let (bucket, key) = parse_s3_parts(uri, true)?;
        let key = key.trim_start_matches('/').to_string();
        if key.is_empty() {
            return Err(Error::InvalidS3Uri {
                uri: uri.to_string(),
                reason: "missing object key".to_string(),
            });
        }
        Ok(Self { bucket, key })
    }

    /// Formats this object as an `s3://bucket/key` URI.
    pub fn uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }
}

/// Parsed S3 destination prefix URI.
///
/// Prefixes may be empty, for example `s3://my-bucket`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3Prefix {
    /// S3 bucket name.
    pub bucket: String,
    /// Destination key prefix, without a leading slash.
    pub prefix: String,
}

impl S3Prefix {
    /// Parses a prefix URI such as `s3://my-bucket/site/`.
    pub fn parse(uri: impl AsRef<str>) -> Result<Self> {
        let uri = uri.as_ref();
        let (bucket, prefix) = parse_s3_parts(uri, false)?;
        let prefix = prefix.trim_start_matches('/').to_string();
        Ok(Self { bucket, prefix })
    }

    /// Joins a normalized ZIP path onto this prefix.
    pub fn join_key(&self, zip_path: &str) -> String {
        join_destination_key(&self.prefix, zip_path)
    }

    /// Formats this prefix as an `s3://bucket/prefix` URI.
    pub fn uri(&self) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.prefix)
        }
    }
}

fn parse_s3_parts(uri: &str, require_key: bool) -> Result<(String, String)> {
    let without_scheme = uri
        .strip_prefix("s3://")
        .ok_or_else(|| Error::InvalidS3Uri {
            uri: uri.to_string(),
            reason: "missing s3:// scheme".to_string(),
        })?;

    let (bucket, key) = match without_scheme.split_once('/') {
        Some((bucket, key)) => (bucket, key),
        None if require_key => {
            return Err(Error::InvalidS3Uri {
                uri: uri.to_string(),
                reason: "missing object key".to_string(),
            });
        }
        None => (without_scheme, ""),
    };

    if bucket.is_empty() {
        return Err(Error::InvalidS3Uri {
            uri: uri.to_string(),
            reason: "missing bucket".to_string(),
        });
    }

    if require_key && key.is_empty() {
        return Err(Error::InvalidS3Uri {
            uri: uri.to_string(),
            reason: "missing object key".to_string(),
        });
    }

    Ok((bucket.to_string(), key.to_string()))
}

pub(crate) fn join_destination_key(prefix: &str, zip_path: &str) -> String {
    if prefix.is_empty() {
        zip_path.to_string()
    } else if prefix.ends_with('/') {
        format!("{prefix}{zip_path}")
    } else {
        format!("{prefix}/{zip_path}")
    }
}

pub(crate) fn normalize_etag(etag: &str) -> Option<String> {
    let trimmed = etag.trim().trim_matches('"').to_ascii_lowercase();
    if trimmed.len() == 32 && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Some(trimmed)
    } else {
        None
    }
}
