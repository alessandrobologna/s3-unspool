use aws_sdk_s3::Client;

use crate::error::{Error, Result, aws_error_message};
use crate::s3_uri::S3Object;

#[derive(Clone, Debug)]
pub(crate) struct SourceHead {
    pub(crate) len: u64,
    pub(crate) etag: Option<String>,
}

pub(crate) async fn head_source(client: &Client, source: &S3Object) -> Result<SourceHead> {
    let output = client
        .head_object()
        .bucket(&source.bucket)
        .key(&source.key)
        .send()
        .await
        .map_err(|err| Error::S3 {
            operation: "HeadObject",
            bucket: source.bucket.clone(),
            key: source.key.clone(),
            message: aws_error_message(&err),
        })?;

    let len = output.content_length().ok_or_else(|| Error::S3 {
        operation: "HeadObject",
        bucket: source.bucket.clone(),
        key: source.key.clone(),
        message: "missing content length".to_string(),
    })?;

    let len = u64::try_from(len).map_err(|_| Error::S3 {
        operation: "HeadObject",
        bucket: source.bucket.clone(),
        key: source.key.clone(),
        message: format!("negative content length {len}"),
    })?;

    Ok(SourceHead {
        len,
        etag: output.e_tag().map(str::to_string),
    })
}
