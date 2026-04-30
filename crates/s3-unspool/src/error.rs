use std::error::Error as StdError;
use std::fmt::Display;
use std::io;

use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use thiserror::Error;

/// Result type returned by `s3-unspool` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type returned by `s3-unspool` operations.
#[derive(Debug, Error)]
pub enum Error {
    /// An `s3://` URI could not be parsed.
    #[error("invalid S3 URI `{uri}`: {reason}")]
    InvalidS3Uri {
        /// Original URI string.
        uri: String,
        /// Human-readable parse failure.
        reason: String,
    },
    /// A local upload path is invalid or unsupported.
    #[error("invalid local path `{path}`: {reason}")]
    InvalidLocalPath {
        /// Local path that failed validation.
        path: String,
        /// Human-readable validation failure.
        reason: String,
    },
    /// A [`SyncOptions`](crate::SyncOptions) or [`UploadOptions`](crate::UploadOptions)
    /// value is invalid.
    #[error("invalid option: {0}")]
    InvalidOption(String),
    /// A ZIP entry path is invalid or unsupported.
    #[error("invalid ZIP entry `{path}`: {reason}")]
    InvalidZipEntry {
        /// ZIP entry path that failed validation.
        path: String,
        /// Human-readable validation failure.
        reason: String,
    },
    /// The source ZIP contains the same normalized path more than once.
    #[error("duplicate ZIP file path `{0}`")]
    DuplicateZipPath(String),
    /// A ZIP entry is larger than the supported single-`PutObject` limit.
    #[error("entry `{path}` is {size} bytes, larger than the S3 single PutObject limit")]
    EntryTooLarge {
        /// ZIP entry path.
        path: String,
        /// Entry size in bytes.
        size: u64,
    },
    /// An AWS S3 operation failed.
    #[error("S3 {operation} failed for s3://{bucket}/{key}: {message}")]
    S3 {
        /// S3 operation name.
        operation: &'static str,
        /// S3 bucket used by the failed operation.
        bucket: String,
        /// S3 key used by the failed operation.
        key: String,
        /// Error message from the AWS SDK.
        message: String,
    },
    /// A conditional destination write failed because the destination changed after listing.
    #[error("conditional write failed for s3://{bucket}/{key}: {message}")]
    ConditionalConflict {
        /// Destination bucket.
        bucket: String,
        /// Destination key.
        key: String,
        /// Conflict message from S3.
        message: String,
    },
    /// A multipart upload failed, and aborting it also failed.
    #[error("{original}; additionally failed to abort multipart upload: {abort}")]
    MultipartAbort {
        /// Original upload failure.
        original: Box<Error>,
        /// Abort failure that may leave an orphaned multipart upload.
        abort: Box<Error>,
    },
    /// Reading or writing ZIP data failed.
    #[error("ZIP operation failed: {0}")]
    Zip(#[from] async_zip::error::ZipError),
    /// Local I/O failed while reading upload sources or streaming data.
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    /// A background Tokio task failed.
    #[error("worker task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// Building an AWS SDK request failed before it was sent.
    #[error("AWS SDK build failed: {0}")]
    Build(String),
}

pub(crate) fn aws_error_message(error: &(impl ProvideErrorMetadata + Display)) -> String {
    match (error.code(), error.message()) {
        (Some(code), Some(message)) if !message.is_empty() && message != code => {
            format!("{code}: {message}")
        }
        (Some(code), _) => code.to_string(),
        (None, Some(message)) if !message.is_empty() => message.to_string(),
        _ => error.to_string(),
    }
}

pub(crate) fn aws_error_context(error: &(impl ProvideErrorMetadata + StdError)) -> String {
    format!("{}", DisplayErrorContext(error))
}

#[cfg(test)]
mod tests {
    use aws_smithy_types::error::ErrorMetadata;

    use super::*;

    #[test]
    fn aws_error_message_prefers_code_and_message() {
        let metadata = ErrorMetadata::builder()
            .code("NoSuchKey")
            .message("The specified key does not exist.")
            .build();

        assert_eq!(
            aws_error_message(&metadata),
            "NoSuchKey: The specified key does not exist."
        );
    }

    #[test]
    fn aws_error_message_falls_back_to_display() {
        let metadata = ErrorMetadata::builder().build();

        assert_eq!(aws_error_message(&metadata), "Error");
    }

    #[test]
    fn aws_error_context_includes_error_chain() {
        let metadata = ErrorMetadata::builder()
            .code("NoSuchKey")
            .message("The specified key does not exist.")
            .build();

        let context = aws_error_context(&metadata);

        assert!(context.contains("NoSuchKey"));
        assert!(context.contains("The specified key does not exist."));
    }
}
