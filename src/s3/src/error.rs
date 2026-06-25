//! S3-style error responses.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::sigv4::AuthError;
use crate::xml::escape;

/// An S3 error, carrying the S3 error code, HTTP status, and a message. Renders
/// as the standard S3 `<Error>` XML document.
#[derive(Debug, Clone)]
pub struct S3Error {
    code: &'static str,
    status: StatusCode,
    message: String,
}

impl S3Error {
    fn new(code: &'static str, status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            code,
            status,
            message: message.into(),
        }
    }

    /// `NoSuchBucket` (404).
    pub fn no_such_bucket(b: &str) -> Self {
        Self::new(
            "NoSuchBucket",
            StatusCode::NOT_FOUND,
            format!("The specified bucket does not exist: {b}"),
        )
    }

    /// `NoSuchKey` (404).
    pub fn no_such_key(k: &str) -> Self {
        Self::new(
            "NoSuchKey",
            StatusCode::NOT_FOUND,
            format!("The specified key does not exist: {k}"),
        )
    }

    /// `BucketAlreadyOwnedByYou` (409).
    pub fn bucket_exists(b: &str) -> Self {
        Self::new(
            "BucketAlreadyOwnedByYou",
            StatusCode::CONFLICT,
            format!("Bucket already exists: {b}"),
        )
    }

    /// `BucketNotEmpty` (409).
    pub fn bucket_not_empty(b: &str) -> Self {
        Self::new(
            "BucketNotEmpty",
            StatusCode::CONFLICT,
            format!("The bucket is not empty: {b}"),
        )
    }

    /// `InvalidBucketName` (400).
    pub fn invalid_bucket_name(b: &str) -> Self {
        Self::new(
            "InvalidBucketName",
            StatusCode::BAD_REQUEST,
            format!("Invalid bucket name: {b}"),
        )
    }

    /// `PreconditionFailed` (412).
    pub fn precondition_failed() -> Self {
        Self::new(
            "PreconditionFailed",
            StatusCode::PRECONDITION_FAILED,
            "At least one of the preconditions you specified did not hold",
        )
    }

    /// `InvalidArgument` (400).
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new("InvalidArgument", StatusCode::BAD_REQUEST, msg)
    }

    /// `InvalidRange` (416).
    pub fn invalid_range() -> Self {
        Self::new(
            "InvalidRange",
            StatusCode::RANGE_NOT_SATISFIABLE,
            "The requested range is not satisfiable",
        )
    }

    /// `AccessDenied` (403).
    pub fn access_denied(msg: impl Into<String>) -> Self {
        Self::new("AccessDenied", StatusCode::FORBIDDEN, msg)
    }

    /// `SignatureDoesNotMatch` (403).
    pub fn signature_mismatch() -> Self {
        Self::new(
            "SignatureDoesNotMatch",
            StatusCode::FORBIDDEN,
            "The request signature does not match",
        )
    }

    /// `NoSuchUpload` (404) — unknown multipart upload id.
    pub fn no_such_upload(id: &str) -> Self {
        Self::new(
            "NoSuchUpload",
            StatusCode::NOT_FOUND,
            format!("The specified multipart upload does not exist: {id}"),
        )
    }

    /// `InvalidPart` (400) — a completed part is missing or mismatched.
    pub fn invalid_part(msg: impl Into<String>) -> Self {
        Self::new("InvalidPart", StatusCode::BAD_REQUEST, msg)
    }

    /// `NotImplemented` (501).
    pub fn not_implemented(msg: impl Into<String>) -> Self {
        Self::new("NotImplemented", StatusCode::NOT_IMPLEMENTED, msg)
    }

    /// `ServerSideEncryptionConfigurationNotFoundError` (404) — the bucket has no
    /// default encryption configured.
    pub fn no_encryption_config() -> Self {
        Self::new(
            "ServerSideEncryptionConfigurationNotFoundError",
            StatusCode::NOT_FOUND,
            "The server side encryption configuration was not found",
        )
    }

    /// `QuotaExceeded` (403) — the tenant's storage quota would be exceeded.
    pub fn quota_exceeded(msg: impl Into<String>) -> Self {
        Self::new("QuotaExceeded", StatusCode::FORBIDDEN, msg)
    }

    /// `SlowDown` (503) — the tenant exceeded its request rate limit.
    pub fn slow_down() -> Self {
        Self::new(
            "SlowDown",
            StatusCode::SERVICE_UNAVAILABLE,
            "Reduce your request rate",
        )
    }

    /// `InternalError` (500).
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(
            "InternalError",
            StatusCode::INTERNAL_SERVER_ERROR,
            msg.into(),
        )
    }

    /// The HTTP status code.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Render the `<Error>` XML document.
    fn to_xml(&self) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>{}</Code><Message>{}</Message></Error>",
            self.code,
            escape(&self.message)
        )
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        (
            self.status,
            [(header::CONTENT_TYPE, "application/xml")],
            self.to_xml(),
        )
            .into_response()
    }
}

impl From<AuthError> for S3Error {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::SignatureMismatch => S3Error::signature_mismatch(),
            AuthError::Missing | AuthError::Malformed => {
                S3Error::access_denied(format!("authorization: {e}"))
            }
            AuthError::UnknownAccessKey => S3Error::access_denied("unknown access key"),
        }
    }
}

impl From<soma_meta::Error> for S3Error {
    fn from(e: soma_meta::Error) -> Self {
        use soma_meta::Error as M;
        match e {
            M::NoSuchBucket(b) => S3Error::no_such_bucket(&b),
            M::BucketAlreadyExists(b) => S3Error::bucket_exists(&b),
            M::BucketNotEmpty(b) => S3Error::bucket_not_empty(&b),
            M::InvalidBucketName(b) => S3Error::invalid_bucket_name(&b),
            M::PreconditionFailed => S3Error::precondition_failed(),
            M::QuotaExceeded(msg) => S3Error::quota_exceeded(msg),
            other => S3Error::internal(other.to_string()),
        }
    }
}

impl From<soma_backend::Error> for S3Error {
    fn from(e: soma_backend::Error) -> Self {
        use soma_backend::Error as B;
        match e {
            B::BadRange { .. } => S3Error::invalid_range(),
            other => S3Error::internal(other.to_string()),
        }
    }
}

/// Convenience result type for handler logic.
pub type S3Result<T> = std::result::Result<T, S3Error>;
