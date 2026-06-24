//! S3-compatible protocol layer for Soma — request parsing, XML responses, S3
//! error codes, and AWS SigV4 verification, mapping S3 operations onto the
//! metadata store (`soma-meta`) and storage backend (`soma-backend`).
//!
//! The `S3Service` and `SigV4Verifier` land in the `feat/m0-s3` branch (see
//! `docs/MVP_DESIGN.md` §5). This crate is currently a placeholder that
//! establishes the workspace boundary.
