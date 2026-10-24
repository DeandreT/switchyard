//! A bounded raw-record artifact and opt-in offline capture.
//!
//! Declared frontiers and identity do not prove domain or journal health,
//! coherent capture, authentication, installation or recovery. Capture validates
//! one image with separately retained provenance; it does not certify authority
//! or installation and requires caller-enforced exclusion of every writer.

mod capture;
mod format;

pub use capture::{SnapshotCapture, SnapshotCaptureError};
pub use format::{
    SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FRAME_DIGEST_BYTES, SNAPSHOT_FRAME_HASH_SCOPE,
    SNAPSHOT_LAYOUT_VERSION, SNAPSHOT_MAGIC, SnapshotFormat, SnapshotFormatError, SnapshotLimit,
    SnapshotLimits, SnapshotManifest, SnapshotView,
};
