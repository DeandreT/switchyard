//! A bounded raw-record artifact, not a certified store image.
//!
//! Declared frontiers and identity do not prove domain or journal health,
//! coherent capture, authentication, installation or recovery.

mod format;

pub use format::{
    SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FRAME_DIGEST_BYTES, SNAPSHOT_FRAME_HASH_SCOPE,
    SNAPSHOT_LAYOUT_VERSION, SNAPSHOT_MAGIC, SnapshotFormat, SnapshotFormatError, SnapshotLimit,
    SnapshotLimits, SnapshotManifest, SnapshotView,
};
