//! A bounded raw-record artifact and opt-in offline capture.
//!
//! Declared frontiers and identity do not prove domain or journal health,
//! coherent capture, authentication, installation or recovery. Capture validates
//! one image with separately retained provenance; it does not certify authority
//! or installation and requires caller-enforced exclusion of every writer.

pub(crate) mod anchor;
mod capture;
mod compaction;
mod format;
mod install;
mod recovery;

pub use anchor::{
    PreparedSnapshotAnchor, SNAPSHOT_ANCHOR_HASH_SCOPE, SNAPSHOT_ANCHOR_VERSION, SnapshotAnchor,
    SnapshotAnchorError, SnapshotAnchorLimits, SnapshotAnchorOwner, SnapshotAnchorReceipt,
};
pub(crate) use capture::FrozenImage;
pub use capture::{SnapshotCapture, SnapshotCaptureError};
pub use compaction::{
    PreparedSnapshotCompaction, SnapshotCompactionError, SnapshotCompactionLimits,
    SnapshotCompactionOwner, SnapshotCompactionReceipt,
};
pub use format::{
    SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FRAME_DIGEST_BYTES, SNAPSHOT_FRAME_HASH_SCOPE,
    SNAPSHOT_LAYOUT_V2, SNAPSHOT_LAYOUT_VERSION, SNAPSHOT_MAGIC, SnapshotFormat,
    SnapshotFormatError, SnapshotLimit, SnapshotLimits, SnapshotManifest, SnapshotView,
};
pub use install::{
    SnapshotInstallError, SnapshotInstallLimits, SnapshotInstallOwner, SnapshotInstallPolicy,
    SnapshotInstallReceipt,
};
pub use recovery::{SnapshotRecoveryClass, SnapshotRecoverySelection, select_recovery};
