//! Read-only validation of an immutable logical record image.
//!
//! Catalog success is partial: recognized domain state and external journal /
//! indexed rows remain unvalidated. It establishes no storage provenance,
//! retained authority, full state health, capture, installation or recovery.

mod catalog;
mod state;

use thiserror::Error;

use crate::Timestamp;

pub use catalog::validate_catalog;
pub use state::{
    DuplicateRowsValidation, MessageRowsValidation, SessionRowsValidation, SnapshotDuplicateError,
    SnapshotSessionError, SnapshotStateError, validate_duplicate_rows, validate_message_rows,
    validate_session_rows,
};

/// Catalog-only observations; pending row counts are not health evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogValidation {
    clock: Option<Timestamp>,
    catalog_rows: usize,
    unvalidated_state_rows: usize,
    unvalidated_external_rows: usize,
}

impl CatalogValidation {
    pub fn clock(&self) -> Option<Timestamp> {
        self.clock
    }

    /// Includes the Clock row, when present.
    pub fn catalog_rows(&self) -> usize {
        self.catalog_rows
    }

    pub fn unvalidated_state_rows(&self) -> usize {
        self.unvalidated_state_rows
    }

    pub fn unvalidated_external_rows(&self) -> usize {
        self.unvalidated_external_rows
    }
}

/// Refusal of a catalog image. Row ordinals refer to the original input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SnapshotCatalogError {
    #[error("snapshot row {row} has an empty key")]
    EmptyKey { row: usize },
    #[error("snapshot row {row} does not advance in strict key order")]
    InputOrder { row: usize },
    #[error("snapshot row {row} has unsupported tag {tag:#04x}")]
    UnsupportedTag { row: usize, tag: u8 },
    #[error("snapshot row {row} has an invalid catalog key: {detail}")]
    InvalidKey { row: usize, detail: &'static str },
    #[error("snapshot row {row} has an invalid catalog value: {detail}")]
    InvalidValue { row: usize, detail: &'static str },
    #[error("populated domain records have no Clock companion")]
    MissingClock,
    #[error("snapshot row {row} has an inconsistent catalog relation: {detail}")]
    InconsistentCatalog { row: usize, detail: &'static str },
}
