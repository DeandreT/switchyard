//! Private physical experiments, not canonical seed validation or adoption.

mod control;
mod creation;
mod fjall;
mod input;
mod inventory;
mod memory;
mod tests;

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Error {
    Limit,
    Allocation,
    InvalidLogical,
    Backend,
    Poisoned,
    CommitUnknown,
    Used,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Limit => "physical fixture logical limit exceeded",
            Self::Allocation => "physical fixture allocation failed",
            Self::InvalidLogical => "physical fixture logical records invalid",
            Self::Backend => "physical fixture backend operation failed",
            Self::Poisoned => "physical fixture mutation admission poisoned",
            Self::CommitUnknown => "physical fixture commit decision unknown",
            Self::Used => "physical fixture operation already attempted",
        })
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
type BorrowedRow<'a> = (&'a [u8], &'a [u8]);

fn copy(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| Error::Allocation)?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitFault {
    None,
    BeforeBackendErr,
    AfterSyncErr,
}

#[derive(Default, Debug, Eq, PartialEq)]
struct Counts {
    entered: [usize; 4],
    backend: [usize; 4],
    captures: usize,
}
