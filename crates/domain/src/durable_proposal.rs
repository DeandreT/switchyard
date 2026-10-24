//! A frozen persistence envelope, not a client authentication or grant format.
//!
//! V1, V2 and V3 carry intent, including intent the state machine may refuse.
//! Decoding does not consult the catalog, acquire authority, or apply a command.
//! The byte ceiling bounds encoded input/output and copied variable data, not
//! total heap usage: owned DTO/container and domain-object overhead is extra.

use thiserror::Error;

use crate::{BoundCommand, Command, CommandKind, EntityBinding};

mod v1;
mod v2;
mod v3;

pub const MAX_DURABLE_PROPOSAL_BYTES: usize = 1024 * 1024;
const HEADER_BYTES: usize = 12;
const MAGIC: &[u8; 4] = b"SWDP";

/// One original instruction and its explicitly retained authority mode.
///
/// This type deliberately has no public serde implementation or raw authority
/// constructor. A successful decode establishes persistence-data shape only;
/// forged but structurally valid bytes are not proof of authenticated authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableProposal {
    command: Command,
    binding: Option<EntityBinding>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableProposalAuthority<'a> {
    Unbound,
    Bound(&'a EntityBinding),
}

impl DurableProposal {
    pub fn unbound(command: Command) -> Self {
        Self {
            command,
            binding: None,
        }
    }

    pub fn bound(command: BoundCommand) -> Result<Self, DurableProposalError> {
        let (binding, command) = command.into_parts();
        if uses_v2(&command.kind) {
            v2::validate_binding(&binding, &command)?;
        } else {
            v1::validate_binding(&binding, &command)?;
        }
        Ok(Self {
            command,
            binding: Some(binding),
        })
    }

    pub fn command(&self) -> &Command {
        &self.command
    }

    pub fn authority(&self) -> DurableProposalAuthority<'_> {
        match self.binding.as_ref() {
            None => DurableProposalAuthority::Unbound,
            Some(binding) => DurableProposalAuthority::Bound(binding),
        }
    }

    /// Existing intent stays byte-for-byte V1/V2. Only nonlegacy topic creation
    /// uses V3. No proposal format uses the ordinary Command serde shape.
    pub fn encode(&self) -> Result<Vec<u8>, DurableProposalError> {
        if uses_v3(&self.command.kind) {
            encode_version(&v3::Proposal::capture(self)?, 3)
        } else if uses_v2(&self.command.kind) {
            encode_version(&v2::Proposal::capture(self)?, 2)
        } else {
            encode_v1(&v1::Proposal::capture(self)?)
        }
    }

    /// Requires exact framing and canonical postcard bytes, including lengths,
    /// tags, property ordering and every authority/resource identity.
    pub fn decode(bytes: &[u8]) -> Result<Self, DurableProposalError> {
        if bytes.len() > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(DurableProposalError::TooLarge);
        }
        if bytes.len() < HEADER_BYTES || &bytes[..4] != MAGIC {
            return Err(DurableProposalError::Malformed);
        }
        let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if !matches!(version, 1..=3) {
            return Err(DurableProposalError::UnsupportedVersion(version));
        }
        let length = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        if length != bytes.len() - HEADER_BYTES {
            return Err(DurableProposalError::Malformed);
        }
        if version == 1 {
            decode_version::<v1::Proposal>(bytes, version)?.restore()
        } else if version == 2 {
            decode_version::<v2::Proposal>(bytes, version)?.restore()
        } else {
            decode_version::<v3::Proposal>(bytes, version)?.restore()
        }
    }
}

fn uses_v2(kind: &CommandKind) -> bool {
    matches!(
        kind,
        CommandKind::UpdateTopic { .. } | CommandKind::UpdateSubscription { .. }
    )
}

fn uses_v3(kind: &CommandKind) -> bool {
    matches!(kind, CommandKind::CreateTopic { config } if !config.is_legacy_representable())
}

fn encode_v1(value: &v1::Proposal) -> Result<Vec<u8>, DurableProposalError> {
    encode_version(value, 1)
}

fn encode_version<T: serde::Serialize>(
    value: &T,
    version: u32,
) -> Result<Vec<u8>, DurableProposalError> {
    let mut output = vec![0; MAX_DURABLE_PROPOSAL_BYTES];
    let length = postcard::to_slice(value, &mut output[HEADER_BYTES..])
        .map_err(|_| DurableProposalError::TooLarge)?
        .len();
    output[..4].copy_from_slice(MAGIC);
    output[4..8].copy_from_slice(&version.to_be_bytes());
    output[8..12].copy_from_slice(&(length as u32).to_be_bytes());
    output.truncate(HEADER_BYTES + length);
    Ok(output)
}

fn decode_version<T: serde::de::DeserializeOwned + serde::Serialize>(
    bytes: &[u8],
    version: u32,
) -> Result<T, DurableProposalError> {
    let (value, unused) = postcard::take_from_bytes::<T>(&bytes[HEADER_BYTES..])
        .map_err(|_| DurableProposalError::Malformed)?;
    if !unused.is_empty() {
        return Err(DurableProposalError::Malformed);
    }
    if encode_version(&value, version)? != bytes {
        return Err(DurableProposalError::NonCanonical);
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DurableProposalError {
    #[error("durable proposal exceeds the one-MiB envelope limit")]
    TooLarge,
    #[error("durable proposal framing or versioned data is malformed")]
    Malformed,
    #[error("durable proposal version {0} is unsupported")]
    UnsupportedVersion(u32),
    #[error("command cannot be represented by durable proposal version {0}")]
    UnsupportedCommandVersion(u32),
    #[error("durable proposal data is not canonical")]
    NonCanonical,
    #[error("durable proposal contains an invalid identifier")]
    InvalidIdentifier,
    #[error("durable proposal authority shape or command scope is invalid")]
    InvalidAuthority,
    #[error("durable proposal integer does not fit this platform")]
    IntegerOutOfRange,
}
