//! Reserved proposal V11: owned intent, never a reinterpretation of V7.

use crate::{
    DurableProposal, DurableProposalAuthority, DurableProposalError as Error, EntityBindingKind,
    EntityPath, MAX_DURABLE_PROPOSAL_BYTES, NamespaceName, retained_accounting::CapacityOwner,
};

const HEADER: usize = 12;

/// Pure persistence shape. Current runtime/replay intentionally refuse V11;
/// there is no conversion that discards the owner fence into executable intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityIntent {
    owner: CapacityOwner,
    original: DurableProposal,
}

impl CapacityIntent {
    pub fn capture(owner: CapacityOwner, original: DurableProposal) -> Result<Self, Error> {
        let command = original.command();
        if command.namespace != *owner.namespace()
            || (command.entity != *owner.parent() && command.entity != *owner.shadow())
        {
            return Err(Error::InvalidAuthority);
        }
        if let DurableProposalAuthority::Bound(binding) = original.authority()
            && (binding.kind() != EntityBindingKind::Queue
                || binding.generation() != owner.generation()
                || binding.owner() != owner.parent()
                || binding.namespace() != owner.namespace()
                || binding.target() != &command.entity)
        {
            return Err(Error::InvalidAuthority);
        }
        Ok(Self { owner, original })
    }

    pub fn owner(&self) -> &CapacityOwner {
        &self.owner
    }
    pub fn original(&self) -> &DurableProposal {
        &self.original
    }

    /// Fixed-width big-endian generation, namespace/path byte lengths, exact
    /// UTF-8 identity, then the original complete canonical proposal envelope.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let namespace = self.owner.namespace().as_str().as_bytes();
        let parent = self.owner.parent().as_str().as_bytes();
        let original = self.original.encode()?;
        check_inner(&original)?;
        let length = 12 + namespace.len() + parent.len() + original.len();
        if HEADER + length > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        let mut bytes = b"SWDP".to_vec();
        bytes.extend_from_slice(&11_u32.to_be_bytes());
        bytes.extend_from_slice(&(length as u32).to_be_bytes());
        bytes.extend_from_slice(&self.owner.generation().to_be_bytes());
        bytes.extend_from_slice(
            &u16::try_from(namespace.len())
                .map_err(|_| Error::TooLarge)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u16::try_from(parent.len())
                .map_err(|_| Error::TooLarge)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(namespace);
        bytes.extend_from_slice(parent);
        bytes.extend_from_slice(&original);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        if bytes.len() < 24 || bytes[..4] != *b"SWDP" {
            return Err(Error::Malformed);
        }
        let version = u32::from_be_bytes(bytes[4..8].try_into().expect("version"));
        if version != 11 {
            return Err(Error::UnsupportedVersion(version));
        }
        if u32::from_be_bytes(bytes[8..12].try_into().expect("length")) as usize
            != bytes.len() - HEADER
        {
            return Err(Error::Malformed);
        }
        let generation = u64::from_be_bytes(bytes[12..20].try_into().expect("generation"));
        let n = u16::from_be_bytes(bytes[20..22].try_into().expect("namespace length")) as usize;
        let p = u16::from_be_bytes(bytes[22..24].try_into().expect("parent length")) as usize;
        let text = |start, end| {
            std::str::from_utf8(bytes.get(start..end).ok_or(Error::Malformed)?)
                .map_err(|_| Error::InvalidIdentifier)
        };
        let namespace =
            NamespaceName::new(text(24, 24 + n)?).map_err(|_| Error::InvalidIdentifier)?;
        let parent =
            EntityPath::new(text(24 + n, 24 + n + p)?).map_err(|_| Error::InvalidIdentifier)?;
        let owner = CapacityOwner::new(namespace, parent, generation)
            .map_err(|_| Error::InvalidAuthority)?;
        // Explicit bound remains even if the general decoder later learns V11.
        let inner = bytes.get(24 + n + p..).ok_or(Error::Malformed)?;
        check_inner(inner)?;
        let original = DurableProposal::decode(inner)?;
        let result = Self::capture(owner, original)?;
        if result.encode()? != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(result)
    }
}

fn check_inner(bytes: &[u8]) -> Result<(), Error> {
    if bytes.len() < HEADER || bytes[..4] != *b"SWDP" {
        return Err(Error::Malformed);
    }
    let version = u32::from_be_bytes(bytes[4..8].try_into().expect("version"));
    if !(1..=10).contains(&version) {
        return Err(Error::UnsupportedVersion(version));
    }
    Ok(())
}
