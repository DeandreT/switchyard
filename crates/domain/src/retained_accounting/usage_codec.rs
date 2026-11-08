//! Canonical, owner/key-bound usage V1. No live writer emits these rows yet.

use sha2::{Digest, Sha256};

use super::{AccountingError, CapacityOwner, RESERVATION_MODEL_V1, RetainedUsage, keys};

pub const USAGE_VERSION: u16 = 1;
pub const ENCODED_USAGE_BYTES: usize = 72;
const MAGIC: &[u8; 4] = b"SYBU";

pub fn encode(owner: &CapacityOwner, usage: RetainedUsage) -> Result<Vec<u8>, AccountingError> {
    if owner.generation() != usage.generation() {
        return Err(AccountingError::GenerationMismatch);
    }
    usage.committed_bytes()?;
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&USAGE_VERSION.to_be_bytes());
    bytes.extend_from_slice(&RESERVATION_MODEL_V1.to_be_bytes());
    for number in [
        usage.generation(),
        usage.retained_bytes(),
        usage.reserved_growth_bytes(),
        usage.message_count(),
    ] {
        bytes.extend_from_slice(&number.to_be_bytes());
    }
    let digest = checksum(owner, &bytes);
    bytes.extend_from_slice(&digest);
    Ok(bytes)
}

pub fn decode(owner: &CapacityOwner, bytes: &[u8]) -> Result<RetainedUsage, AccountingError> {
    if bytes.len() != ENCODED_USAGE_BYTES || bytes.get(..4) != Some(MAGIC) {
        return Err(AccountingError::CorruptUsage);
    }
    if bytes[4..6] != USAGE_VERSION.to_be_bytes()
        || bytes[6..8] != RESERVATION_MODEL_V1.to_be_bytes()
    {
        return Err(AccountingError::UnsupportedVersion);
    }
    if bytes[40..] != checksum(owner, &bytes[..40]) {
        return Err(AccountingError::CorruptUsage);
    }
    let number = |start| {
        u64::from_be_bytes(
            bytes[start..start + 8]
                .try_into()
                .expect("fixed usage field"),
        )
    };
    let usage = RetainedUsage::new(number(8), number(16), number(24), number(32))?;
    if usage.generation() != owner.generation() {
        return Err(AccountingError::GenerationMismatch);
    }
    Ok(usage)
}

fn checksum(owner: &CapacityOwner, bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"switchyard retained usage v1\0");
    hash.update(keys::usage(owner));
    hash.update(bytes);
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntityPath, NamespaceName};

    #[test]
    fn valid_checksums_do_not_hide_impossible_or_overflowing_totals() {
        let owner = CapacityOwner::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("q").unwrap(),
            1,
        )
        .unwrap();
        let original = encode(&owner, RetainedUsage::new(1, 1, 0, 1).unwrap()).unwrap();
        for (actual, reserved, count, error) in [
            (u64::MAX, 1_u64, 1_u64, AccountingError::Overflow),
            (1, 0, 0, AccountingError::CorruptUsage),
            (0, 1, 1, AccountingError::CorruptUsage),
        ] {
            let mut bytes = original[..40].to_vec();
            bytes[16..24].copy_from_slice(&actual.to_be_bytes());
            bytes[24..32].copy_from_slice(&reserved.to_be_bytes());
            bytes[32..40].copy_from_slice(&count.to_be_bytes());
            bytes.extend_from_slice(&checksum(&owner, &bytes));
            assert_eq!(decode(&owner, &bytes), Err(error));
        }
        // Fixed unsigned fields have no alternate signed/negative encoding.
        let negative = vec![0xff; ENCODED_USAGE_BYTES];
        assert_eq!(
            decode(&owner, &negative),
            Err(AccountingError::CorruptUsage)
        );
        let empty = RetainedUsage::new(1, 0, 0, 0).unwrap();
        assert_eq!(decode(&owner, &encode(&owner, empty).unwrap()), Ok(empty));
    }
}
