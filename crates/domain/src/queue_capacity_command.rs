use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::{BrokerError, EntityBinding, EntityPath, NamespaceName, QueueConfig, Timestamp};

/// A logical reservation limit, not a physical disk or namespace size.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FiniteQueueCapacity(NonZeroU64);

impl FiniteQueueCapacity {
    pub fn new(bytes: u64) -> Result<Self, BrokerError> {
        NonZeroU64::new(bytes)
            .map(Self)
            .ok_or(BrokerError::InvalidQueueCapacity)
    }

    pub const fn bytes(self) -> u64 {
        self.0.get()
    }

    pub(crate) const fn nonzero(self) -> NonZeroU64 {
        self.0
    }
}

/// Separate versioned instructions preserve the existing CommandKind encoding.
/// Issued timestamps come from the proposer, never from the state machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum QueueCapacityCommandV1 {
    CreateFinite {
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    },
    SetLimitFenced {
        binding: EntityBinding,
        issued_at: Timestamp,
        limit: FiniteQueueCapacity,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum QueueCapacityStatus {
    NonFinite,
    FiniteV1 {
        limit: FiniteQueueCapacity,
        reserved_bytes: u64,
        message_count: u64,
    },
}

/// A clock-free view from one owner turn; it does not certify the entire ledger.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueCapacityView {
    pub binding: EntityBinding,
    pub config: QueueConfig,
    pub capacity: QueueCapacityStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_limit_refuses_zero_and_preserves_the_full_unsigned_range() {
        assert_eq!(
            FiniteQueueCapacity::new(0),
            Err(BrokerError::InvalidQueueCapacity)
        );
        for bytes in [1, u64::MAX] {
            let limit = FiniteQueueCapacity::new(bytes).unwrap();
            assert_eq!(limit.bytes(), bytes);
            assert_eq!(
                crate::codec::decode::<FiniteQueueCapacity>(&crate::codec::encode(&bytes).unwrap())
                    .unwrap(),
                limit
            );
        }
        assert!(
            crate::codec::decode::<FiniteQueueCapacity>(&crate::codec::encode(&0_u64).unwrap())
                .is_err()
        );
    }
}
