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
    SetDefinitionFenced {
        binding: EntityBinding,
        issued_at: Timestamp,
        config: QueueConfig,
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

/// An owner-profile view from a describe or prepared mutation.
/// It does not certify the entire ledger.
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

    fn golden_config() -> QueueConfig {
        QueueConfig {
            lock_duration_millis: 1,
            max_delivery_count: 2,
            default_time_to_live_millis: None,
            max_message_bytes: 4,
            requires_session: false,
            requires_duplicate_detection: false,
            duplicate_detection_history_time_window_millis: 20_000,
            dead_lettering_on_message_expiration: true,
        }
    }

    fn golden_binding() -> Result<EntityBinding, BrokerError> {
        let entity = EntityPath::new("q")?;
        EntityBinding::new(
            NamespaceName::new("a")?,
            entity.clone(),
            entity,
            crate::EntityIncarnationKind::Queue,
            1,
        )
    }

    #[test]
    fn original_capacity_instruction_variants_keep_literal_bytes() -> Result<(), BrokerError> {
        let create = QueueCapacityCommandV1::CreateFinite {
            namespace: NamespaceName::new("a")?,
            entity: EntityPath::new("q")?,
            issued_at: Timestamp::from_millis(3),
            config: golden_config(),
            limit: FiniteQueueCapacity::new(5)?,
        };
        let limit = QueueCapacityCommandV1::SetLimitFenced {
            binding: golden_binding()?,
            issued_at: Timestamp::from_millis(3),
            limit: FiniteQueueCapacity::new(5)?,
        };
        for (instruction, bytes) in [
            (
                create,
                vec![11, 0, 1, 97, 1, 113, 3, 1, 2, 0, 4, 0, 0, 160, 156, 1, 1, 5],
            ),
            (limit, vec![11, 1, 1, 97, 1, 113, 1, 113, 0, 1, 3, 5]),
        ] {
            assert_eq!(crate::codec::encode(&instruction)?, bytes);
            assert_eq!(
                crate::codec::decode::<QueueCapacityCommandV1>(&bytes)?,
                instruction
            );
        }
        Ok(())
    }

    #[test]
    fn full_definition_is_additive_and_preserves_unlimited_ttl() -> Result<(), BrokerError> {
        let instruction = QueueCapacityCommandV1::SetDefinitionFenced {
            binding: golden_binding()?,
            issued_at: Timestamp::from_millis(3),
            config: golden_config(),
            limit: FiniteQueueCapacity::new(5)?,
        };
        let bytes = vec![
            11, 2, 1, 97, 1, 113, 1, 113, 0, 1, 3, 1, 2, 0, 4, 0, 0, 160, 156, 1, 1, 5,
        ];
        assert_eq!(crate::codec::encode(&instruction)?, bytes);
        assert_eq!(
            crate::codec::decode::<QueueCapacityCommandV1>(&bytes)?,
            instruction
        );
        let finite_ttl = QueueCapacityCommandV1::SetDefinitionFenced {
            config: QueueConfig {
                default_time_to_live_millis: Some(7),
                ..golden_config()
            },
            binding: golden_binding()?,
            issued_at: Timestamp::from_millis(3),
            limit: FiniteQueueCapacity::new(5)?,
        };
        assert_eq!(
            crate::codec::decode::<QueueCapacityCommandV1>(&crate::codec::encode(&finite_ttl)?)?,
            finite_ttl
        );
        Ok(())
    }

    #[test]
    fn full_definition_rejects_a_decoded_zero_capacity() {
        let bytes = [
            11, 2, 1, 97, 1, 113, 1, 113, 0, 1, 3, 1, 2, 0, 4, 0, 0, 160, 156, 1, 1, 0,
        ];
        assert!(crate::codec::decode::<QueueCapacityCommandV1>(&bytes).is_err());
    }
}
