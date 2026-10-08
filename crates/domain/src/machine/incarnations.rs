//! Creation-time owner identity; retained endpoint authority is a separate layer.

use serde::{Deserialize, Serialize};
use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, DEAD_LETTER_QUEUE_SUFFIX, EntityPath, NamespaceName, SubscriptionName, codec,
    identifier::SUBSCRIPTION_PATH_SEGMENT, keys,
};

use super::StateMachine;

const MAX_HEAD_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) enum Kind {
    Queue,
    Topic,
    Subscription,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Record {
    generation: u64,
    kind: Kind,
    retired: bool,
}

fn decode_live(raw: &[u8], expected: Kind) -> Result<Record, BrokerError> {
    if raw.len() > MAX_HEAD_BYTES {
        return Err(BrokerError::EntityMetadataCorrupt);
    }
    let record: Record = codec::decode(raw).map_err(|_| BrokerError::EntityMetadataCorrupt)?;
    if record.generation == 0
        || record.kind != expected
        || record.retired
        || codec::encode(&record).map_err(|_| BrokerError::EntityMetadataCorrupt)? != raw
    {
        return Err(BrokerError::EntityMetadataCorrupt);
    }
    Ok(record)
}

fn primary_path(value: &str) -> Option<EntityPath> {
    let path = EntityPath::new(value).ok()?;
    (!path.is_dead_letter_queue() && !path.is_subscription() && !path.is_management())
        .then_some(path)
}

fn queue_owner(entity: &EntityPath) -> Result<(EntityPath, Kind, bool), BrokerError> {
    let (value, is_shadow) = entity
        .as_str()
        .strip_suffix(DEAD_LETTER_QUEUE_SUFFIX)
        .map_or((entity.as_str(), false), |owner| (owner, true));
    let (owner, kind) = if let Some((topic, name)) = value.split_once(SUBSCRIPTION_PATH_SEGMENT) {
        let topic = primary_path(topic).ok_or(BrokerError::EntityMetadataCorrupt)?;
        let name = SubscriptionName::new(name).map_err(|_| BrokerError::EntityMetadataCorrupt)?;
        let owner = topic
            .subscription(&name)
            .map_err(|_| BrokerError::EntityMetadataCorrupt)?;
        if owner.as_str() != value {
            return Err(BrokerError::EntityMetadataCorrupt);
        }
        (owner, Kind::Subscription)
    } else {
        (
            primary_path(value).ok_or(BrokerError::EntityMetadataCorrupt)?,
            Kind::Queue,
        )
    };
    Ok((owner, kind, is_shadow))
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn stage_new_owner(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
        kind: Kind,
        batch: &mut WriteBatch,
    ) -> Result<(), BrokerError> {
        let key = keys::entity_metadata(namespace, owner);
        if self.store().get(&key)?.is_some()
            || self
                .store()
                .get(&keys::entity_metadata(
                    namespace,
                    &owner.dead_letter_queue()?,
                ))?
                .is_some()
        {
            return Err(BrokerError::EntityMetadataCorrupt);
        }
        batch.push_put(
            key,
            codec::encode(&Record {
                generation: 1,
                kind,
                retired: false,
            })?,
        );
        Ok(())
    }

    pub(super) fn require_live_owner(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
        kind: Kind,
    ) -> Result<(), BrokerError> {
        let raw = self
            .store()
            .get(&keys::entity_metadata(namespace, owner))?
            .ok_or(BrokerError::EntityMetadataCorrupt)?;
        decode_live(&raw, kind)?;
        if self
            .store()
            .get(&keys::entity_metadata(
                namespace,
                &owner.dead_letter_queue()?,
            ))?
            .is_some()
        {
            return Err(BrokerError::EntityMetadataCorrupt);
        }
        Ok(())
    }

    pub(super) fn require_queue_owner(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<(), BrokerError> {
        let (owner, kind, is_shadow) = queue_owner(entity)?;
        let shadow = owner.dead_letter_queue()?;
        if (is_shadow && self.raw_queue_config(namespace, &owner)?.is_none())
            || self
                .store()
                .get(&keys::topic_config(namespace, &owner))?
                .is_some()
            || self
                .store()
                .get(&keys::topic_config(namespace, &shadow))?
                .is_some()
        {
            return Err(BrokerError::EntityMetadataCorrupt);
        }
        self.require_live_owner(namespace, &owner, kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_head_requires_canonical_nonzero_generation_and_matching_kind() {
        for generation in [1, u64::MAX] {
            for kind in [Kind::Queue, Kind::Topic, Kind::Subscription] {
                let record = Record {
                    generation,
                    kind,
                    retired: false,
                };
                let raw = codec::encode(&record).expect("head");
                assert!(raw.len() <= MAX_HEAD_BYTES);
                assert_eq!(decode_live(&raw, kind), Ok(record));
                for wrong in [Kind::Queue, Kind::Topic, Kind::Subscription] {
                    if wrong != kind {
                        assert_eq!(
                            decode_live(&raw, wrong),
                            Err(BrokerError::EntityMetadataCorrupt)
                        );
                    }
                }
                let mut trailing = raw;
                trailing.push(0);
                assert_eq!(
                    decode_live(&trailing, kind),
                    Err(BrokerError::EntityMetadataCorrupt)
                );
            }
        }
        for record in [
            Record {
                generation: 0,
                kind: Kind::Queue,
                retired: false,
            },
            Record {
                generation: 1,
                kind: Kind::Queue,
                retired: true,
            },
        ] {
            assert_eq!(
                decode_live(&codec::encode(&record).expect("head"), Kind::Queue),
                Err(BrokerError::EntityMetadataCorrupt)
            );
        }
        for raw in [
            vec![],
            vec![1, 1, 3, 0],
            vec![1, 0x81, 0, 0, 0],
            vec![0; MAX_HEAD_BYTES + 1],
        ] {
            assert_eq!(
                decode_live(&raw, Kind::Queue),
                Err(BrokerError::EntityMetadataCorrupt)
            );
        }
    }

    #[test]
    fn queue_owner_rejects_nested_reserved_paths_but_preserves_maximum_components() {
        for value in [
            "q/$deadletterqueue/$deadletterqueue",
            "t/subscriptions",
            "t/subscriptions/s/subscriptions/x",
            "t/$management/subscriptions/s",
            "t/subscriptions/",
            "t/subscriptions/s/$management",
        ] {
            let entity = EntityPath::from_internal(value).expect("typed short path");
            assert_eq!(
                queue_owner(&entity),
                Err(BrokerError::EntityMetadataCorrupt)
            );
        }
        let topic = EntityPath::new("t".repeat(crate::MAX_ENTITY_PATH_BYTES)).expect("topic");
        let child = topic
            .subscription(
                &SubscriptionName::new("s".repeat(crate::MAX_SUBSCRIPTION_NAME_CHARACTERS))
                    .expect("name"),
            )
            .expect("child");
        for (owner, kind) in [(topic, Kind::Queue), (child, Kind::Subscription)] {
            assert_eq!(queue_owner(&owner), Ok((owner.clone(), kind, false)));
            assert_eq!(
                queue_owner(&owner.dead_letter_queue().expect("shadow")),
                Ok((owner, kind, true))
            );
        }
    }
}
