//! Retained allocation claims, not deleted-history or reachability evidence.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use crate::{DEAD_LETTER_QUEUE_SUFFIX, MessageState, keys::StateKey};

use super::{Facts, Scope, SnapshotCompleteStateError, StateValue};

enum SequenceClaim<'a> {
    // An ordinary queue (including its DLQ) or a topic placeholder.
    Exclusive,
    // One retained copy per subscription family; its DLQ is the same family.
    Families(BTreeSet<&'a str>),
}

pub(super) fn validate(facts: &Facts<'_>) -> Result<(), SnapshotCompleteStateError> {
    let mut sequences = BTreeMap::<(Scope, u64), SequenceClaim<'_>>::new();
    let mut tokens = BTreeSet::new();
    // Only primary 03/08 rows claim identifiers, in original input order.
    // Each message's sequence frontier/claim precedes its retained token.
    for row in &facts.rows {
        let (namespace, entity) = row.value.key.scope();
        let endpoint = (namespace.clone(), entity.clone());
        let token = match (&row.value.key, &row.value.value) {
            (StateKey::Message(_, _, sequence), StateValue::Message(record)) => {
                let owner = facts
                    .owners
                    .get(&endpoint)
                    .ok_or_else(|| inconsistent(row.ordinal, "message has no allocation owner"))?;
                let next = facts
                    .counters
                    .get(&owner.value.sequence_owner)
                    .map_or(1, |counter| counter.value.next_sequence);
                let sequence = sequence.as_u64();
                if sequence == 0 || sequence >= next {
                    return Err(inconsistent(
                        row.ordinal,
                        "message sequence is not positive and below its owner frontier",
                    ));
                }
                let family = entity
                    .as_str()
                    .strip_suffix(DEAD_LETTER_QUEUE_SUFFIX)
                    .unwrap_or(entity.as_str());
                match sequences.entry((owner.value.sequence_owner.clone(), sequence)) {
                    Entry::Vacant(entry) => {
                        entry.insert(if owner.value.subscription {
                            SequenceClaim::Families(BTreeSet::from([family]))
                        } else {
                            SequenceClaim::Exclusive
                        });
                    }
                    Entry::Occupied(mut entry) => {
                        let distinct_family = match entry.get_mut() {
                            SequenceClaim::Families(families) if owner.value.subscription => {
                                families.insert(family)
                            }
                            _ => false,
                        };
                        if !distinct_family {
                            return Err(inconsistent(
                                row.ordinal,
                                "message sequence aliases another retained allocation claim",
                            ));
                        }
                    }
                }
                match &record.state {
                    MessageState::Locked { token, .. } => Some(*token),
                    _ => None,
                }
            }
            (StateKey::Session(..), StateValue::Session(record)) => {
                record.lock.map(|lock| lock.token)
            }
            _ => None,
        };
        if let Some(token) = token {
            let next = facts
                .counters
                .get(&endpoint)
                .map_or(1, |counter| counter.value.next_lock_token);
            let token = token.as_u64();
            if token == 0 || token >= next {
                return Err(inconsistent(
                    row.ordinal,
                    "retained lock token is not positive and below its endpoint frontier",
                ));
            }
            if !tokens.insert((endpoint, token)) {
                return Err(inconsistent(
                    row.ordinal,
                    "lock token aliases another retained endpoint hold",
                ));
            }
        }
    }
    Ok(())
}

fn inconsistent(row: usize, detail: &'static str) -> SnapshotCompleteStateError {
    SnapshotCompleteStateError::InconsistentAllocation { row, detail }
}
