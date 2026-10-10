use crate::{
    DeliveryOrigin, EntityPath, MessageRecord, MessageState, NamespaceName,
    keys::{self, StateKey},
};

use super::{Facts, Located, Profile, SnapshotStateError, StateRow, StateValue};

pub(super) fn validate(facts: &Facts<'_>) -> Result<(), SnapshotStateError> {
    // Forward message checks precede reverse index checks, each in input order.
    for row in &facts.rows {
        let StateKey::Message(namespace, entity, sequence) = &row.value.key else {
            continue;
        };
        let record = message(row)?;
        if record.sequence != *sequence {
            return Err(inconsistent(
                row.ordinal,
                "message sequence does not match its key",
            ));
        }
        let owner = facts
            .owners
            .get(&(namespace.clone(), entity.clone()))
            .ok_or_else(|| inconsistent(row.ordinal, "message has no catalog endpoint"))?;
        match &owner.value.profile {
            Profile::Topic(_)
                if record.state != MessageState::Scheduled
                    || record.session_id.is_some()
                    || record.dead_letter.is_some() =>
            {
                return Err(inconsistent(
                    row.ordinal,
                    "topic owns only non-session scheduled placeholders",
                ));
            }
            Profile::Topic(_) => {}
            Profile::Queue(config) => {
                if config.requires_session != record.session_id.is_some() {
                    return Err(inconsistent(
                        row.ordinal,
                        "message session shape differs from its queue",
                    ));
                }
                if owner.value.shadow {
                    if record.dead_letter.is_none()
                        || record.expires_at.is_some()
                        || record.state == MessageState::Scheduled
                    {
                        return Err(inconsistent(
                            row.ordinal,
                            "dead-letter message has incompatible provenance or state",
                        ));
                    }
                } else if record.dead_letter.is_some() {
                    return Err(inconsistent(
                        row.ordinal,
                        "ordinary message carries dead-letter provenance",
                    ));
                }
                if owner.value.subscription && record.state == MessageState::Scheduled {
                    return Err(inconsistent(
                        row.ordinal,
                        "subscription cannot own a scheduled placeholder",
                    ));
                }
            }
        }
        for expected in expected_keys(namespace, entity, record, row.ordinal)? {
            if facts.row(&expected).is_none() {
                return Err(inconsistent(
                    row.ordinal,
                    "message is missing its exact index companion",
                ));
            }
        }
    }
    for row in &facts.rows {
        let sequence = match &row.value.key {
            StateKey::Ready(_, _, sequence)
            | StateKey::Lock(_, _, _, sequence)
            | StateKey::Expiry(_, _, _, sequence)
            | StateKey::Deferred(_, _, sequence)
            | StateKey::SessionReady(_, _, _, sequence)
            | StateKey::Scheduled(_, _, _, sequence) => *sequence,
            _ => continue,
        };
        let (namespace, entity) = row.value.key.scope();
        let primary = facts
            .row(&keys::message(namespace, entity, sequence))
            .ok_or_else(|| inconsistent(row.ordinal, "index has no message in its exact scope"))?;
        let record = message(primary)?;
        if !expected_keys(namespace, entity, record, primary.ordinal)?
            .iter()
            .any(|expected| expected.as_slice() == row.key)
        {
            return Err(inconsistent(
                row.ordinal,
                "index differs from its message state or identity",
            ));
        }
    }
    Ok(())
}

fn expected_keys(
    namespace: &NamespaceName,
    entity: &EntityPath,
    record: &MessageRecord,
    row: usize,
) -> Result<Vec<Vec<u8>>, SnapshotStateError> {
    let sequence = record.sequence;
    Ok(match &record.state {
        MessageState::Ready => {
            let ready = match &record.session_id {
                Some(session) => keys::session_ready(namespace, entity, session, sequence),
                None => keys::ready(namespace, entity, sequence),
            };
            let mut expected = vec![ready];
            if let Some(deadline) = record.expires_at {
                expected.push(keys::expiry(namespace, entity, deadline, sequence));
            }
            expected
        }
        MessageState::Locked {
            locked_until,
            origin,
            ..
        } => {
            if *origin == DeliveryOrigin::Scheduled {
                return Err(inconsistent(
                    row,
                    "scheduled origin cannot carry a delivery lock",
                ));
            }
            vec![keys::lock(namespace, entity, *locked_until, sequence)]
        }
        MessageState::Deferred => vec![keys::deferred(namespace, entity, sequence)],
        MessageState::Scheduled => {
            let enqueue_at = record.scheduled_enqueue_at.ok_or_else(|| {
                inconsistent(row, "scheduled message has no requested enqueue time")
            })?;
            vec![keys::scheduled(namespace, entity, enqueue_at, sequence)]
        }
    })
}

fn message<'a>(row: &'a Located<'_, StateRow>) -> Result<&'a MessageRecord, SnapshotStateError> {
    match &row.value.value {
        StateValue::Message(record) => Ok(record.as_ref()),
        _ => Err(inconsistent(
            row.ordinal,
            "message key has no decoded message",
        )),
    }
}

fn inconsistent(row: usize, detail: &'static str) -> SnapshotStateError {
    SnapshotStateError::InconsistentMessage { row, detail }
}
