use std::collections::BTreeMap;

use crate::{
    CommittedCheckpoint, DEAD_LETTER_QUEUE_SUFFIX, EntityIncarnationKind, MAX_SEQUENCE_NUMBER,
};

use super::{CommittedImageValidationError as Error, Result, Rows, keys::Scope};

pub(super) fn validate(rows: &Rows<'_>, checkpoint: &CommittedCheckpoint) -> Result<usize> {
    let primary_queues = metadata(rows)?;
    clock(rows, checkpoint, primary_queues)?;
    messages(rows)?;
    history(rows)?;
    Ok(primary_queues)
}

fn metadata(rows: &Rows<'_>) -> Result<usize> {
    for incarnation in rows.incarnations.values() {
        if incarnation.generation() == 0 {
            return Err(Error::InvalidRecord);
        }
        if incarnation.generation() != 1
            || incarnation.kind() != EntityIncarnationKind::Queue
            || incarnation.is_retired()
        {
            return Err(Error::UnsupportedProfile);
        }
    }
    let mut primary_queues = 0;
    for (&scope, config) in &rows.configs {
        if scope.is_shadow() {
            let Some(parent) = scope.entity.strip_suffix(DEAD_LETTER_QUEUE_SUFFIX) else {
                return Err(Error::InconsistentMetadata);
            };
            let primary = Scope {
                namespace: scope.namespace,
                entity: parent,
            };
            if !primary.is_primary()
                || rows
                    .configs
                    .get(&primary)
                    .map(|config| config.dead_letter_shadow())
                    != Some(*config)
            {
                return Err(Error::InconsistentMetadata);
            }
            continue;
        }
        if !scope.is_primary() {
            return Err(Error::UnsupportedProfile);
        }
        let shadow_name = format!("{}{DEAD_LETTER_QUEUE_SUFFIX}", scope.entity);
        let shadow = Scope {
            namespace: scope.namespace,
            entity: &shadow_name,
        };
        if rows.configs.get(&shadow) != Some(&config.dead_letter_shadow()) {
            return Err(Error::InconsistentMetadata);
        }
        let incarnation = rows
            .incarnations
            .get(&scope)
            .ok_or(Error::InconsistentMetadata)?;
        if incarnation.generation() != 1
            || incarnation.kind() != EntityIncarnationKind::Queue
            || incarnation.is_retired()
        {
            return Err(Error::InconsistentMetadata);
        }
        primary_queues += 1;
    }
    if rows.incarnations.len() != primary_queues {
        return Err(Error::InconsistentMetadata);
    }
    for (&scope, counters) in &rows.counters {
        if counters.next_lock_token > 1 {
            return Err(Error::UnsupportedProfile);
        }
        if !scope.is_primary()
            || !rows.configs.contains_key(&scope)
            || counters.next_sequence == 0
            || counters.next_sequence > MAX_SEQUENCE_NUMBER + 1
            || counters.next_lock_token != 1
        {
            return Err(Error::InconsistentMetadata);
        }
    }
    Ok(primary_queues)
}

fn clock(rows: &Rows<'_>, checkpoint: &CommittedCheckpoint, primary_queues: usize) -> Result<()> {
    if checkpoint.last().is_none() && (primary_queues != 0 || rows.clock.is_some()) {
        return Err(Error::InvalidClock);
    }
    if primary_queues == 0 {
        if rows.clock.is_some() {
            return Err(Error::InvalidClock);
        }
    } else if rows.clock.is_none() {
        return Err(Error::InvalidClock);
    }
    if rows
        .clock
        .is_some_and(|clock| clock > checkpoint.highest_timestamp().as_millis())
    {
        return Err(Error::InvalidClock);
    }
    Ok(())
}

fn messages(rows: &Rows<'_>) -> Result<()> {
    let mut previous: BTreeMap<Scope<'_>, u64> = BTreeMap::new();
    for (&(scope, sequence), message) in &rows.messages {
        if !scope.is_primary() {
            return Err(Error::UnsupportedProfile);
        }
        let config = rows.configs.get(&scope).ok_or(Error::InconsistentMessage)?;
        let counters = rows
            .counters
            .get(&scope)
            .ok_or(Error::InconsistentMetadata)?;
        if sequence >= counters.next_sequence
            || message.body.len() > config.max_message_bytes
            || config.requires_session != message.session_id.is_some()
            || rows.clock.is_none_or(|clock| message.enqueued_at > clock)
            || previous
                .get(&scope)
                .is_some_and(|time| *time > message.enqueued_at)
        {
            return Err(Error::InconsistentMessage);
        }
        previous.insert(scope, message.enqueued_at);
        if let Some(ceiling) = config.default_time_to_live_millis
            && message
                .expires_at
                .is_none_or(|deadline| deadline > message.enqueued_at.saturating_add(ceiling))
        {
            return Err(Error::InconsistentMessage);
        }
        if rows.ready.get(&(scope, sequence)) != Some(&message.session_id)
            || rows.expiry.get(&(scope, sequence)).copied() != message.expires_at
        {
            return Err(Error::InconsistentIndex);
        }
    }
    if rows.ready.len() != rows.messages.len()
        || rows.expiry.len()
            != rows
                .messages
                .values()
                .filter(|message| message.expires_at.is_some())
                .count()
    {
        return Err(Error::InconsistentIndex);
    }
    Ok(())
}

fn history(rows: &Rows<'_>) -> Result<()> {
    let mut latest = BTreeMap::new();
    for (&(scope, _sequence), message) in &rows.messages {
        let config = rows.configs.get(&scope).ok_or(Error::InconsistentHistory)?;
        if config.requires_duplicate_detection && !message.message_id.is_empty() {
            if latest
                .get(&(scope, message.message_id))
                .is_some_and(|deadline| *deadline > message.enqueued_at)
            {
                return Err(Error::InconsistentHistory);
            }
            // Ascending sequence order deliberately chooses the final retained
            // send, not an arbitrary same-timestamp message.
            latest.insert(
                (scope, message.message_id),
                message
                    .enqueued_at
                    .saturating_add(config.duplicate_detection_history_time_window_millis),
            );
        }
    }
    if latest != rows.history || latest != rows.history_expiry {
        return Err(Error::InconsistentHistory);
    }
    // Histories whose deadline is behind the checkpoint are valid: this role
    // has no timer or history-expiration command that would remove them.
    Ok(())
}
