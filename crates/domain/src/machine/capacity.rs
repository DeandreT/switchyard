//! Owned preparation over a validated, owner-local physical image. The view is
//! read-only; all lifecycle decisions still run through the ordinary dispatcher.
//! No public constructor selects this context on a normal StateMachine.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use storage::{Key, MemoryStore, Mutation, StateStore, Value, WriteBatch};

use super::{PreparationFailure, StateMachine};
use crate::{
    BrokerError, CommandKind, CommandOutcome, DurableProposalAuthority, EntityBinding,
    EntityBindingKind, EntityPath, MessageRecord, MessageState, QueueCapacity, QueueConfig,
    SequenceNumber, Timestamp,
    capacity_fence::{self, CapacityIntent},
    codec, keys,
    retained_accounting::{
        self as accounting, CapacityFramingBudget, CapacityOwner, MessageReservation,
        ReservationDelta, RetainedUsage, usage_codec,
    },
};

type Rows = BTreeMap<Key, Value>;
type Groups = BTreeMap<(EntityPath, SequenceNumber), Vec<(Key, Value)>>;

fn corrupt() -> BrokerError {
    BrokerError::CapacityStateCorrupt
}
fn checked<T>(value: Result<T, accounting::AccountingError>) -> Result<T, BrokerError> {
    value.map_err(|_| corrupt())
}

/// A page bound, not a bound on total validation work or raw-value allocation.
pub(crate) fn scan<S: StateStore>(
    store: &S,
    prefix: &[u8],
) -> Result<Vec<(Key, Value)>, BrokerError> {
    let mut result = Vec::new();
    let mut cursor = prefix.to_vec();
    loop {
        let page = store.scan_from(prefix, &cursor, 32)?;
        if page.len() > 32 {
            return Err(corrupt());
        }
        if page.is_empty() {
            return Ok(result);
        }
        let mut previous: Option<&[u8]> = None;
        for (key, _) in &page {
            if !key.starts_with(prefix)
                || key.as_slice() < cursor.as_slice()
                || previous.is_some_and(|old| old >= key.as_slice())
            {
                return Err(corrupt());
            }
            previous = Some(key);
        }
        cursor = page.last().expect("nonempty").0.clone();
        cursor.push(0);
        result.extend(page);
    }
}

#[derive(Debug)]
pub(crate) struct OwnerImage {
    pub(crate) owner: CapacityOwner,
    pub(crate) config: QueueConfig,
    pub(crate) usage: RetainedUsage,
    rows: Rows,
    virtual_rows: Rows,
    reservations: BTreeMap<SequenceNumber, MessageReservation>,
}

impl OwnerImage {
    pub(crate) fn load<S: StateStore>(
        store: &S,
        owner: &CapacityOwner,
    ) -> Result<Self, BrokerError> {
        let head_key = keys::entity_metadata(owner.namespace(), owner.parent());
        let head = store.get(&head_key)?.ok_or_else(corrupt)?;
        let generation =
            super::decode_catalog_owner(&head, EntityBindingKind::Queue).map_err(|_| corrupt())?;
        if generation != owner.generation() {
            return Err(BrokerError::StaleEntityBinding);
        }
        if store
            .get(&keys::entity_metadata(owner.namespace(), owner.shadow()))?
            .is_some()
            || store
                .get(&keys::topic_config(owner.namespace(), owner.parent()))?
                .is_some()
            || store
                .get(&keys::topic_config(owner.namespace(), owner.shadow()))?
                .is_some()
        {
            return Err(corrupt());
        }
        let mut virtual_rows = Rows::from([(head_key, head)]);
        let mut configs = Vec::new();
        let mut rows = Rows::new();
        for endpoint in [owner.parent(), owner.shadow()] {
            let key = keys::queue_config(owner.namespace(), endpoint);
            let raw = store.get(&key)?.ok_or_else(corrupt)?;
            let config = checked(capacity_fence::decode_profile(owner, endpoint, &raw))?;
            virtual_rows.insert(key.clone(), codec::queue::encode(&config)?);
            configs.push(config);
            // Exact terminated endpoint scope. Never read legacy rows as data.
            for tag in 2..=16 {
                let mut prefix = key.clone();
                prefix[0] = tag;
                if !store.scan_prefix(&prefix, 1)?.is_empty() {
                    return Err(corrupt());
                }
                prefix.insert(0, capacity_fence::DATA_TAG);
                for (physical, value) in scan(store, &prefix)? {
                    let original = checked(capacity_fence::original_key(&physical))?;
                    virtual_rows.insert(original.to_vec(), value.clone());
                    rows.insert(physical, value);
                }
            }
        }
        let config = configs[0];
        if configs[1] != config.dead_letter_shadow() {
            return Err(corrupt());
        }
        // No shadow ledger, even when it is otherwise well-formed.
        let mut shadow_usage = keys::queue_config(owner.namespace(), owner.shadow());
        shadow_usage[0] = accounting::keys::USAGE_TAG;
        if store.get(&shadow_usage)?.is_some() {
            return Err(corrupt());
        }
        let raw_usage = store
            .get(&accounting::keys::usage(owner))?
            .ok_or(BrokerError::CapacityUsageMissing)?;
        let usage = checked(usage_codec::decode(owner, &raw_usage))?;
        let (actual, reservations) = validate_rows(owner, &virtual_rows, &rows)?;
        if usage != actual {
            return Err(corrupt());
        }
        let QueueCapacity::Finite { max_size_bytes } = config.capacity else {
            return Err(corrupt());
        };
        if checked(usage.committed_bytes())? > max_size_bytes {
            return Err(corrupt());
        }
        Ok(Self {
            owner: owner.clone(),
            config,
            usage,
            rows,
            virtual_rows,
            reservations,
        })
    }

    pub(crate) fn binding(&self, binding: &EntityBinding) -> Result<(), BrokerError> {
        if binding.namespace() != self.owner.namespace()
            || binding.owner() != self.owner.parent()
            || binding.kind() != EntityBindingKind::Queue
            || (binding.target() != self.owner.parent() && binding.target() != self.owner.shadow())
        {
            return Err(BrokerError::InvalidEntityBinding);
        }
        if binding.generation() != self.owner.generation() {
            return Err(BrokerError::StaleEntityBinding);
        }
        Ok(())
    }
}

fn groups(owner: &CapacityOwner, rows: &Rows) -> Result<Groups, BrokerError> {
    let mut groups = Groups::new();
    for (physical, value) in rows {
        let key = checked(capacity_fence::original_key(physical))?;
        if key[0] == 2 {
            continue;
        }
        let state = keys::state_parts(key).ok_or_else(corrupt)?;
        let (namespace, endpoint) = state.scope();
        if namespace != owner.namespace()
            || (endpoint != owner.parent() && endpoint != owner.shadow())
        {
            return Err(corrupt());
        }
        let sequence = keys::trailing_sequence(key).ok_or_else(corrupt)?;
        groups
            .entry((endpoint.clone(), sequence))
            .or_default()
            .push((physical.clone(), value.clone()));
    }
    Ok(groups)
}

fn validate_rows(
    owner: &CapacityOwner,
    catalog: &Rows,
    rows: &Rows,
) -> Result<(RetainedUsage, BTreeMap<SequenceNumber, MessageReservation>), BrokerError> {
    let mut actual = 0u64;
    let mut reserved = 0u64;
    let mut reservations = BTreeMap::new();
    for ((_, sequence), group) in groups(owner, rows)? {
        let value = group
            .iter()
            .find(|(key, _)| key.get(1) == Some(&3))
            .map(|(_, value)| value)
            .ok_or_else(corrupt)?;
        let version = *value.first().ok_or_else(corrupt)?;
        if !(1..=3).contains(&version) {
            return Err(BrokerError::CapacityMessageUnsupported { version });
        }
        let reservation = checked(accounting::reserve_message(
            owner,
            &group,
            CapacityFramingBudget::IsolatedKeyspaceV1,
        ))?;
        actual = actual
            .checked_add(reservation.retained_bytes)
            .ok_or_else(corrupt)?;
        reserved = reserved
            .checked_add(reservation.reserved_growth_bytes)
            .ok_or_else(corrupt)?;
        if reservations.insert(sequence, reservation).is_some() {
            return Err(corrupt());
        }
    }
    // Reuse the real catalog/forward/reverse/allocation validators on a private
    // logical view. No generic validator is taught to accept owned persistence.
    let mut view = catalog.clone();
    view.retain(|key, _| !matches!(key.first(), Some(2..=13)));
    for (physical, value) in rows {
        view.insert(
            checked(capacity_fence::original_key(physical))?.to_vec(),
            value.clone(),
        );
    }
    view.insert(keys::clock(), codec::encode(&Timestamp::UNIX_EPOCH)?);
    crate::snapshot_validation::validate_complete_state(&view.into_iter().collect::<Vec<_>>())
        .map_err(|_| corrupt())?;
    let usage = checked(RetainedUsage::new(
        owner.generation(),
        actual,
        reserved,
        u64::try_from(reservations.len()).map_err(|_| corrupt())?,
    ))?;
    Ok((usage, reservations))
}

pub(crate) fn record_rows(
    owner: &CapacityOwner,
    endpoint: &EntityPath,
    record: &MessageRecord,
) -> Result<Vec<(Key, Value)>, BrokerError> {
    let n = owner.namespace();
    let s = record.sequence;
    let mut result = vec![(
        checked(capacity_fence::data_key(&keys::message(n, endpoint, s)))?,
        codec::message::encode(record)?,
    )];
    let mut indexes = Vec::new();
    match record.state {
        MessageState::Ready => {
            indexes.push(keys::ready(n, endpoint, s));
            if let Some(time) = record.expires_at {
                indexes.push(keys::expiry(n, endpoint, time, s));
            }
        }
        MessageState::Deferred => indexes.push(keys::deferred(n, endpoint, s)),
        MessageState::Scheduled => indexes.push(keys::scheduled(
            n,
            endpoint,
            record.scheduled_enqueue_at.ok_or_else(corrupt)?,
            s,
        )),
        MessageState::Locked { locked_until, .. } => {
            indexes.push(keys::lock(n, endpoint, locked_until, s))
        }
    }
    for key in indexes {
        result.push((checked(capacity_fence::data_key(&key))?, Vec::new()));
    }
    Ok(result)
}

#[derive(Clone, Debug)]
pub(crate) struct TransitionContext {
    owner: CapacityOwner,
    total: u64,
    limit: u64,
    original: BTreeMap<SequenceNumber, MessageReservation>,
    proof: Arc<Mutex<Proof>>,
}

#[derive(Debug, Default)]
struct Proof {
    admitted: BTreeMap<SequenceNumber, u64>,
    activations: BTreeMap<SequenceNumber, SequenceNumber>,
    healthy_refusal: Option<(u64, u64)>,
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn prove_capacity_rewrite(
        &self,
        endpoint: &EntityPath,
        record: &MessageRecord,
    ) -> Result<(), BrokerError> {
        let Some(context) = &self.capacity else {
            return Ok(());
        };
        let rows = record_rows(&context.owner, endpoint, record)?;
        let candidate = checked(accounting::reserve_message(
            &context.owner,
            &rows,
            CapacityFramingBudget::IsolatedKeyspaceV1,
        ))?;
        let old = context.original.get(&record.sequence).ok_or_else(corrupt)?;
        let committed = context
            .total
            .checked_sub(old.ceiling_bytes)
            .and_then(|value| value.checked_add(candidate.ceiling_bytes))
            .ok_or_else(corrupt)?;
        let mut proof = context.proof.lock().map_err(|_| corrupt())?;
        if committed > context.limit {
            proof.healthy_refusal = Some((committed, context.limit));
            return Err(BrokerError::RetainedCapacityExceeded {
                committed,
                limit: context.limit,
            });
        }
        proof
            .admitted
            .insert(record.sequence, candidate.ceiling_bytes);
        Ok(())
    }

    pub(super) fn note_capacity_activation(
        &self,
        old: SequenceNumber,
        new: SequenceNumber,
    ) -> Result<(), BrokerError> {
        if let Some(context) = &self.capacity
            && context
                .proof
                .lock()
                .map_err(|_| corrupt())?
                .activations
                .insert(new, old)
                .is_some()
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

fn allowed(kind: &CommandKind) -> bool {
    matches!(
        kind,
        CommandKind::Send { .. }
            | CommandKind::SendBatch { .. }
            | CommandKind::SendContent { .. }
            | CommandKind::SendContentBatch { .. }
            | CommandKind::CancelScheduled { .. }
            | CommandKind::ActivateScheduled
            | CommandKind::Peek { .. }
            | CommandKind::Receive { .. }
            | CommandKind::ReceiveDeferred { .. }
            | CommandKind::Complete { .. }
            | CommandKind::RenewLock { .. }
            | CommandKind::Abandon { .. }
            | CommandKind::AbandonWithPatch { .. }
            | CommandKind::Defer { .. }
            | CommandKind::DeferWithPatch { .. }
            | CommandKind::DeadLetter { .. }
            | CommandKind::DeadLetterWithPatch { .. }
            | CommandKind::ExpireLocks
            | CommandKind::ExpireMessages
    )
}

pub(crate) fn check_kind(kind: &CommandKind) -> Result<(), BrokerError> {
    if allowed(kind) {
        Ok(())
    } else {
        Err(BrokerError::CapacityStateUnsupported)
    }
}

pub(crate) fn prepare<S: StateStore>(
    store: &S,
    intent: &CapacityIntent,
) -> Result<(CommandOutcome, WriteBatch), PreparationFailure> {
    // Recheck canonical typed framing, including the total outer byte bound.
    intent.encode().map_err(|_| corrupt())?;
    let command = intent.original().command();
    if !allowed(&command.kind) {
        return Err(BrokerError::CapacityStateUnsupported.into());
    }
    let image = OwnerImage::load(store, intent.owner())?;
    if let DurableProposalAuthority::Bound(binding) = intent.original().authority() {
        image.binding(binding)?;
    }
    let QueueCapacity::Finite {
        max_size_bytes: limit,
    } = image.config.capacity
    else {
        return Err(corrupt().into());
    };
    let context = TransitionContext {
        owner: image.owner.clone(),
        total: checked(image.usage.committed_bytes())?,
        limit,
        original: image.reservations.clone(),
        proof: Arc::new(Mutex::new(Proof::default())),
    };
    // Only now may preparation observe stored Clock. This view contains no
    // other owner's records, and never falls back to raw legacy data.
    let mut view = image.virtual_rows.clone();
    let clock = store.get(&keys::clock())?.ok_or_else(corrupt)?;
    let timestamp: Timestamp = codec::decode(&clock)?;
    if codec::encode(&timestamp)? != clock {
        return Err(corrupt().into());
    }
    view.insert(keys::clock(), clock);
    let memory = MemoryStore::default();
    let mut seed = WriteBatch::default();
    for (key, value) in view {
        seed.push_put(key, value);
    }
    memory.apply(seed)?;
    let machine = StateMachine {
        store: memory,
        capacity: Some(context.clone()),
    };
    let prepared = machine.prepare_unfenced(command);
    let (outcome, logical) = match prepared {
        Ok(value) => value,
        Err(error) => {
            if let Some((committed, limit)) =
                context.proof.lock().map_err(|_| corrupt())?.healthy_refusal
            {
                return Err(PreparationFailure::HealthyRetainedCapacity { committed, limit });
            }
            return Err(error);
        }
    };
    if logical.is_empty() {
        return Ok((outcome, logical));
    }
    let mut batch = WriteBatch::default();
    let mut after = image.rows.clone();
    let mut clock = None;
    let mut counters = Vec::new();
    for mutation in logical.into_mutations() {
        let (key, value) = match mutation {
            Mutation::Put { key, value } => (key, Some(value)),
            Mutation::Delete { key } => (key, None),
        };
        if key == keys::clock() {
            clock = value;
            continue;
        }
        let physical = checked(capacity_fence::data_key(&key))?;
        // Every original operation, even an overwritten one, is scoped.
        let scope = if key[0] == 2 {
            keys::catalog_entity_parts(&key)
        } else {
            keys::state_parts(&key).map(|state| {
                let (n, e) = state.scope();
                (n.clone(), e.clone())
            })
        };
        let (namespace, endpoint) = scope.ok_or_else(corrupt)?;
        if &namespace != image.owner.namespace()
            || (&endpoint != image.owner.parent() && &endpoint != image.owner.shadow())
        {
            return Err(corrupt().into());
        }
        let mutation = if let Some(value) = value {
            after.insert(physical.clone(), value.clone());
            Mutation::Put {
                key: physical,
                value,
            }
        } else {
            after.remove(&physical);
            Mutation::Delete { key: physical }
        };
        if key[0] == 2 {
            counters.push(mutation);
        } else {
            match mutation {
                Mutation::Put { key, value } => batch.push_put(key, value),
                Mutation::Delete { key } => batch.push_delete(key),
            }
        }
    }
    let (new_usage, reservations) = validate_rows(&image.owner, &image.virtual_rows, &after)?;
    let proof = context.proof.lock().map_err(|_| corrupt())?;
    let ingress = matches!(
        command.kind,
        CommandKind::Send { .. }
            | CommandKind::SendBatch { .. }
            | CommandKind::SendContent { .. }
            | CommandKind::SendContentBatch { .. }
    );
    for (sequence, reservation) in &reservations {
        let old_sequence = proof.activations.get(sequence).unwrap_or(sequence);
        if let Some(original) = image.reservations.get(old_sequence) {
            let ceiling = proof
                .admitted
                .get(old_sequence)
                .copied()
                .unwrap_or(original.ceiling_bytes);
            if reservation.ceiling_bytes > ceiling {
                return Err(corrupt().into());
            }
        } else if !ingress {
            return Err(corrupt().into());
        }
    }
    let committed = checked(new_usage.committed_bytes())?;
    if committed > limit {
        if ingress {
            return Err(PreparationFailure::HealthyRetainedCapacity { committed, limit });
        }
        return Err(corrupt().into());
    }
    let before = image
        .rows
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    let delta = checked(accounting::calculate_batch(
        std::slice::from_ref(&image.owner),
        &before,
        &batch,
    ))?;
    let predicted = checked(image.usage.apply(
        &delta[0],
        ReservationDelta {
            removed: image.usage.reserved_growth_bytes(),
            added: new_usage.reserved_growth_bytes(),
        },
    ))?;
    if predicted != new_usage {
        return Err(corrupt().into());
    }
    // Only the fully proved candidate gets publishable counter/usage/Clock rows.
    for mutation in counters {
        match mutation {
            Mutation::Put { key, value } => batch.push_put(key, value),
            Mutation::Delete { key } => batch.push_delete(key),
        }
    }
    batch.push_put(
        accounting::keys::usage(&image.owner),
        checked(usage_codec::encode(&image.owner, new_usage))?,
    );
    batch.push_put(keys::clock(), clock.ok_or_else(corrupt)?);
    Ok((outcome, batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Command, DurableProposal, LockToken, MessageContent, MessageEnvelope, MessageRecordFormat,
        NamespaceName, RawMessage, ReceiveMode,
        capacity_conservation::{CapacityHarness, TestGenesis},
    };
    use storage::{StorageError, StoreSnapshot};

    #[derive(Clone)]
    struct NoClock(MemoryStore);
    impl StateStore for NoClock {
        fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
            assert_ne!(key, keys::clock(), "refusal must precede stored Clock");
            self.0.get(key)
        }
        fn scan_from(
            &self,
            p: &[u8],
            s: &[u8],
            l: usize,
        ) -> Result<Vec<(Key, Value)>, StorageError> {
            self.0.scan_from(p, s, l)
        }
        fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
            self.0.snapshot()
        }
        fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
            panic!("refused preparation cannot apply")
        }
    }
    fn fixture() -> (MemoryStore, TestGenesis) {
        let g = TestGenesis::new(
            NamespaceName::new("tenant").unwrap(),
            EntityPath::new("orders").unwrap(),
            QueueConfig {
                capacity: QueueCapacity::Finite {
                    max_size_bytes: 1024 * 1024,
                },
                ..QueueConfig::default()
            },
        )
        .unwrap();
        let s = MemoryStore::default();
        g.initialize(&s).unwrap();
        (s, g)
    }
    fn intent(owner: CapacityOwner, kind: CommandKind) -> CapacityIntent {
        let command = Command::new(
            owner.namespace().clone(),
            owner.parent().clone(),
            Timestamp::from_millis(2),
            kind,
        );
        CapacityIntent::capture(owner, DurableProposal::unbound(command)).unwrap()
    }
    #[test]
    fn v4_every_owned_operation_refuses_before_clock_or_refund() {
        for text_ids in [false, true] {
            for removals in [Vec::new(), vec!["old".to_owned()]] {
                let (s, g) = fixture();
                let mut actor = CapacityHarness::open(s.clone(), g.clone()).unwrap();
                actor
                    .apply(&intent(
                        g.owner().clone(),
                        CommandKind::SendContent {
                            content: MessageContent::Amqp(vec![0, 0x53, 0x75, 0xa0, 1, 7]),
                        },
                    ))
                    .unwrap();
                drop(actor);
                let key = capacity_fence::data_key(&keys::message(
                    g.owner().namespace(),
                    g.owner().parent(),
                    SequenceNumber::new(1),
                ))
                .unwrap();
                let mut record = codec::message::decode(&s.get(&key).unwrap().unwrap()).unwrap();
                record.format = MessageRecordFormat::V4 { text_ids };
                record.envelope = Some(record.envelope.unwrap().with_removals(removals));
                s.apply(WriteBatch::default().put(key, codec::message::encode(&record).unwrap()))
                    .unwrap();
                let before = s.snapshot().unwrap();
                let sequence = SequenceNumber::new(1);
                let lock_token = LockToken::new(1);
                for kind in [
                    CommandKind::SendContent {
                        content: MessageContent::Raw(RawMessage::default()),
                    },
                    CommandKind::SendBatch { messages: vec![] },
                    CommandKind::SendContentBatch { messages: vec![] },
                    CommandKind::Receive {
                        mode: ReceiveMode::PeekLock,
                        lock_duration_millis: None,
                        session: None,
                    },
                    CommandKind::Receive {
                        mode: ReceiveMode::ReceiveAndDelete,
                        lock_duration_millis: None,
                        session: None,
                    },
                    CommandKind::Peek {
                        from_sequence: sequence,
                        max_messages: 1,
                        session: None,
                    },
                    CommandKind::ReceiveDeferred {
                        sequences: vec![sequence],
                        mode: ReceiveMode::ReceiveAndDelete,
                        lock_duration_millis: None,
                        session: None,
                    },
                    CommandKind::Complete {
                        sequence,
                        lock_token,
                    },
                    CommandKind::RenewLock {
                        sequence,
                        lock_token,
                        lock_duration_millis: None,
                    },
                    CommandKind::Abandon {
                        sequence,
                        lock_token,
                        replacement_envelope: None,
                    },
                    CommandKind::AbandonWithPatch {
                        sequence,
                        lock_token,
                        patch: vec![],
                    },
                    CommandKind::Defer {
                        sequence,
                        lock_token,
                        replacement_envelope: None,
                    },
                    CommandKind::DeferWithPatch {
                        sequence,
                        lock_token,
                        patch: vec![],
                    },
                    CommandKind::DeadLetter {
                        sequence,
                        lock_token,
                        replacement_envelope: None,
                        reason: "r".into(),
                        description: "d".into(),
                    },
                    CommandKind::DeadLetterWithPatch {
                        sequence,
                        lock_token,
                        patch: vec![],
                        reason: "r".into(),
                        description: "d".into(),
                    },
                    CommandKind::CancelScheduled {
                        sequences: vec![sequence],
                    },
                    CommandKind::ActivateScheduled,
                    CommandKind::ExpireLocks,
                    CommandKind::ExpireMessages,
                ] {
                    assert!(matches!(
                        prepare(&NoClock(s.clone()), &intent(g.owner().clone(), kind)),
                        Err(PreparationFailure::Ordinary(
                            BrokerError::CapacityMessageUnsupported { version: 4 }
                        ))
                    ));
                    assert_eq!(s.snapshot().unwrap(), before);
                }
                // General codec corruption is distinct from the numerical policy.
                let envelope = record.envelope.take().unwrap();
                record.envelope = Some(
                    MessageEnvelope::new(envelope.as_bytes().to_vec())
                        .with_overlay(vec![crate::OverlayProperty {
                            name: "same".into(),
                            value: vec![0x40],
                        }])
                        .with_removals(vec!["same".into()]),
                );
                assert!(codec::message::encode(&record).is_err());
            }
        }
    }
    #[test]
    fn stale_owner_and_missing_usage_do_not_observe_clock() {
        let (s, g) = fixture();
        let stale =
            CapacityOwner::new(g.owner().namespace().clone(), g.owner().parent().clone(), 2)
                .unwrap();
        assert!(matches!(
            prepare(
                &NoClock(s.clone()),
                &intent(stale, CommandKind::ExpireLocks)
            ),
            Err(PreparationFailure::Ordinary(
                BrokerError::StaleEntityBinding
            ))
        ));
        s.apply(WriteBatch::default().delete(accounting::keys::usage(g.owner())))
            .unwrap();
        assert!(matches!(
            prepare(
                &NoClock(s),
                &intent(g.owner().clone(), CommandKind::ExpireLocks)
            ),
            Err(PreparationFailure::Ordinary(
                BrokerError::CapacityUsageMissing
            ))
        ));
    }
}
