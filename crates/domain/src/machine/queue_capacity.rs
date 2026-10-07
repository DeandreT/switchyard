use std::collections::{BTreeMap, BTreeSet};

use storage::{Mutation, StateStore, WriteBatch};

use crate::queue_capacity::{
    MessageCharge, QueueCapacityError, QueueCapacityMode, QueueCapacityUsage,
    RecordChargeObservation,
};
use crate::{
    BrokerError, DEAD_LETTER_QUEUE_SUFFIX, EntityIncarnation, EntityIncarnationKind, EntityPath,
    MAX_SEQUENCE_NUMBER, NamespaceName, QueueConfig, SequenceNumber, TopicConfig, codec, keys,
};

use super::StateMachine;

pub(super) const MAX_CAPACITY_MESSAGES: usize = 1_024;
const MAX_CAPACITY_EVENTS: usize = 2_048;
const MAX_CAPACITY_LEDGER_KEYS: usize = 2_048;
const MAX_CAPACITY_READS: usize = 4_096;
const MAX_CAPACITY_READ_KEY_BYTES: usize = 4 * 1024 * 1024;
const MAX_CAPACITY_READ_VALUE_BYTES: usize = 256 * 1024;
const MAX_PROFILE_RECORD_BYTES: usize = 4_096;
const MAX_SMALL_RECORD_BYTES: usize = 64;

fn corrupt(_: impl std::fmt::Debug) -> BrokerError {
    BrokerError::QueueCapacityCorrupt
}

fn proposed(error: QueueCapacityError) -> BrokerError {
    match error {
        QueueCapacityError::LimitExceeded | QueueCapacityError::ArithmeticOverflow => {
            BrokerError::QueueCapacityFull
        }
        QueueCapacityError::UnsupportedFiniteQueue => BrokerError::QueueCapacityNotSupported,
        _ => BrokerError::QueueCapacityCorrupt,
    }
}

#[derive(Default)]
struct ReadBudget {
    reads: usize,
    key_bytes: usize,
    value_bytes: usize,
}

impl ReadBudget {
    fn get<S: StateStore>(
        &mut self,
        store: &S,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, BrokerError> {
        self.reads = self
            .reads
            .checked_add(1)
            .filter(|value| *value <= MAX_CAPACITY_READS)
            .ok_or(BrokerError::QueueCapacityWorkLimitExceeded)?;
        self.key_bytes = self
            .key_bytes
            .checked_add(key.len())
            .filter(|value| *value <= MAX_CAPACITY_READ_KEY_BYTES)
            .ok_or(BrokerError::QueueCapacityWorkLimitExceeded)?;
        // StateStore materializes one value before this logical limit is known.
        let value = store.get(key)?;
        self.value_bytes = self
            .value_bytes
            .checked_add(value.as_ref().map_or(0, Vec::len))
            .filter(|value| *value <= MAX_CAPACITY_READ_VALUE_BYTES)
            .ok_or(BrokerError::QueueCapacityWorkLimitExceeded)?;
        Ok(value)
    }

    fn absent<S: StateStore>(&mut self, store: &S, key: &[u8]) -> Result<(), BrokerError> {
        if self.get(store, key)?.is_some() {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        Ok(())
    }
}

fn normalize_target(target: &EntityPath) -> Result<EntityPath, BrokerError> {
    EntityPath::new(target.as_str())?;
    if !target.is_dead_letter_queue() {
        return Ok(target.clone());
    }
    let end = target
        .as_str()
        .len()
        .checked_sub(DEAD_LETTER_QUEUE_SUFFIX.len())
        .ok_or(BrokerError::QueueCapacityCorrupt)?;
    let owner = EntityPath::new(&target.as_str()[..end])?;
    if owner.dead_letter_queue()? != *target || owner.is_dead_letter_queue() {
        return Err(BrokerError::QueueCapacityCorrupt);
    }
    Ok(owner)
}

fn incarnation<S: StateStore>(
    store: &S,
    budget: &mut ReadBudget,
    namespace: &NamespaceName,
    owner: &EntityPath,
) -> Result<EntityIncarnation, BrokerError> {
    let value = optional_incarnation(store, budget, namespace, owner)?
        .ok_or(BrokerError::QueueCapacityCorrupt)?;
    if value.is_retired() {
        return Err(BrokerError::QueueCapacityCorrupt);
    }
    Ok(value)
}

fn optional_incarnation<S: StateStore>(
    store: &S,
    budget: &mut ReadBudget,
    namespace: &NamespaceName,
    owner: &EntityPath,
) -> Result<Option<EntityIncarnation>, BrokerError> {
    let Some(bytes) = budget.get(store, &keys::entity_incarnation(namespace, owner))? else {
        return Ok(None);
    };
    if bytes.len() > MAX_SMALL_RECORD_BYTES {
        return Err(BrokerError::QueueCapacityCorrupt);
    }
    let value: EntityIncarnation = codec::decode(&bytes).map_err(corrupt)?;
    value.validate().map_err(corrupt)?;
    Ok(Some(value))
}

fn queue_config<S: StateStore>(
    store: &S,
    budget: &mut ReadBudget,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Result<QueueConfig, BrokerError> {
    let bytes = budget
        .get(store, &keys::queue_config(namespace, entity))?
        .ok_or(BrokerError::QueueCapacityCorrupt)?;
    if bytes.len() > MAX_PROFILE_RECORD_BYTES {
        return Err(BrokerError::QueueCapacityCorrupt);
    }
    QueueConfig::decode(&bytes)
        .map_err(corrupt)?
        .validate()
        .map_err(corrupt)
}

/// Validated primary ownership and mode, with runtime usage intentionally absent.
/// This proof supports opaque whole-owner deletion, not message admission.
#[derive(Clone)]
pub(super) struct QueueCapacityOwner {
    namespace: NamespaceName,
    owner: EntityPath,
    target: EntityPath,
    config: QueueConfig,
    generation: u64,
    mode: QueueCapacityMode,
}

impl QueueCapacityOwner {
    pub(super) fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }
    pub(super) fn owner(&self) -> &EntityPath {
        &self.owner
    }
    #[cfg(test)]
    pub(super) fn target(&self) -> &EntityPath {
        &self.target
    }
    #[cfg(test)]
    pub(super) fn kind(&self) -> EntityIncarnationKind {
        EntityIncarnationKind::Queue
    }
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
    pub(super) fn config(&self) -> QueueConfig {
        self.config
    }
    pub(super) fn mode(&self) -> QueueCapacityMode {
        self.mode
    }
}

/// One clock-free checked ordinary primary queue and its shared DLQ capacity.
#[derive(Clone)]
pub(super) struct QueueCapacityOwnerProfile {
    owner: QueueCapacityOwner,
    usage: Option<QueueCapacityUsage>,
}

impl QueueCapacityOwnerProfile {
    pub(super) fn namespace(&self) -> &NamespaceName {
        self.owner.namespace()
    }
    pub(super) fn owner(&self) -> &EntityPath {
        self.owner.owner()
    }
    #[cfg(test)]
    pub(super) fn target(&self) -> &EntityPath {
        self.owner.target()
    }
    #[cfg(test)]
    pub(super) fn kind(&self) -> EntityIncarnationKind {
        self.owner.kind()
    }
    pub(super) fn generation(&self) -> u64 {
        self.owner.generation()
    }
    pub(super) fn config(&self) -> QueueConfig {
        self.owner.config()
    }
    pub(super) fn mode(&self) -> QueueCapacityMode {
        self.owner.mode()
    }
    pub(super) fn usage(&self) -> Option<QueueCapacityUsage> {
        self.usage
    }
}

fn read_owner<S: StateStore>(
    store: &S,
    budget: &mut ReadBudget,
    namespace: &NamespaceName,
    target: &EntityPath,
    identity: Option<EntityIncarnation>,
) -> Result<QueueCapacityOwner, BrokerError> {
    NamespaceName::new(namespace.as_str())?;
    let owner = normalize_target(target)?;
    if owner.is_subscription_path() {
        return Err(BrokerError::QueueCapacityNotSupported);
    }
    let identity = match identity {
        Some(value) => value,
        None => incarnation(store, budget, namespace, &owner)?,
    };
    if identity.kind() != EntityIncarnationKind::Queue || identity.is_retired() {
        return Err(BrokerError::QueueCapacityNotSupported);
    }
    let config = queue_config(store, budget, namespace, &owner)?;
    let shadow = owner.dead_letter_queue()?;
    if queue_config(store, budget, namespace, &shadow)? != config.dead_letter_shadow() {
        return Err(BrokerError::QueueCapacityCorrupt);
    }
    for entity in [&owner, &shadow] {
        budget.absent(store, &keys::topic_config(namespace, entity))?;
    }
    let bytes = budget
        .get(store, &keys::queue_capacity_mode(namespace, &owner))?
        .ok_or(BrokerError::QueueCapacityCorrupt)?;
    let mode = QueueCapacityMode::decode(&bytes, identity.generation()).map_err(corrupt)?;
    mode.validate_config(&config).map_err(corrupt)?;
    budget.absent(store, &keys::queue_capacity_mode(namespace, &shadow))?;
    Ok(QueueCapacityOwner {
        namespace: namespace.clone(),
        owner,
        target: target.clone(),
        config,
        generation: identity.generation(),
        mode,
    })
}

fn read_usage<S: StateStore>(
    store: &S,
    budget: &mut ReadBudget,
    owner: QueueCapacityOwner,
) -> Result<QueueCapacityOwnerProfile, BrokerError> {
    let bytes = budget.get(
        store,
        &keys::queue_capacity_usage(&owner.namespace, &owner.owner),
    )?;
    let usage = match (owner.mode.limit_bytes(), bytes) {
        (None, None) => None,
        (Some(_), Some(bytes)) => {
            let value = QueueCapacityUsage::decode(&bytes, owner.generation).map_err(corrupt)?;
            value.validate_mode(owner.mode).map_err(corrupt)?;
            Some(value)
        }
        _ => return Err(BrokerError::QueueCapacityCorrupt),
    };
    budget.absent(
        store,
        &keys::queue_capacity_usage(&owner.namespace, &owner.owner.dead_letter_queue()?),
    )?;
    Ok(QueueCapacityOwnerProfile { owner, usage })
}

fn validate_excluded_owner<S: StateStore>(
    store: &S,
    budget: &mut ReadBudget,
    namespace: &NamespaceName,
    owner: &EntityPath,
    identity: EntityIncarnation,
) -> Result<(), BrokerError> {
    match identity.kind() {
        EntityIncarnationKind::Topic => {
            if owner.is_subscription_path() {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
            budget.absent(store, &keys::queue_config(namespace, owner))?;
            let bytes = budget
                .get(store, &keys::topic_config(namespace, owner))?
                .ok_or(BrokerError::QueueCapacityCorrupt)?;
            if bytes.len() > MAX_PROFILE_RECORD_BYTES {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
            let config: TopicConfig = codec::decode(&bytes).map_err(corrupt)?;
            config.validate().map_err(corrupt)?;
            // Topics do not require enough path headroom to own a DLQ.
            if let Ok(shadow) = owner.dead_letter_queue() {
                budget.absent(store, &keys::queue_config(namespace, &shadow))?;
                budget.absent(store, &keys::topic_config(namespace, &shadow))?;
                budget.absent(store, &keys::queue_capacity_mode(namespace, &shadow))?;
                budget.absent(store, &keys::queue_capacity_usage(namespace, &shadow))?;
            }
        }
        EntityIncarnationKind::Subscription => {
            if !owner.is_subscription_path() {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
            let config = queue_config(store, budget, namespace, owner)?;
            let shadow = owner.dead_letter_queue()?;
            if queue_config(store, budget, namespace, &shadow)? != config.dead_letter_shadow() {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
            for entity in [owner, &shadow] {
                budget.absent(store, &keys::topic_config(namespace, entity))?;
            }
            budget.absent(store, &keys::queue_capacity_mode(namespace, &shadow))?;
            budget.absent(store, &keys::queue_capacity_usage(namespace, &shadow))?;
        }
        EntityIncarnationKind::Queue => return Err(BrokerError::QueueCapacityCorrupt),
    }
    budget.absent(store, &keys::queue_capacity_mode(namespace, owner))?;
    budget.absent(store, &keys::queue_capacity_usage(namespace, owner))?;
    Ok(())
}

pub(super) fn validate_owner_mode<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    target: &EntityPath,
) -> Result<QueueCapacityOwner, BrokerError> {
    read_owner(
        machine.store(),
        &mut ReadBudget::default(),
        namespace,
        target,
        None,
    )
}

pub(super) fn validate_owner_profile<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    target: &EntityPath,
) -> Result<QueueCapacityOwnerProfile, BrokerError> {
    let mut budget = ReadBudget::default();
    let owner = read_owner(machine.store(), &mut budget, namespace, target, None)?;
    read_usage(machine.store(), &mut budget, owner)
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct CapacityMessage {
    entity: EntityPath,
    sequence: SequenceNumber,
}

impl CapacityMessage {
    pub(super) fn new(entity: EntityPath, sequence: SequenceNumber) -> Self {
        Self { entity, sequence }
    }
}

type Observation = Result<RecordChargeObservation, QueueCapacityError>;

struct SourceCoverage {
    initial: QueueCapacityUsage,
    bytes: u64,
    count: u64,
}

impl SourceCoverage {
    fn new(initial: QueueCapacityUsage) -> Self {
        Self {
            initial,
            bytes: 0,
            count: 0,
        }
    }

    fn include(&mut self, charge: MessageCharge) -> Result<(), BrokerError> {
        let bytes = self
            .bytes
            .checked_add(charge.charged_bytes())
            .filter(|value| *value <= self.initial.reserved_bytes())
            .ok_or(BrokerError::QueueCapacityCorrupt)?;
        let count = self
            .count
            .checked_add(1)
            .filter(|value| *value <= self.initial.message_count())
            .ok_or(BrokerError::QueueCapacityCorrupt)?;
        let remaining_bytes = self
            .initial
            .reserved_bytes()
            .checked_sub(bytes)
            .ok_or(BrokerError::QueueCapacityCorrupt)?;
        let remaining_count = self
            .initial
            .message_count()
            .checked_sub(count)
            .ok_or(BrokerError::QueueCapacityCorrupt)?;
        QueueCapacityUsage::new(self.initial.generation(), remaining_bytes, remaining_count)
            .map_err(corrupt)?;
        self.bytes = bytes;
        self.count = count;
        Ok(())
    }
}

enum Event {
    Retain(CapacityMessage, Observation),
    Check(CapacityMessage, Observation),
    Remove(CapacityMessage, Observation),
    Replace(CapacityMessage, Observation, Observation),
    Transfer(CapacityMessage, CapacityMessage, Observation, Observation),
}

enum OwnerState {
    Existing,
    Prepared(QueueCapacityOwnerProfile),
    PreparedTopic,
    Retired(QueueCapacityOwner),
}

/// Command-local numeric observations. No body clone, store mutation or global cache.
pub(super) struct CapacityPlan {
    namespace: NamespaceName,
    target: EntityPath,
    state: OwnerState,
    resolved: Option<Option<QueueCapacityOwnerProfile>>,
    budget: ReadBudget,
    events: Vec<Event>,
    touched: BTreeSet<CapacityMessage>,
    pending: Option<BrokerError>,
    allow_idle_absent_owner: bool,
}

impl CapacityPlan {
    pub(super) fn existing(namespace: &NamespaceName, target: &EntityPath) -> Self {
        Self {
            namespace: namespace.clone(),
            target: target.clone(),
            state: OwnerState::Existing,
            resolved: None,
            budget: ReadBudget::default(),
            events: Vec::new(),
            touched: BTreeSet::new(),
            pending: None,
            allow_idle_absent_owner: false,
        }
    }

    /// Only a no-mutation idle session-lock sweep can accept an absent owner.
    pub(super) fn allow_idle_absent_owner(&mut self) -> Result<(), BrokerError> {
        if !matches!(self.state, OwnerState::Existing)
            || self.resolved.is_some()
            || !self.events.is_empty()
            || self.pending.is_some()
        {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        self.allow_idle_absent_owner = true;
        Ok(())
    }

    /// Creation has already staged configurations, incarnation and canonical Mode.
    pub(super) fn prepare_owner(
        &mut self,
        config: QueueConfig,
        identity: EntityIncarnation,
        mode: QueueCapacityMode,
    ) -> Result<(), BrokerError> {
        if !matches!(self.state, OwnerState::Existing)
            || self.resolved.is_some()
            || !self.events.is_empty()
            || self.pending.is_some()
        {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        let namespace = &self.namespace;
        let owner = &self.target;
        NamespaceName::new(namespace.as_str())?;
        EntityPath::new(owner.as_str())?;
        if owner.is_dead_letter_queue()
            || owner.is_subscription_path()
            || identity.kind() != EntityIncarnationKind::Queue
            || identity.is_retired()
        {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        identity.validate().map_err(corrupt)?;
        config.validate().map_err(corrupt)?;
        owner.dead_letter_queue()?;
        mode.validate_generation(identity.generation())
            .map_err(corrupt)?;
        mode.validate_config(&config).map_err(proposed)?;
        let usage = mode
            .limit_bytes()
            .map(|_| QueueCapacityUsage::new(identity.generation(), 0, 0))
            .transpose()
            .map_err(corrupt)?;
        let profile = QueueCapacityOwnerProfile {
            owner: QueueCapacityOwner {
                namespace: namespace.clone(),
                target: owner.clone(),
                owner: owner.clone(),
                config,
                generation: identity.generation(),
                mode,
            },
            usage,
        };
        self.state = OwnerState::Prepared(profile);
        Ok(())
    }

    pub(super) fn mark_retired(&mut self, owner: QueueCapacityOwner) -> Result<(), BrokerError> {
        if !matches!(self.state, OwnerState::Existing)
            || self.namespace != owner.namespace
            || self.target != owner.owner
            || owner.target != owner.owner
            || !self.events.is_empty()
            || self.pending.is_some()
        {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        self.resolved = None;
        self.state = OwnerState::Retired(owner);
        Ok(())
    }

    /// Fresh topic topology and incarnation have already passed old creation checks.
    pub(super) fn prepare_excluded_owner(
        &mut self,
        identity: EntityIncarnation,
    ) -> Result<(), BrokerError> {
        if !matches!(self.state, OwnerState::Existing)
            || self.resolved.is_some()
            || !self.events.is_empty()
            || self.pending.is_some()
            || identity.kind() != EntityIncarnationKind::Topic
            || identity.is_retired()
            || self.target.is_dead_letter_queue()
            || self.target.is_subscription_path()
        {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        NamespaceName::new(self.namespace.as_str())?;
        EntityPath::new(self.target.as_str())?;
        identity.validate().map_err(corrupt)?;
        self.state = OwnerState::PreparedTopic;
        Ok(())
    }

    fn record(&mut self, event: Event, messages: &[CapacityMessage]) -> Result<(), BrokerError> {
        let disabled = self.resolved.as_ref().is_some_and(|profile| {
            profile
                .as_ref()
                .is_none_or(|profile| profile.usage().is_none())
        }) || matches!(&self.state, OwnerState::Prepared(profile) if profile.usage().is_none())
            || matches!(self.state, OwnerState::PreparedTopic);
        if disabled {
            return Ok(());
        }
        if let Some(error) = &self.pending {
            return Err(error.clone());
        }
        if self.events.len() == MAX_CAPACITY_EVENTS {
            self.pending = Some(BrokerError::QueueCapacityWorkLimitExceeded);
            return Err(BrokerError::QueueCapacityWorkLimitExceeded);
        }
        for message in messages {
            if !self.touched.contains(message) && self.touched.len() == MAX_CAPACITY_MESSAGES {
                self.pending = Some(BrokerError::QueueCapacityWorkLimitExceeded);
                return Err(BrokerError::QueueCapacityWorkLimitExceeded);
            }
            self.touched.insert(message.clone());
        }
        self.events.push(event);
        Ok(())
    }

    pub(super) fn record_new(
        &mut self,
        target: &EntityPath,
        sequence: SequenceNumber,
        proposed: Observation,
    ) -> Result<(), BrokerError> {
        let target = CapacityMessage::new(target.clone(), sequence);
        self.record(Event::Retain(target.clone(), proposed), &[target])
    }
    pub(super) fn record_check(
        &mut self,
        source: &EntityPath,
        sequence: SequenceNumber,
        original: Observation,
    ) -> Result<(), BrokerError> {
        let source = CapacityMessage::new(source.clone(), sequence);
        self.record(Event::Check(source.clone(), original), &[source])
    }
    pub(super) fn record_remove(
        &mut self,
        source: &EntityPath,
        sequence: SequenceNumber,
        original: Observation,
    ) -> Result<(), BrokerError> {
        let source = CapacityMessage::new(source.clone(), sequence);
        self.record(Event::Remove(source.clone(), original), &[source])
    }
    pub(super) fn record_replace(
        &mut self,
        source: &EntityPath,
        sequence: SequenceNumber,
        original: Observation,
        proposed: Observation,
    ) -> Result<(), BrokerError> {
        let source = CapacityMessage::new(source.clone(), sequence);
        self.record(
            Event::Replace(source.clone(), original, proposed),
            &[source],
        )
    }
    pub(super) fn record_transfer(
        &mut self,
        source: &EntityPath,
        old_sequence: SequenceNumber,
        target: &EntityPath,
        new_sequence: SequenceNumber,
        original: Observation,
        proposed: Observation,
    ) -> Result<(), BrokerError> {
        let source = CapacityMessage::new(source.clone(), old_sequence);
        let target = CapacityMessage::new(target.clone(), new_sequence);
        self.record(
            Event::Transfer(source.clone(), target, original, proposed),
            &[source],
        )
    }

    fn resolve<S: StateStore>(
        &mut self,
        machine: &StateMachine<S>,
    ) -> Result<Option<QueueCapacityOwnerProfile>, BrokerError> {
        if let Some(value) = &self.resolved {
            return Ok(value.clone());
        }
        let profile = match &self.state {
            OwnerState::Prepared(profile) => Some(profile.clone()),
            OwnerState::PreparedTopic => {
                self.budget.absent(
                    machine.store(),
                    &keys::queue_capacity_mode(&self.namespace, &self.target),
                )?;
                self.budget.absent(
                    machine.store(),
                    &keys::queue_capacity_usage(&self.namespace, &self.target),
                )?;
                if let Ok(shadow) = self.target.dead_letter_queue() {
                    self.budget.absent(
                        machine.store(),
                        &keys::queue_capacity_mode(&self.namespace, &shadow),
                    )?;
                    self.budget.absent(
                        machine.store(),
                        &keys::queue_capacity_usage(&self.namespace, &shadow),
                    )?;
                }
                None
            }
            OwnerState::Retired(_) => return Err(BrokerError::QueueCapacityCorrupt),
            OwnerState::Existing => {
                NamespaceName::new(self.namespace.as_str())?;
                let owner = normalize_target(&self.target)?;
                let identity = if self.allow_idle_absent_owner {
                    match optional_incarnation(
                        machine.store(),
                        &mut self.budget,
                        &self.namespace,
                        &owner,
                    )? {
                        Some(value) if !value.is_retired() => value,
                        _ => {
                            let shadow = owner.dead_letter_queue().ok();
                            for entity in std::iter::once(&owner).chain(shadow.as_ref()) {
                                for key in [
                                    keys::queue_config(&self.namespace, entity),
                                    keys::topic_config(&self.namespace, entity),
                                    keys::queue_capacity_mode(&self.namespace, entity),
                                    keys::queue_capacity_usage(&self.namespace, entity),
                                ] {
                                    self.budget.absent(machine.store(), &key)?;
                                }
                            }
                            self.resolved = Some(None);
                            return Ok(None);
                        }
                    }
                } else {
                    incarnation(machine.store(), &mut self.budget, &self.namespace, &owner)?
                };
                if identity.kind() == EntityIncarnationKind::Queue {
                    let owner = read_owner(
                        machine.store(),
                        &mut self.budget,
                        &self.namespace,
                        &self.target,
                        Some(identity),
                    )?;
                    Some(read_usage(machine.store(), &mut self.budget, owner)?)
                } else {
                    // Full topic membership/rules remain the caller's validation.
                    validate_excluded_owner(
                        machine.store(),
                        &mut self.budget,
                        &self.namespace,
                        &owner,
                        identity,
                    )?;
                    None
                }
            }
        };
        self.resolved = Some(profile.clone());
        Ok(profile)
    }

    /// Only oversized vectors use the explicit early profile-validation exception.
    pub(super) fn check_input_count<S: StateStore>(
        &mut self,
        machine: &StateMachine<S>,
        count: usize,
    ) -> Result<(), BrokerError> {
        if count <= MAX_CAPACITY_MESSAGES {
            return Ok(());
        }
        if self
            .resolve(machine)?
            .is_some_and(|profile| profile.mode().limit_bytes().is_some())
        {
            return Err(BrokerError::QueueCapacityWorkLimitExceeded);
        }
        Ok(())
    }

    /// Consume only after every original handler validation has succeeded.
    /// Failures append no sidecar mutation, even if callers retain their private batch.
    pub(super) fn finish<S: StateStore>(
        mut self,
        machine: &StateMachine<S>,
        batch: &mut WriteBatch,
    ) -> Result<(), BrokerError> {
        if self.allow_idle_absent_owner
            && (!batch.is_empty() || !self.events.is_empty() || self.pending.is_some())
        {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        if let OwnerState::Retired(owner) = &self.state {
            if !self.events.is_empty()
                || self.pending.is_some()
                || owner.owner.is_dead_letter_queue()
            {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
            return Ok(());
        }
        let Some(profile) = self.resolve(machine)? else {
            return Ok(());
        };
        let prepared = matches!(self.state, OwnerState::Prepared(_));
        let mut sidecars = WriteBatch::default();
        let Some(mut usage) = profile.usage() else {
            append(batch, sidecars);
            return Ok(());
        };
        if let Some(error) = self.pending.take() {
            return Err(error);
        }
        let mut coverage = SourceCoverage::new(usage);
        let mut ledgers: BTreeMap<CapacityMessage, Option<MessageCharge>> = BTreeMap::new();
        let mut changed = BTreeSet::new();
        let mut usage_changed = prepared;
        for event in std::mem::take(&mut self.events) {
            match event {
                Event::Retain(target, observation) => {
                    let observation = observation.map_err(corrupt)?;
                    validate_message_scope(&profile, &target, observation)?;
                    if target.entity != *profile.owner() {
                        return Err(BrokerError::QueueCapacityCorrupt);
                    }
                    ensure_destination(machine, &profile, &target, &mut self.budget, &mut ledgers)?;
                    let charge =
                        MessageCharge::for_new_observation(profile.generation(), observation)
                            .map_err(corrupt)?;
                    usage = usage.reserve(profile.mode(), charge).map_err(proposed)?;
                    ledgers.insert(target.clone(), Some(charge));
                    changed.insert(target);
                    usage_changed = true;
                }
                Event::Check(source, original) => {
                    source_charge(
                        machine,
                        &profile,
                        &source,
                        original,
                        &mut self.budget,
                        &mut ledgers,
                        &mut coverage,
                    )?;
                }
                Event::Remove(source, original) => {
                    let charge = source_charge(
                        machine,
                        &profile,
                        &source,
                        original,
                        &mut self.budget,
                        &mut ledgers,
                        &mut coverage,
                    )?;
                    usage = usage.refund(profile.mode(), charge).map_err(corrupt)?;
                    ledgers.insert(source.clone(), None);
                    changed.insert(source);
                    usage_changed = true;
                }
                Event::Replace(source, original, observation) => {
                    let charge = source_charge(
                        machine,
                        &profile,
                        &source,
                        original,
                        &mut self.budget,
                        &mut ledgers,
                        &mut coverage,
                    )?;
                    let observation = observation.map_err(corrupt)?;
                    validate_message_scope(&profile, &source, observation)?;
                    let proposed_charge =
                        charge.recharge_observation(observation).map_err(corrupt)?;
                    usage = usage
                        .replace(profile.mode(), charge, proposed_charge)
                        .map_err(proposed)?;
                    if charge != proposed_charge {
                        ledgers.insert(source.clone(), Some(proposed_charge));
                        changed.insert(source);
                        usage_changed = true;
                    }
                }
                Event::Transfer(source, target, original, observation) => {
                    if source == target {
                        return Err(BrokerError::QueueCapacityCorrupt);
                    }
                    let charge = source_charge(
                        machine,
                        &profile,
                        &source,
                        original,
                        &mut self.budget,
                        &mut ledgers,
                        &mut coverage,
                    )?;
                    let observation = observation.map_err(corrupt)?;
                    validate_message_scope(&profile, &target, observation)?;
                    ensure_destination(machine, &profile, &target, &mut self.budget, &mut ledgers)?;
                    let proposed_charge =
                        charge.recharge_observation(observation).map_err(corrupt)?;
                    usage = usage
                        .replace(profile.mode(), charge, proposed_charge)
                        .map_err(proposed)?;
                    ledgers.insert(source.clone(), None);
                    changed.insert(source);
                    ledgers.insert(target.clone(), Some(proposed_charge));
                    changed.insert(target);
                    usage_changed = true;
                }
            }
            if ledgers.len() > MAX_CAPACITY_LEDGER_KEYS {
                return Err(BrokerError::QueueCapacityWorkLimitExceeded);
            }
        }
        for message in changed {
            let key = keys::message_charge(&self.namespace, &message.entity, message.sequence);
            match ledgers.get(&message).copied().flatten() {
                Some(charge) => sidecars.push_put(key, charge.encode().map_err(corrupt)?),
                None => sidecars.push_delete(key),
            }
        }
        if usage_changed {
            sidecars.push_put(
                keys::queue_capacity_usage(&self.namespace, profile.owner()),
                usage.encode().map_err(corrupt)?,
            );
        }
        append(batch, sidecars);
        Ok(())
    }
}

fn validate_message_scope(
    profile: &QueueCapacityOwnerProfile,
    message: &CapacityMessage,
    observation: RecordChargeObservation,
) -> Result<(), BrokerError> {
    let shadow = profile.owner().dead_letter_queue()?;
    if (message.entity != *profile.owner() && message.entity != shadow)
        || message.sequence.as_u64() == 0
        || message.sequence.as_u64() > MAX_SEQUENCE_NUMBER
        || observation.sequence() != message.sequence
        || observation.is_dead_letter() != (message.entity == shadow)
    {
        return Err(BrokerError::QueueCapacityCorrupt);
    }
    Ok(())
}

fn append(batch: &mut WriteBatch, sidecars: WriteBatch) {
    for mutation in sidecars.into_mutations() {
        match mutation {
            Mutation::Put { key, value } => batch.push_put(key, value),
            Mutation::Delete { key } => batch.push_delete(key),
        }
    }
}

fn source_charge<S: StateStore>(
    machine: &StateMachine<S>,
    profile: &QueueCapacityOwnerProfile,
    source: &CapacityMessage,
    observation: Observation,
    budget: &mut ReadBudget,
    ledgers: &mut BTreeMap<CapacityMessage, Option<MessageCharge>>,
    coverage: &mut SourceCoverage,
) -> Result<MessageCharge, BrokerError> {
    let observation = observation.map_err(corrupt)?;
    validate_message_scope(profile, source, observation)?;
    if let Some(value) = ledgers.get(source) {
        let charge = value.ok_or(BrokerError::QueueCapacityCorrupt)?;
        charge.validate_observation(observation).map_err(corrupt)?;
        return Ok(charge);
    }
    let bytes = budget
        .get(
            machine.store(),
            &keys::message_charge(profile.namespace(), &source.entity, source.sequence),
        )?
        .ok_or(BrokerError::QueueCapacityCorrupt)?;
    let charge = MessageCharge::decode(&bytes, profile.generation()).map_err(corrupt)?;
    charge.validate_observation(observation).map_err(corrupt)?;
    coverage.include(charge)?;
    ledgers.insert(source.clone(), Some(charge));
    Ok(charge)
}

fn ensure_destination<S: StateStore>(
    machine: &StateMachine<S>,
    profile: &QueueCapacityOwnerProfile,
    target: &CapacityMessage,
    budget: &mut ReadBudget,
    ledgers: &mut BTreeMap<CapacityMessage, Option<MessageCharge>>,
) -> Result<(), BrokerError> {
    if let Some(value) = ledgers.get(target) {
        if value.is_some() {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        return Ok(());
    }
    budget.absent(
        machine.store(),
        &keys::message_charge(profile.namespace(), &target.entity, target.sequence),
    )?;
    budget.absent(
        machine.store(),
        &keys::message(profile.namespace(), &target.entity, target.sequence),
    )?;
    ledgers.insert(target.clone(), None);
    Ok(())
}

#[cfg(test)]
mod tests;
