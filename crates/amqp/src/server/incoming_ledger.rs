use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use crate::ReceiverSettleMode;

pub(super) const MAX_INCOMING_DELIVERIES_PER_LINK: usize = 1_024;
pub(super) const MAX_INCOMING_DELIVERIES_PER_SESSION: usize = 4_096;
const MAX_TAG_BYTES: usize = 32;

#[derive(Clone, Debug)]
pub(super) struct LinkIdentity(Arc<LinkGeneration>);

#[derive(Debug)]
struct LinkGeneration {
    retired: AtomicBool,
}

impl LinkIdentity {
    pub(super) fn new() -> Self {
        Self(Arc::new(LinkGeneration {
            retired: AtomicBool::new(false),
        }))
    }

    pub(super) fn same_link(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(super) fn retire(&self) {
        self.0.retired.store(true, Ordering::Release);
    }

    fn key(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    fn check_live(&self) -> Result<(), IncomingLedgerError> {
        if self.0.retired.load(Ordering::Acquire) {
            Err(IncomingLedgerError::RetiredLink)
        } else {
            Ok(())
        }
    }
}

impl PartialEq for LinkIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.same_link(other)
    }
}

impl Eq for LinkIdentity {}

#[derive(Clone, Debug)]
pub(super) struct DeliveryIdentity(Arc<DeliveryGeneration>);

#[derive(Debug)]
struct DeliveryGeneration {
    id: u32,
    owner: LinkIdentity,
    terminal: AtomicU8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum Terminal {
    Live,
    Settled,
    Aborted,
    Invalidated,
}

impl DeliveryIdentity {
    pub(super) fn id(&self) -> u32 {
        self.0.id
    }

    pub(super) fn belongs_to(&self, owner: &LinkIdentity) -> bool {
        self.0.owner.same_link(owner)
    }

    pub(super) fn same_delivery(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn terminal(&self) -> Terminal {
        match self.0.terminal.load(Ordering::Acquire) {
            0 => Terminal::Live,
            1 => Terminal::Settled,
            2 => Terminal::Aborted,
            3 => Terminal::Invalidated,
            _ => unreachable!("only ledger terminal states can be stored"),
        }
    }

    fn mark_terminal(&self, terminal: Terminal) {
        self.0.terminal.store(terminal as u8, Ordering::Release);
    }
}

impl PartialEq for DeliveryIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.same_delivery(other)
    }
}

impl Eq for DeliveryIdentity {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Completion {
    Unsettled,
    SenderSettled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SettlementAction {
    SendDisposition { settled: bool },
    NoDisposition,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SenderSettlement {
    pub matched: usize,
    pub released: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Partial,
    Complete,
    AwaitingSenderAck,
}

#[derive(Debug)]
struct IncomingDelivery {
    identity: DeliveryIdentity,
    tag: Vec<u8>,
    phase: Phase,
    receiver_mode: Option<ReceiverSettleMode>,
    remote_settled: bool,
}

#[derive(Debug, Default)]
pub(super) struct IncomingLedger {
    deliveries: HashMap<u32, IncomingDelivery>,
    // Every indexed tag has a live entry owning its generation's Arc, so its
    // address cannot be recycled until that generation's index is removed.
    tags: HashMap<usize, HashMap<Vec<u8>, u32>>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum IncomingLedgerError {
    #[error("incoming delivery id {id} is already in use on this session")]
    DeliveryIdInUse { id: u32 },
    #[error("incoming delivery tag is already in use on this link")]
    DeliveryTagInUse,
    #[error("incoming delivery tag has {actual} bytes, maximum is {maximum}")]
    DeliveryTagTooLong { actual: usize, maximum: usize },
    #[error("incoming delivery limit of {maximum} reached on this link")]
    LinkLimitReached { maximum: usize },
    #[error("incoming delivery limit of {maximum} reached on this session")]
    SessionLimitReached { maximum: usize },
    #[error("delivery belongs to a different receiving link generation")]
    WrongOwner,
    #[error("receiving link generation has been retired")]
    RetiredLink,
    #[error("incoming delivery has been aborted")]
    AbortedDelivery,
    #[error("incoming delivery is no longer live")]
    UnknownDelivery,
    #[error("incoming delivery id now belongs to a different delivery generation")]
    StaleDelivery,
    #[error("incoming delivery has not completed")]
    IncompleteDelivery,
    #[error("incoming delivery has already completed")]
    AlreadyComplete,
}

impl IncomingLedger {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn reserve(
        &mut self,
        owner: &LinkIdentity,
        id: u32,
        tag: &[u8],
    ) -> Result<DeliveryIdentity, IncomingLedgerError> {
        owner.check_live()?;
        if tag.len() > MAX_TAG_BYTES {
            return Err(IncomingLedgerError::DeliveryTagTooLong {
                actual: tag.len(),
                maximum: MAX_TAG_BYTES,
            });
        }
        if self.deliveries.contains_key(&id) {
            return Err(IncomingLedgerError::DeliveryIdInUse { id });
        }
        let tags = self.tags.get(&owner.key());
        if tags.is_some_and(|tags| tags.contains_key(tag)) {
            return Err(IncomingLedgerError::DeliveryTagInUse);
        }
        if tags.is_some_and(|tags| tags.len() >= MAX_INCOMING_DELIVERIES_PER_LINK) {
            return Err(IncomingLedgerError::LinkLimitReached {
                maximum: MAX_INCOMING_DELIVERIES_PER_LINK,
            });
        }
        if self.deliveries.len() >= MAX_INCOMING_DELIVERIES_PER_SESSION {
            return Err(IncomingLedgerError::SessionLimitReached {
                maximum: MAX_INCOMING_DELIVERIES_PER_SESSION,
            });
        }

        let identity = DeliveryIdentity(Arc::new(DeliveryGeneration {
            id,
            owner: owner.clone(),
            terminal: AtomicU8::new(Terminal::Live as u8),
        }));
        self.tags
            .entry(owner.key())
            .or_default()
            .insert(tag.to_vec(), id);
        self.deliveries.insert(
            id,
            IncomingDelivery {
                identity: identity.clone(),
                tag: tag.to_vec(),
                phase: Phase::Partial,
                receiver_mode: None,
                remote_settled: false,
            },
        );
        Ok(identity)
    }

    pub(super) fn complete(
        &mut self,
        identity: &DeliveryIdentity,
        sender_settled: bool,
        receiver_mode: ReceiverSettleMode,
    ) -> Result<Completion, IncomingLedgerError> {
        let delivery = self.live_delivery(identity)?;
        if delivery.phase != Phase::Partial {
            return Err(IncomingLedgerError::AlreadyComplete);
        }
        if sender_settled || delivery.remote_settled {
            self.release(identity, Terminal::Settled);
            return Ok(Completion::SenderSettled);
        }
        let delivery = self
            .deliveries
            .get_mut(&identity.id())
            .expect("live delivery was checked");
        delivery.phase = Phase::Complete;
        delivery.receiver_mode = Some(receiver_mode);
        Ok(Completion::Unsettled)
    }

    pub(super) fn sender_is_settled(
        &self,
        identity: &DeliveryIdentity,
    ) -> Result<bool, IncomingLedgerError> {
        Ok(self.live_delivery(identity)?.remote_settled)
    }

    // Read-only preflight: writing or validating an outcome must not consume an alias.
    pub(super) fn settlement(
        &self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<SettlementAction, IncomingLedgerError> {
        if !identity.belongs_to(owner) {
            return Err(IncomingLedgerError::WrongOwner);
        }
        owner.check_live()?;
        // Terminal identity is authoritative before numeric lookup: that ID may
        // already belong to a later delivery on the same open link.
        match identity.terminal() {
            Terminal::Settled => return Ok(SettlementAction::NoDisposition),
            Terminal::Aborted => return Err(IncomingLedgerError::AbortedDelivery),
            Terminal::Invalidated => return Err(IncomingLedgerError::UnknownDelivery),
            Terminal::Live => {}
        }
        let delivery = self.live_delivery(identity)?;
        if delivery.phase == Phase::Partial {
            return Err(IncomingLedgerError::IncompleteDelivery);
        }
        if delivery.phase == Phase::AwaitingSenderAck || delivery.remote_settled {
            return Ok(SettlementAction::NoDisposition);
        }
        Ok(SettlementAction::SendDisposition {
            settled: delivery.receiver_mode == Some(ReceiverSettleMode::First),
        })
    }

    pub(super) fn commit_settlement(
        &mut self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<(), IncomingLedgerError> {
        let action = self.settlement(owner, identity)?;
        if identity.terminal() == Terminal::Settled {
            return Ok(());
        }
        let delivery = self.live_delivery(identity)?;
        if delivery.phase == Phase::AwaitingSenderAck {
            return Ok(());
        }
        if action == (SettlementAction::SendDisposition { settled: false }) {
            self.deliveries
                .get_mut(&identity.id())
                .expect("live delivery was checked")
                .phase = Phase::AwaitingSenderAck;
        } else {
            self.release(identity, Terminal::Settled);
        }
        Ok(())
    }

    pub(super) fn sender_settled_range(
        &mut self,
        first: u32,
        last: Option<u32>,
    ) -> SenderSettlement {
        let width = last.unwrap_or(first).wrapping_sub(first);
        // Never iterate the serial-number span; only already bounded live IDs.
        let identities: Vec<_> = self
            .deliveries
            .values()
            .filter(|delivery| delivery.identity.id().wrapping_sub(first) <= width)
            .map(|delivery| delivery.identity.clone())
            .collect();
        let mut result = SenderSettlement {
            matched: identities.len(),
            released: 0,
        };
        for identity in identities {
            let delivery = self
                .deliveries
                .get_mut(&identity.id())
                .expect("matching live delivery exists");
            if delivery.phase == Phase::AwaitingSenderAck {
                self.release(&identity, Terminal::Settled);
                result.released += 1;
            } else {
                delivery.remote_settled = true;
            }
        }
        result
    }

    pub(super) fn abort(&mut self, identity: &DeliveryIdentity) -> Result<(), IncomingLedgerError> {
        if self.live_delivery(identity)?.phase != Phase::Partial {
            return Err(IncomingLedgerError::AlreadyComplete);
        }
        self.remove(identity)?;
        identity.mark_terminal(Terminal::Aborted);
        Ok(())
    }

    pub(super) fn remove(
        &mut self,
        identity: &DeliveryIdentity,
    ) -> Result<(), IncomingLedgerError> {
        self.live_delivery(identity)?;
        self.release(identity, Terminal::Invalidated);
        Ok(())
    }

    pub(super) fn remove_link(&mut self, owner: &LinkIdentity) -> usize {
        owner.retire();
        let Some(tags) = self.tags.remove(&owner.key()) else {
            return 0;
        };
        let removed = tags.len();
        for id in tags.into_values() {
            let delivery = self
                .deliveries
                .remove(&id)
                .expect("link tag index points to a live delivery");
            debug_assert!(delivery.identity.belongs_to(owner));
            delivery.identity.mark_terminal(Terminal::Invalidated);
        }
        removed
    }

    fn live_delivery(
        &self,
        identity: &DeliveryIdentity,
    ) -> Result<&IncomingDelivery, IncomingLedgerError> {
        identity.0.owner.check_live()?;
        match identity.terminal() {
            Terminal::Live => {}
            Terminal::Aborted => return Err(IncomingLedgerError::AbortedDelivery),
            Terminal::Settled | Terminal::Invalidated => {
                return Err(IncomingLedgerError::UnknownDelivery);
            }
        }
        let delivery = self
            .deliveries
            .get(&identity.id())
            .ok_or(IncomingLedgerError::UnknownDelivery)?;
        if !delivery.identity.same_delivery(identity) {
            return Err(IncomingLedgerError::StaleDelivery);
        }
        Ok(delivery)
    }

    fn release(&mut self, identity: &DeliveryIdentity, terminal: Terminal) {
        let delivery = self
            .deliveries
            .remove(&identity.id())
            .expect("release follows a checked live delivery");
        debug_assert!(delivery.identity.same_delivery(identity));
        let owner = &identity.0.owner;
        let tags = self
            .tags
            .get_mut(&owner.key())
            .expect("live delivery has a link tag index");
        let removed = tags.remove(delivery.tag.as_slice());
        debug_assert_eq!(removed, Some(identity.id()));
        if tags.is_empty() {
            self.tags.remove(&owner.key());
        }
        identity.mark_terminal(terminal);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct Snapshot {
        deliveries: Vec<SnapshotDelivery>,
        tags: HashMap<usize, HashMap<Vec<u8>, u32>>,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct SnapshotDelivery {
        id: u32,
        identity: DeliveryIdentity,
        tag: Vec<u8>,
        phase: Phase,
        receiver_mode: Option<ReceiverSettleMode>,
        remote_settled: bool,
        terminal: Terminal,
    }

    fn snapshot(ledger: &IncomingLedger) -> Snapshot {
        assert_eq!(
            ledger.deliveries.len(),
            ledger.tags.values().map(HashMap::len).sum::<usize>()
        );
        let mut deliveries: Vec<_> = ledger
            .deliveries
            .iter()
            .map(|(&id, delivery)| {
                assert_eq!(
                    ledger.tags[&delivery.identity.0.owner.key()].get(&delivery.tag),
                    Some(&id)
                );
                SnapshotDelivery {
                    id,
                    identity: delivery.identity.clone(),
                    tag: delivery.tag.clone(),
                    phase: delivery.phase,
                    receiver_mode: delivery.receiver_mode.clone(),
                    remote_settled: delivery.remote_settled,
                    terminal: delivery.identity.terminal(),
                }
            })
            .collect();
        deliveries.sort_by_key(|delivery| delivery.id);
        Snapshot {
            deliveries,
            tags: ledger.tags.clone(),
        }
    }

    fn reserve(ledger: &mut IncomingLedger, owner: &LinkIdentity, id: u32) -> DeliveryIdentity {
        ledger
            .reserve(owner, id, &id.to_be_bytes())
            .expect("fresh delivery")
    }

    fn completed(
        ledger: &mut IncomingLedger,
        owner: &LinkIdentity,
        id: u32,
        mode: ReceiverSettleMode,
    ) -> DeliveryIdentity {
        let identity = reserve(ledger, owner, id);
        assert_eq!(
            ledger.complete(&identity, false, mode),
            Ok(Completion::Unsettled)
        );
        identity
    }

    #[test]
    fn link_and_delivery_identity_follow_arc_generation_not_numeric_aliases() {
        let mut ledger = IncomingLedger::new();
        let first = LinkIdentity::new();
        let first_clone = first.clone();
        let other = LinkIdentity::new();
        assert!(first.same_link(&first_clone));
        assert!(!first.same_link(&other));
        let delivery = reserve(&mut ledger, &first, 7);
        assert_eq!(delivery.id(), 7);
        assert!(delivery.belongs_to(&first_clone));
        assert!(!delivery.belongs_to(&other));
        assert!(delivery.same_delivery(&delivery.clone()));
        ledger.abort(&delivery).expect("abort first generation");
        let replacement = reserve(&mut ledger, &first, 7);
        assert!(!delivery.same_delivery(&replacement));
    }

    #[test]
    fn ids_are_session_scoped_and_collision_checks_are_mutation_free() {
        let mut ledger = IncomingLedger::new();
        let first = LinkIdentity::new();
        let second = LinkIdentity::new();
        let original = reserve(&mut ledger, &first, 0);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.reserve(&second, 0, b"different"),
            Err(IncomingLedgerError::DeliveryIdInUse { id: 0 })
        );
        assert_eq!(snapshot(&ledger), before);
        ledger
            .abort(&original)
            .expect("original still belongs to first");
        assert!(ledger.reserve(&second, 0, b"different").is_ok());
    }

    #[test]
    fn tags_are_link_scoped_binary_exact_and_empty_is_a_real_tag() {
        let mut ledger = IncomingLedger::new();
        let first = LinkIdentity::new();
        let second = LinkIdentity::new();
        for (id, tag) in [(0, b"".as_slice()), (1, b"a"), (2, b"a\0"), (3, b"a\0b")] {
            ledger.reserve(&first, id, tag).expect("distinct bytes");
        }
        for (id, tag) in [(4, b"".as_slice()), (5, b"a\0b")] {
            let before = snapshot(&ledger);
            assert_eq!(
                ledger.reserve(&first, id, tag),
                Err(IncomingLedgerError::DeliveryTagInUse)
            );
            assert_eq!(snapshot(&ledger), before);
            ledger.reserve(&second, id, tag).expect("sibling tag scope");
        }
        assert_eq!(ledger.deliveries.len(), 6);
    }

    #[test]
    fn tag_size_checks_precede_cloning_or_index_mutation() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        ledger.reserve(&owner, 0, &[1; 32]).expect("maximum tag");
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.reserve(&owner, 1, &[2; 33]),
            Err(IncomingLedgerError::DeliveryTagTooLong {
                actual: 33,
                maximum: 32,
            })
        );
        assert_eq!(snapshot(&ledger), before);
    }

    #[test]
    fn exact_link_cap_is_bounded_and_abort_restores_capacity() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let first = reserve(&mut ledger, &owner, 0);
        for id in 1..MAX_INCOMING_DELIVERIES_PER_LINK as u32 {
            reserve(&mut ledger, &owner, id);
        }
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.reserve(&owner, 1_024, b"overflow"),
            Err(IncomingLedgerError::LinkLimitReached { maximum: 1_024 })
        );
        assert_eq!(snapshot(&ledger), before);
        ledger.abort(&first).expect("one alias released");
        reserve(&mut ledger, &owner, 1_024);
        assert_eq!(ledger.deliveries.len(), MAX_INCOMING_DELIVERIES_PER_LINK);
        snapshot(&ledger);
    }

    #[test]
    fn exact_session_cap_is_bounded_and_link_cleanup_restores_capacity() {
        let mut ledger = IncomingLedger::new();
        let owners: Vec<_> = (0..4).map(|_| LinkIdentity::new()).collect();
        for (index, owner) in owners.iter().enumerate() {
            for offset in 0..MAX_INCOMING_DELIVERIES_PER_LINK {
                reserve(
                    &mut ledger,
                    owner,
                    (index * MAX_INCOMING_DELIVERIES_PER_LINK + offset) as u32,
                );
            }
        }
        let new_owner = LinkIdentity::new();
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.reserve(&new_owner, 4_096, b"overflow"),
            Err(IncomingLedgerError::SessionLimitReached { maximum: 4_096 })
        );
        assert_eq!(snapshot(&ledger), before);
        assert_eq!(ledger.remove_link(&owners[0]), 1_024);
        reserve(&mut ledger, &new_owner, 4_096);
        assert_eq!(ledger.deliveries.len(), 3_073);
        snapshot(&ledger);
    }

    #[test]
    fn partial_complete_and_pending_ack_entries_all_consume_the_same_link_budget() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        for id in 0..MAX_INCOMING_DELIVERIES_PER_LINK as u32 {
            let identity = reserve(&mut ledger, &owner, id);
            if id % 3 != 0 {
                ledger
                    .complete(&identity, false, ReceiverSettleMode::Second)
                    .expect("complete delivery");
            }
            if id % 3 == 2 {
                ledger
                    .commit_settlement(&owner, &identity)
                    .expect("local outcome awaits sender acknowledgement");
            }
        }
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.reserve(&owner, 1_024, b"overflow"),
            Err(IncomingLedgerError::LinkLimitReached { maximum: 1_024 })
        );
        assert_eq!(snapshot(&ledger), before);
        assert_eq!(
            ledger.sender_settled_range(0, Some(u32::MAX)),
            SenderSettlement {
                matched: 1_024,
                released: 341,
            }
        );
        assert_eq!(ledger.deliveries.len(), 683);
        reserve(&mut ledger, &owner, 1_024);
        snapshot(&ledger);
    }

    #[test]
    fn partials_reserve_both_aliases_and_cannot_be_locally_settled() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let identity = reserve(&mut ledger, &owner, 9);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.settlement(&owner, &identity),
            Err(IncomingLedgerError::IncompleteDelivery)
        );
        assert_eq!(
            ledger.commit_settlement(&owner, &identity),
            Err(IncomingLedgerError::IncompleteDelivery)
        );
        assert_eq!(snapshot(&ledger), before);
    }

    #[test]
    fn wrong_owner_refusal_is_before_io_and_does_not_consume_identity() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let wrong = LinkIdentity::new();
        let identity = completed(&mut ledger, &owner, 12, ReceiverSettleMode::First);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.settlement(&wrong, &identity),
            Err(IncomingLedgerError::WrongOwner)
        );
        assert_eq!(
            ledger.commit_settlement(&wrong, &identity),
            Err(IncomingLedgerError::WrongOwner)
        );
        assert_eq!(snapshot(&ledger), before);
        assert_eq!(
            ledger.settlement(&owner, &identity),
            Ok(SettlementAction::SendDisposition { settled: true })
        );
    }

    #[test]
    fn preflight_is_read_only_and_first_settlement_is_terminal_and_idempotent() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let identity = completed(&mut ledger, &owner, 1, ReceiverSettleMode::First);
        let before = snapshot(&ledger);
        for _ in 0..3 {
            assert_eq!(
                ledger.settlement(&owner, &identity),
                Ok(SettlementAction::SendDisposition { settled: true })
            );
            assert_eq!(snapshot(&ledger), before);
        }
        ledger
            .commit_settlement(&owner, &identity)
            .expect("frame written");
        assert!(ledger.deliveries.is_empty());
        assert!(ledger.tags.is_empty());
        assert_eq!(identity.terminal(), Terminal::Settled);
        assert_eq!(
            ledger.settlement(&owner, &identity),
            Ok(SettlementAction::NoDisposition)
        );
        ledger
            .commit_settlement(&owner, &identity)
            .expect("owned terminal no-op");
    }

    #[test]
    fn second_settlement_waits_for_sender_ack_and_repeat_emits_nothing() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let identity = completed(&mut ledger, &owner, 5, ReceiverSettleMode::Second);
        assert_eq!(
            ledger.settlement(&owner, &identity),
            Ok(SettlementAction::SendDisposition { settled: false })
        );
        ledger
            .commit_settlement(&owner, &identity)
            .expect("outcome written");
        assert_eq!(ledger.deliveries[&5].phase, Phase::AwaitingSenderAck);
        assert_eq!(identity.terminal(), Terminal::Live);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.settlement(&owner, &identity),
            Ok(SettlementAction::NoDisposition)
        );
        ledger
            .commit_settlement(&owner, &identity)
            .expect("pending outcome no-op");
        assert_eq!(snapshot(&ledger), before);
        assert_eq!(
            ledger.sender_settled_range(5, None),
            SenderSettlement {
                matched: 1,
                released: 1
            }
        );
        assert_eq!(identity.terminal(), Terminal::Settled);
        assert!(ledger.deliveries.is_empty());
    }

    #[test]
    fn terminal_clone_cannot_settle_reused_numeric_id_on_the_same_open_link() {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut ledger = IncomingLedger::new();
            let owner = LinkIdentity::new();
            let old = completed(&mut ledger, &owner, u32::MAX, mode);
            let old_clone = old.clone();
            ledger
                .commit_settlement(&owner, &old)
                .expect("local outcome");
            ledger.sender_settled_range(u32::MAX, None);
            let current = completed(&mut ledger, &owner, u32::MAX, ReceiverSettleMode::First);
            let before = snapshot(&ledger);
            assert_eq!(
                ledger.settlement(&owner, &old_clone),
                Ok(SettlementAction::NoDisposition)
            );
            ledger
                .commit_settlement(&owner, &old_clone)
                .expect("old owned no-op");
            assert_eq!(snapshot(&ledger), before);
            assert_eq!(
                ledger.settlement(&owner, &current),
                Ok(SettlementAction::SendDisposition { settled: true })
            );
        }
    }

    #[test]
    fn sender_settled_completion_releases_aliases_but_retains_owned_noop_identity() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let identity = reserve(&mut ledger, &owner, 3);
        assert_eq!(
            ledger.complete(&identity, true, ReceiverSettleMode::Second),
            Ok(Completion::SenderSettled)
        );
        let current = completed(&mut ledger, &owner, 3, ReceiverSettleMode::Second);
        let before = snapshot(&ledger);
        ledger
            .commit_settlement(&owner, &identity)
            .expect("presettled owned no-op");
        assert_eq!(snapshot(&ledger), before);
        assert!(!identity.same_delivery(&current));
    }

    #[test]
    fn repeated_presettled_deliveries_retain_no_ledger_tombstones() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let first = ledger
            .reserve(&owner, 0, b"constant")
            .expect("first delivery");
        ledger
            .complete(&first, true, ReceiverSettleMode::First)
            .expect("first presettled completion");
        for _ in 0..2_048 {
            let identity = ledger
                .reserve(&owner, 0, b"constant")
                .expect("released alias");
            ledger
                .complete(&identity, true, ReceiverSettleMode::Second)
                .expect("presettled completion");
            assert!(ledger.deliveries.is_empty());
            assert!(ledger.tags.is_empty());
        }
        completed(&mut ledger, &owner, 0, ReceiverSettleMode::First);
        let before = snapshot(&ledger);
        ledger
            .commit_settlement(&owner, &first)
            .expect("original token remains an owned no-op");
        assert_eq!(snapshot(&ledger), before);
    }

    #[test]
    fn wrong_owner_is_rejected_even_when_the_foreign_token_is_already_settled() {
        let mut ledger = IncomingLedger::new();
        let original_owner = LinkIdentity::new();
        let new_owner = LinkIdentity::new();
        let identity = reserve(&mut ledger, &original_owner, 1);
        ledger
            .complete(&identity, true, ReceiverSettleMode::Second)
            .expect("sender settled delivery");
        completed(&mut ledger, &new_owner, 1, ReceiverSettleMode::First);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.settlement(&new_owner, &identity),
            Err(IncomingLedgerError::WrongOwner)
        );
        assert_eq!(
            ledger.commit_settlement(&new_owner, &identity),
            Err(IncomingLedgerError::WrongOwner)
        );
        assert_eq!(snapshot(&ledger), before);
    }

    #[test]
    fn early_sender_ack_keeps_partial_aliases_until_completion_or_abort() {
        for abort in [false, true] {
            let mut ledger = IncomingLedger::new();
            let owner = LinkIdentity::new();
            let identity = reserve(&mut ledger, &owner, 4);
            assert_eq!(
                ledger.sender_settled_range(4, None),
                SenderSettlement {
                    matched: 1,
                    released: 0
                }
            );
            assert_eq!(ledger.deliveries[&4].phase, Phase::Partial);
            assert!(ledger.deliveries[&4].remote_settled);
            assert_eq!(
                ledger.reserve(&owner, 4, b"other"),
                Err(IncomingLedgerError::DeliveryIdInUse { id: 4 })
            );
            if abort {
                ledger.abort(&identity).expect("abort still allowed");
                assert_eq!(identity.terminal(), Terminal::Aborted);
            } else {
                assert_eq!(
                    ledger.complete(&identity, false, ReceiverSettleMode::First),
                    Ok(Completion::SenderSettled)
                );
            }
            assert!(ledger.deliveries.is_empty());
            assert!(ledger.tags.is_empty());
        }
    }

    #[test]
    fn sender_settled_accessor_checks_live_generation_before_reading_remote_state() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let identity = reserve(&mut ledger, &owner, 4);
        assert_eq!(ledger.sender_is_settled(&identity), Ok(false));
        ledger.sender_settled_range(4, None);
        assert_eq!(ledger.sender_is_settled(&identity), Ok(true));

        let mut other_session = IncomingLedger::new();
        let other = reserve(&mut other_session, &owner, 4);
        assert_eq!(
            ledger.sender_is_settled(&other),
            Err(IncomingLedgerError::StaleDelivery)
        );
        assert_eq!(ledger.sender_is_settled(&identity), Ok(true));
        ledger.abort(&identity).expect("remove original generation");
        reserve(&mut ledger, &owner, 4);
        assert_eq!(
            ledger.sender_is_settled(&identity),
            Err(IncomingLedgerError::AbortedDelivery)
        );
        owner.retire();
        assert_eq!(
            ledger.sender_is_settled(&other),
            Err(IncomingLedgerError::RetiredLink)
        );
    }

    #[test]
    fn sender_settlement_before_application_outcome_avoids_unnecessary_disposition() {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut ledger = IncomingLedger::new();
            let owner = LinkIdentity::new();
            let identity = completed(&mut ledger, &owner, 6, mode);
            assert_eq!(
                ledger.sender_settled_range(6, None),
                SenderSettlement {
                    matched: 1,
                    released: 0
                }
            );
            assert_eq!(
                ledger.settlement(&owner, &identity),
                Ok(SettlementAction::NoDisposition)
            );
            ledger
                .commit_settlement(&owner, &identity)
                .expect("local app finishes");
            assert!(ledger.deliveries.is_empty());
            assert_eq!(identity.terminal(), Terminal::Settled);
        }
    }

    #[test]
    fn wrapping_ack_ranges_touch_only_existing_matching_incoming_ids() {
        let mut ledger = IncomingLedger::new();
        let first = LinkIdentity::new();
        let second = LinkIdentity::new();
        let mut identities = Vec::new();
        for (id, owner) in [
            (u32::MAX - 1, &first),
            (u32::MAX, &second),
            (0, &first),
            (1, &second),
            (2, &first),
        ] {
            let identity = completed(&mut ledger, owner, id, ReceiverSettleMode::Second);
            ledger
                .commit_settlement(owner, &identity)
                .expect("local outcome");
            identities.push(identity);
        }
        assert_eq!(
            ledger.sender_settled_range(u32::MAX, Some(1)),
            SenderSettlement {
                matched: 3,
                released: 3
            }
        );
        assert_eq!(ledger.deliveries.len(), 2);
        assert_eq!(identities[0].terminal(), Terminal::Live);
        assert_eq!(identities[4].terminal(), Terminal::Live);
        for identity in &identities[1..4] {
            assert_eq!(identity.terminal(), Terminal::Settled);
        }
        assert_eq!(
            ledger.sender_settled_range(0, Some(u32::MAX)),
            SenderSettlement {
                matched: 2,
                released: 2
            }
        );
        assert_eq!(
            ledger.sender_settled_range(0, Some(u32::MAX)),
            SenderSettlement::default()
        );
        assert!(ledger.tags.is_empty());
    }

    #[test]
    fn aborted_and_removed_tokens_reject_after_numeric_alias_reuse() {
        for abort in [false, true] {
            let mut ledger = IncomingLedger::new();
            let owner = LinkIdentity::new();
            let old = reserve(&mut ledger, &owner, 8);
            if abort {
                ledger.abort(&old).expect("abort partial");
            } else {
                ledger.remove(&old).expect("discard partial");
            }
            completed(&mut ledger, &owner, 8, ReceiverSettleMode::First);
            let before = snapshot(&ledger);
            let expected = if abort {
                IncomingLedgerError::AbortedDelivery
            } else {
                IncomingLedgerError::UnknownDelivery
            };
            assert_eq!(ledger.settlement(&owner, &old), Err(expected.clone()));
            assert_eq!(ledger.commit_settlement(&owner, &old), Err(expected));
            assert_eq!(snapshot(&ledger), before);
        }
    }

    #[test]
    fn link_retirement_invalidates_partial_complete_pending_and_terminal_tokens_only_for_owner() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let sibling = LinkIdentity::new();
        let partial = reserve(&mut ledger, &owner, 1);
        let complete = completed(&mut ledger, &owner, 2, ReceiverSettleMode::First);
        let pending = completed(&mut ledger, &owner, 3, ReceiverSettleMode::Second);
        ledger
            .commit_settlement(&owner, &pending)
            .expect("pending sender ack");
        let terminal = completed(&mut ledger, &owner, 4, ReceiverSettleMode::First);
        ledger
            .commit_settlement(&owner, &terminal)
            .expect("terminal token outside ledger");
        let survivor = completed(&mut ledger, &sibling, 5, ReceiverSettleMode::First);
        assert_eq!(ledger.remove_link(&owner), 3);
        assert_eq!(ledger.remove_link(&owner), 0);
        for token in [&partial, &complete, &pending, &terminal] {
            assert_eq!(
                ledger.settlement(&owner, token),
                Err(IncomingLedgerError::RetiredLink)
            );
        }
        assert_eq!(
            ledger.reserve(&owner, 1, b"reused"),
            Err(IncomingLedgerError::RetiredLink)
        );
        let replacement = LinkIdentity::new();
        completed(&mut ledger, &replacement, 1, ReceiverSettleMode::First);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.settlement(&replacement, &partial),
            Err(IncomingLedgerError::WrongOwner)
        );
        assert_eq!(snapshot(&ledger), before);
        assert_eq!(
            ledger.settlement(&sibling, &survivor),
            Ok(SettlementAction::SendDisposition { settled: true })
        );
    }

    #[test]
    fn stop_link_can_retire_identity_before_live_session_removes_its_entries() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let identity = completed(&mut ledger, &owner, 1, ReceiverSettleMode::First);
        owner.retire();
        owner.retire();
        assert_eq!(
            ledger.settlement(&owner, &identity),
            Err(IncomingLedgerError::RetiredLink)
        );
        assert_eq!(ledger.remove_link(&owner), 1);
        assert!(ledger.deliveries.is_empty());
        assert!(ledger.tags.is_empty());
        let replacement = LinkIdentity::new();
        completed(&mut ledger, &replacement, 1, ReceiverSettleMode::First);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.commit_settlement(&replacement, &identity),
            Err(IncomingLedgerError::WrongOwner)
        );
        assert_eq!(snapshot(&ledger), before);
    }

    #[test]
    fn completion_and_abort_phase_errors_preserve_live_state() {
        let mut ledger = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let identity = completed(&mut ledger, &owner, 11, ReceiverSettleMode::Second);
        let before = snapshot(&ledger);
        assert_eq!(
            ledger.complete(&identity, true, ReceiverSettleMode::First),
            Err(IncomingLedgerError::AlreadyComplete)
        );
        assert_eq!(
            ledger.abort(&identity),
            Err(IncomingLedgerError::AlreadyComplete)
        );
        assert_eq!(snapshot(&ledger), before);
    }

    #[test]
    fn same_id_on_a_different_session_cannot_accept_foreign_live_token() {
        let mut first = IncomingLedger::new();
        let mut second = IncomingLedger::new();
        let owner = LinkIdentity::new();
        let foreign = completed(&mut first, &owner, 0, ReceiverSettleMode::First);
        // Deliberately share an owner to prove the per-delivery token check is
        // independent of the normal fresh-link-generation protection.
        completed(&mut second, &owner, 0, ReceiverSettleMode::First);
        let before = snapshot(&second);
        assert_eq!(
            second.settlement(&owner, &foreign),
            Err(IncomingLedgerError::StaleDelivery)
        );
        assert_eq!(
            second.commit_settlement(&owner, &foreign),
            Err(IncomingLedgerError::StaleDelivery)
        );
        assert_eq!(snapshot(&second), before);
    }
}
