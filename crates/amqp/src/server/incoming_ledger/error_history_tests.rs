use std::collections::HashSet;

use super::*;

fn reserve(ledger: &mut IncomingLedger, owner: &LinkIdentity, id: u32) -> DeliveryIdentity {
    ledger
        .reserve(owner, id, &id.to_be_bytes())
        .expect("owned delivery")
}

#[test]
fn exact_owner_snapshot_includes_partial_complete_and_awaiting_sender_ack_only() {
    let mut ledger = IncomingLedger::new();
    let owner = LinkIdentity::new();
    let foreign = LinkIdentity::new();
    let partial = reserve(&mut ledger, &owner, u32::MAX);
    let complete = reserve(&mut ledger, &owner, 0);
    ledger
        .complete(&complete, false, ReceiverSettleMode::First)
        .expect("complete delivery");
    let awaiting = reserve(&mut ledger, &owner, 1);
    ledger
        .complete(&awaiting, false, ReceiverSettleMode::Second)
        .expect("second-mode delivery");
    ledger
        .commit_settlement(&owner, &awaiting)
        .expect("awaiting sender ACK");
    let presettled = reserve(&mut ledger, &owner, 2);
    ledger
        .complete(&presettled, true, ReceiverSettleMode::First)
        .expect("sender-settled delivery");
    let aborted = reserve(&mut ledger, &owner, 3);
    ledger.abort(&aborted).expect("aborted delivery");
    let terminal = reserve(&mut ledger, &owner, 4);
    ledger
        .complete(&terminal, false, ReceiverSettleMode::First)
        .expect("first-mode delivery");
    ledger
        .commit_settlement(&owner, &terminal)
        .expect("terminal first-mode settlement");
    reserve(&mut ledger, &foreign, 5);
    let before = (ledger.deliveries.len(), ledger.tags.clone());
    let ids: HashSet<_> = ledger.owned_live_ids(&owner).collect();
    assert_eq!(ids, HashSet::from([u32::MAX, 0, 1]));
    assert_eq!(
        ledger.owned_live_ids(&foreign).collect::<HashSet<_>>(),
        HashSet::from([5])
    );
    assert_eq!(ledger.owned_live_ids(&LinkIdentity::new()).count(), 0);
    assert_eq!((ledger.deliveries.len(), ledger.tags.clone()), before);
    assert_eq!(ledger.deliveries[&u32::MAX].phase, Phase::Partial);
    assert_eq!(ledger.deliveries[&0].phase, Phase::Complete);
    assert_eq!(ledger.deliveries[&1].phase, Phase::AwaitingSenderAck);
    assert_eq!(partial.terminal(), Terminal::Live);
    assert_eq!(complete.terminal(), Terminal::Live);
    assert_eq!(awaiting.terminal(), Terminal::Live);
    owner.retire();
    assert_eq!(ledger.owned_live_ids(&owner).count(), 0);
    assert_eq!(ledger.owned_live_ids(&foreign).count(), 1);
}

#[test]
fn snapshot_rejects_miskeyed_foreign_and_terminal_records_without_mutating_them() {
    let mut ledger = IncomingLedger::new();
    let owner = LinkIdentity::new();
    let foreign = LinkIdentity::new();
    let original = reserve(&mut ledger, &owner, 7);
    let terminal = reserve(&mut ledger, &owner, 8);
    reserve(&mut ledger, &foreign, 9);
    let miskeyed = ledger.deliveries.remove(&7).expect("original record");
    ledger.deliveries.insert(70, miskeyed);
    // Defensive snapshots must not bless malformed internal aliases.
    terminal.mark_terminal(Terminal::Settled);
    let before = (ledger.deliveries.len(), ledger.tags.clone());
    assert!(ledger.owned_live_ids(&owner).next().is_none());
    assert_eq!(
        ledger.owned_live_ids(&foreign).collect::<HashSet<_>>(),
        HashSet::from([9])
    );
    assert_eq!((ledger.deliveries.len(), ledger.tags.clone()), before);
    assert!(ledger.deliveries[&70].identity.same_delivery(&original));
    assert_eq!(terminal.terminal(), Terminal::Settled);
}

#[test]
fn maximum_known_live_snapshot_remains_bounded_across_wrapping_ids() {
    let mut ledger = IncomingLedger::new();
    let owners: Vec<_> = (0..4).map(|_| LinkIdentity::new()).collect();
    let first = u32::MAX - 2_047;
    for (index, owner) in owners.iter().enumerate() {
        for offset in 0..MAX_INCOMING_DELIVERIES_PER_LINK {
            let id = first.wrapping_add((index * MAX_INCOMING_DELIVERIES_PER_LINK + offset) as u32);
            reserve(&mut ledger, owner, id);
        }
    }
    assert_eq!(ledger.deliveries.len(), MAX_INCOMING_DELIVERIES_PER_SESSION);
    let mut union = HashSet::new();
    for owner in &owners {
        let ids: HashSet<_> = ledger.owned_live_ids(owner).collect();
        assert_eq!(ids.len(), MAX_INCOMING_DELIVERIES_PER_LINK);
        union.extend(ids);
    }
    assert_eq!(union.len(), MAX_INCOMING_DELIVERIES_PER_SESSION);
    assert!(union.contains(&u32::MAX) && union.contains(&0));
    assert_eq!(ledger.deliveries.len(), MAX_INCOMING_DELIVERIES_PER_SESSION);
}
