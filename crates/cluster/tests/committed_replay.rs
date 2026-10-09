//! Externally serialized Memory/Fjall replay controls, not power-cut or CAS proof.
//! Raw edits and fault wrappers are local setup; every Fjall handle drops on reopen.

use std::{
    collections::BTreeMap,
    error::Error,
    panic::{AssertUnwindSafe, catch_unwind, panic_any},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use cluster::{
    CommittedReplay, JOURNAL_VALIDATION_PAGE_ENTRIES, Journal, JournalError,
    MAX_JOURNAL_READ_ENTRIES, ReplayError, ReplayProgress,
};
use domain::{
    BoundCommand, BrokerError, Command, CommandKind, DurableProposal, DurableProposalError,
    EntityPath, IndexedApplyError, IndexedApplyOutcome, IndexedWriter, MAX_SUBSCRIPTION_RULES,
    MAX_TOPIC_SUBSCRIPTIONS, NamespaceName, QueueConfig, ReceiveMode, RuleDefinition, RuleFilter,
    RuleName, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec,
    keys,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use storage::{
    FjallStore, Key, MemoryStore, Mutation, StateStore, StorageError, StoreSnapshot, Value,
    WriteBatch,
};
use tempfile::TempDir;

#[path = "committed_replay/fixtures.rs"]
mod fixtures;
use fixtures::*;

fn adoption_and_adapter(durable: bool) -> TestResult {
    for tail in [false, true] {
        let fixture = Fixture::new(durable)?;
        if tail {
            fixture.log(&[vec![0xAA]], 0);
        }
        let before = fixture.snapshot();
        let original = fixture.observed();
        assert_eq!(original.snapshot()?.entries(), before.as_slice());
        fixture.reset();
        let mut owner = CommittedReplay::open(original)?;
        assert_eq!(fixture.snapshot(), before);
        assert_eq!(fixture.control.clone_calls.load(Ordering::SeqCst), 0);
        assert!(fixture.trace().prefixes.contains(&(vec![0xF1], 3)));
        assert!(
            fixture
                .trace()
                .scans
                .iter()
                .all(|(prefix, _, _)| prefix == F0)
        );
        caught_up(&mut owner, &fixture, 0, u64::from(tail));
    }
    for tag in 0x00..=0x11 {
        let fixture = Fixture::new(durable)?;
        fixture.raw(WriteBatch::default().put(vec![tag, 0xAA], vec![1]));
        let before = fixture.snapshot();
        assert!(matches!(
            fixture.open(),
            Err(ReplayError::Indexed(IndexedApplyError::PopulatedStore))
        ));
        assert_eq!(fixture.snapshot(), before);
        assert!(fixture.trace().batches.is_empty());
    }
    let fixture = Fixture::new(durable)?;
    fixture.control.prefix_fault.store(true, Ordering::SeqCst);
    assert!(
        matches!(fixture.open(), Err(ReplayError::Indexed(IndexedApplyError::Storage(error))) if error == injected_error())
    );
    assert_eq!(fixture.trace().prefixes, vec![(vec![0xF1], 3)]);
    assert!(fixture.snapshot().is_empty());
    assert_eq!(fixture.control.clone_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

fn structure_and_identity(durable: bool) -> TestResult {
    for case in 0..10 {
        let fixture = Fixture::new(durable)?;
        let originals = vec![create(10), noop(20)];
        fixture.proposals(&originals, 1);
        if case >= 3 {
            fixture.seed(&originals[..1]);
        }
        let batch = match case {
            0 => WriteBatch::default().delete(key(F0, 0)),
            1 => WriteBatch::default().put(key(F0, 1), vec![0]),
            2 => WriteBatch::default().put(entry_key(2), vec![0]),
            3 => WriteBatch::default().delete(key(F1, 0)),
            4 => WriteBatch::default().put(key(F1, 1), vec![0]),
            5 => WriteBatch::default().put(b"\xF1other".to_vec(), vec![0]),
            6 => WriteBatch::default().put(key(F1, 1), checkpoint(2, &originals[1])),
            7 => WriteBatch::default().put(key(F1, 1), checkpoint(1, &noop(10))),
            8 => WriteBatch::default().put(key(F0, 0), 2_u32.to_be_bytes().to_vec()),
            _ => WriteBatch::default().put(key(F1, 0), owner_record(2)),
        };
        fixture.raw(batch);
        let before = fixture.snapshot();
        let result = fixture.open();
        match case {
            0..=2 => assert!(matches!(result, Err(ReplayError::Journal(_)))),
            3..=5 => assert!(matches!(
                result,
                Err(ReplayError::Indexed(IndexedApplyError::Corrupt { .. }))
            )),
            6 => assert!(matches!(
                result,
                Err(ReplayError::AppliedAhead {
                    applied: 2,
                    committed: 1
                })
            )),
            7 => assert!(matches!(
                result,
                Err(ReplayError::Indexed(
                    IndexedApplyError::ConflictingProposal { index: 1 }
                ))
            )),
            8 => assert!(matches!(
                result,
                Err(ReplayError::Journal(JournalError::UnsupportedVersion {
                    found: 2,
                    ..
                }))
            )),
            _ => assert!(matches!(
                result,
                Err(ReplayError::Indexed(
                    IndexedApplyError::UnsupportedVersion { found: 2, .. }
                ))
            )),
        }
        assert_eq!(fixture.snapshot(), before);
        assert!(fixture.trace().batches.is_empty());
    }
    let fixture = Fixture::new(durable)?;
    fixture.seed(&[create(10)]);
    let before = fixture.snapshot();
    assert!(matches!(
        fixture.open(),
        Err(ReplayError::AppliedAhead {
            applied: 1,
            committed: 0
        })
    ));
    assert_eq!(fixture.snapshot(), before);
    assert!(fixture.trace().batches.is_empty());
    Ok(())
}

fn malformed_proposals() -> Vec<Vec<u8>> {
    let original = noop(10).encode().unwrap();
    let mut version = original.clone();
    version[7] = 2;
    let mut length = original.clone();
    length[11] ^= 1;
    let mut trailing = original.clone();
    trailing.push(0);
    let mut alias = original.clone();
    alias[12] |= 128;
    alias.insert(13, 0);
    alias[11] += 1;
    let mut tag = original.clone();
    tag[12] = 127;
    assert_eq!(
        DurableProposal::decode(&version),
        Err(DurableProposalError::UnsupportedVersion(2))
    );
    assert_eq!(
        DurableProposal::decode(&alias),
        Err(DurableProposalError::NonCanonical)
    );
    let machine = StateMachine::new(MemoryStore::default());
    let first = create(10);
    machine.apply(first.command()).unwrap();
    let binding = machine.bind_entity(&ns(), &queue()).unwrap();
    let mut authority =
        DurableProposal::bound(BoundCommand::new(binding, receive(20).command().clone()))
            .unwrap()
            .encode()
            .unwrap();
    // Equal-length command namespace drift preserves framing/canonical encoding.
    let command_namespace = authority
        .windows(6)
        .rposition(|bytes| bytes == b"tenant")
        .unwrap();
    authority[command_namespace..command_namespace + 6].copy_from_slice(b"otherx");
    assert_eq!(
        DurableProposal::decode(&authority),
        Err(DurableProposalError::InvalidAuthority)
    );
    vec![
        Vec::new(),
        vec![0xAA],
        version,
        length,
        trailing,
        alias,
        tag,
        authority,
    ]
}

fn committed_schema_pages(durable: bool) -> TestResult {
    for malformed in malformed_proposals() {
        for (bad_index, applied) in [(1, 0), (33, 0), (70, 0), (1, 2)] {
            let fixture = Fixture::new(durable)?;
            let originals = (1..=70)
                .map(|at| if at == 1 { create(at) } else { noop(at) })
                .collect::<Vec<_>>();
            let mut payloads = originals
                .iter()
                .map(|p| p.encode().unwrap())
                .collect::<Vec<_>>();
            payloads[bad_index - 1] = malformed.clone();
            fixture.log(&payloads, 70);
            if applied > 0 {
                fixture.seed(&originals[..applied]);
            }
            let before = fixture.snapshot();
            assert!(
                matches!(fixture.open(), Err(ReplayError::Proposal { index, .. }) if index == bad_index as u64)
            );
            assert_eq!(fixture.snapshot(), before);
            assert!(fixture.trace().batches.is_empty());
            assert!(
                fixture
                    .trace()
                    .scans
                    .iter()
                    .all(|(_, _, limit)| *limit <= JOURNAL_VALIDATION_PAGE_ENTRIES)
            );
        }
    }
    // A structurally oversized raw tail is a journal error, never opaque adoption.
    let fixture = Fixture::new(durable)?;
    fixture.proposals(&[create(10)], 1);
    let oversized = vec![0; cluster::MAX_JOURNAL_PAYLOAD_BYTES + 1];
    fixture.raw(WriteBatch::default().put(entry_key(2), entry_value(2, &oversized)));
    let before = fixture.snapshot();
    assert!(matches!(fixture.open(), Err(ReplayError::Journal(_))));
    assert_eq!(fixture.snapshot(), before);
    assert!(fixture.trace().batches.is_empty());
    Ok(())
}

fn opaque_tail_and_reopen(durable: bool) -> TestResult {
    for tail in [Vec::new(), vec![0xAA, 0xBB]] {
        let mut fixture = Fixture::new(durable)?;
        fixture.log(&[create(10).encode()?, tail], 1);
        let before = fixture.snapshot();
        let mut owner = fixture.open()?;
        assert_eq!(owner.last_appended_index(), Ok(2));
        assert_eq!(owner.replay_batch(64)?, progress(1, 1, 1));
        frozen_f0(&fixture, &before);
        caught_up(&mut owner, &fixture, 1, 2);
        let applied = fixture.snapshot();
        drop(owner);
        fixture.reopen()?;
        let mut journal = Journal::open(fixture.store().clone())?;
        journal.commit(2)?;
        drop(journal);
        let committed = fixture.snapshot();
        fixture.reset();
        assert!(matches!(
            fixture.open(),
            Err(ReplayError::Proposal { index: 2, .. })
        ));
        assert_eq!(fixture.snapshot(), committed);
        assert!(fixture.trace().batches.is_empty());
        assert_eq!(reserved(&applied, F1), reserved(&committed, F1));
    }
    Ok(())
}

fn limits_and_progress(durable: bool) -> TestResult {
    let mut fixture = Fixture::new(durable)?;
    let originals = (1..=70)
        .map(|at| if at == 1 { create(at) } else { noop(at) })
        .collect::<Vec<_>>();
    let mut payloads = originals
        .iter()
        .map(|p| p.encode().unwrap())
        .collect::<Vec<_>>();
    payloads.push(vec![0xAA]);
    fixture.log(&payloads, 70);
    let before = fixture.snapshot();
    let mut owner = fixture.open()?;
    let opened = fixture.trace();
    let validation = opened
        .scans
        .iter()
        .filter(|(prefix, _, _)| prefix == &key(F0, 2))
        .collect::<Vec<_>>();
    assert_eq!(validation.len(), 3);
    assert!(validation.windows(2).all(|pages| pages[0].1 < pages[1].1));
    assert!(validation.iter().all(|(_, _, limit)| *limit <= 32));
    for (at, limit, expected) in [(0, 1, 1), (1, 32, 33), (33, 64, 70), (70, 64, 70)] {
        let rows = fixture.snapshot();
        fixture.reset();
        for invalid in [0, 65, usize::MAX] {
            assert_eq!(
                owner.replay_batch(invalid),
                Err(ReplayError::InvalidBatchLimit {
                    requested: invalid,
                    maximum: 64
                })
            );
        }
        assert_eq!(owner.applied_index(), Ok(at));
        assert_eq!(owner.committed_index(), Ok(70));
        assert_eq!(owner.last_appended_index(), Ok(71));
        no_io(&fixture);
        assert_eq!(fixture.snapshot(), rows);
        assert_eq!(
            owner.replay_batch(limit)?,
            progress(expected, 70, (expected - at) as usize)
        );
        assert_eq!(fixture.trace().batches.len(), (expected - at) as usize);
        if at < 70 {
            assert_eq!(
                fixture.trace().scans[0],
                (
                    key(F0, 2),
                    entry_key(at + 1),
                    (70 - at).min(limit as u64) as usize
                )
            );
        }
        frozen_f0(&fixture, &before);
    }
    let complete = fixture.snapshot();
    drop(owner);
    fixture.reopen()?;
    let mut reopened = fixture.open()?;
    caught_up(&mut reopened, &fixture, 70, 71);
    assert_eq!(fixture.snapshot(), complete);
    Ok(())
}

#[derive(Serialize)]
struct Head {
    generation: u64,
    kind: u8,
    retired: bool,
}

fn authority_time_and_parity(durable: bool) -> TestResult {
    for use_bound in [false, true] {
        let actual = Fixture::new(durable)?;
        let reference = Fixture::new(durable)?;
        let first = create(10);
        actual.seed(std::slice::from_ref(&first));
        reference.seed(std::slice::from_ref(&first));
        let mut originals = vec![first];
        for original in [
            send(20),
            receive(30),
            proposal(&queue(), 200, CommandKind::ExpireLocks),
            receive(5),
        ] {
            originals.push(if use_bound {
                bound(&actual, original)
            } else {
                original
            });
        }
        actual.proposals(&originals, 5);
        reference.proposals(&originals, 5);
        actual.raw(WriteBatch::default().put(b"outside-reserved".to_vec(), b"unchanged".to_vec()));
        reference
            .raw(WriteBatch::default().put(b"outside-reserved".to_vec(), b"unchanged".to_vec()));
        let before = actual.snapshot();
        let mut owner = actual.open()?;
        let mut writer = IndexedWriter::open(reference.observed())?;
        actual.reset();
        reference.reset();
        for (offset, original) in originals.iter().enumerate().skip(1) {
            let outcome = writer.apply(offset as u64 + 1, original)?;
            if offset == 4 {
                assert!(matches!(
                    outcome,
                    IndexedApplyOutcome::Refused(BrokerError::ClockRegression { .. })
                ));
            }
        }
        assert_eq!(owner.replay_batch(64)?, progress(5, 5, 4));
        assert_eq!(actual.trace().batches, reference.trace().batches);
        assert_eq!(actual.snapshot(), reference.snapshot());
        frozen_f0(&actual, &before);
        assert_eq!(
            StateMachine::new(actual.store().clone()).last_applied_time()?,
            Timestamp::from_millis(200)
        );
        caught_up(&mut owner, &actual, 5, 5);
    }
    let fixture = Fixture::new(durable)?;
    let first = create(10);
    fixture.seed(std::slice::from_ref(&first));
    let retained = bound(&fixture, receive(100));
    fixture.proposals(&[first, retained.clone(), noop(200)], 3);
    fixture.raw(
        WriteBatch::default()
            .put(
                keys::entity_metadata(&ns(), &queue()),
                codec::encode(&Head {
                    generation: 2,
                    kind: 0,
                    retired: false,
                })?,
            )
            .put(keys::clock(), vec![255]),
    );
    let before = fixture.snapshot();
    let mut owner = fixture.open()?;
    fixture.reset();
    assert_eq!(owner.replay_batch(1)?, progress(2, 3, 1));
    let expected = WriteBatch::default().put(key(F1, 1), checkpoint(2, &retained));
    assert_eq!(fixture.trace().batches, vec![expected.clone()]);
    assert!(!fixture.trace().gets.contains(&keys::clock()));
    assert_eq!(fixture.snapshot(), apply_rows(&before, &expected));
    assert!(matches!(
        owner.replay_batch(1),
        Err(ReplayError::Indexed(IndexedApplyError::Domain(
            BrokerError::Codec(_)
        )))
    ));
    let fatal = fixture.snapshot();
    unusable(&mut owner, &fixture);
    assert_eq!(fixture.snapshot(), fatal);
    frozen_f0(&fixture, &before);
    Ok(())
}

#[derive(Clone, Copy)]
enum Catalog {
    Rules,
    Subscriptions,
}

impl Catalog {
    fn create(self, at: u64) -> DurableProposal {
        match self {
            Self::Rules => proposal(
                &subscription(),
                at,
                CommandKind::CreateRule {
                    name: RuleName::new("new-rule").unwrap(),
                    filter: RuleFilter::True,
                },
            ),
            Self::Subscriptions => proposal(
                &topic(),
                at,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("new-child").unwrap(),
                    config: SubscriptionConfig::default(),
                },
            ),
        }
    }
}

fn capacity_setup(fixture: &Fixture, catalog: Catalog) -> Vec<DurableProposal> {
    let originals = vec![
        proposal(
            &topic(),
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        proposal(
            &topic(),
            20,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker").unwrap(),
                config: SubscriptionConfig::default(),
            },
        ),
    ];
    fixture.seed(&originals);
    let mut batch = WriteBatch::default();
    match catalog {
        Catalog::Rules => {
            for offset in 1..MAX_SUBSCRIPTION_RULES {
                let name = RuleName::new(format!("rule-{offset:04}")).unwrap();
                batch.push_put(
                    keys::subscription_rule(&ns(), &subscription(), &name),
                    codec::encode(&RuleDefinition {
                        name,
                        filter: RuleFilter::True,
                        created_at: Timestamp::from_millis(20),
                    })
                    .unwrap(),
                );
            }
        }
        Catalog::Subscriptions => {
            let store = fixture.store();
            let default = RuleName::new(domain::DEFAULT_RULE_NAME).unwrap();
            let backing = store
                .get(&keys::queue_config(&ns(), &subscription()))
                .unwrap()
                .unwrap();
            let shadow = store
                .get(&keys::queue_config(
                    &ns(),
                    &subscription().dead_letter_queue().unwrap(),
                ))
                .unwrap()
                .unwrap();
            let head = store
                .get(&keys::entity_metadata(&ns(), &subscription()))
                .unwrap()
                .unwrap();
            let rule = store
                .get(&keys::subscription_rule(&ns(), &subscription(), &default))
                .unwrap()
                .unwrap();
            // Mirror profiles actually created above, not thousands of separate fsyncs.
            for offset in 1..MAX_TOPIC_SUBSCRIPTIONS {
                let name = SubscriptionName::new(format!("child-{offset:04}")).unwrap();
                let entity = topic().subscription(&name).unwrap();
                batch.push_put(
                    keys::topic_subscription(&ns(), &topic(), &name),
                    codec::encode(&entity).unwrap(),
                );
                batch.push_put(keys::queue_config(&ns(), &entity), backing.clone());
                batch.push_put(
                    keys::queue_config(&ns(), &entity.dead_letter_queue().unwrap()),
                    shadow.clone(),
                );
                batch.push_put(keys::entity_metadata(&ns(), &entity), head.clone());
                batch.push_put(
                    keys::subscription_rule(&ns(), &entity, &default),
                    rule.clone(),
                );
            }
        }
    }
    fixture.raw(batch);
    originals
}

fn corrupt_capacity(fixture: &Fixture, catalog: Catalog, overfull: bool) {
    let batch = match catalog {
        Catalog::Rules => {
            let name = RuleName::new(if overfull { "zz-overfull" } else { "rule-0001" }).unwrap();
            let value = if overfull {
                codec::encode(&RuleDefinition {
                    name: name.clone(),
                    filter: RuleFilter::True,
                    created_at: Timestamp::from_millis(20),
                })
                .unwrap()
            } else {
                vec![255]
            };
            WriteBatch::default().put(
                keys::subscription_rule(&ns(), &subscription(), &name),
                value,
            )
        }
        Catalog::Subscriptions => {
            let name =
                SubscriptionName::new(if overfull { "zz-overfull" } else { "worker" }).unwrap();
            WriteBatch::default().put(keys::topic_subscription(&ns(), &topic(), &name), vec![255])
        }
    };
    fixture.raw(batch);
}

fn capacity_origins(durable: bool) -> TestResult {
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        for use_bound in [false, true] {
            for fault in 0..3 {
                let mut fixture = Fixture::new(durable)?;
                let mut originals = capacity_setup(&fixture, catalog);
                let original = catalog.create(30);
                let original = if use_bound {
                    bound(&fixture, original)
                } else {
                    original
                };
                originals.push(original.clone());
                originals.push(noop(40));
                fixture.proposals(&originals, 4);
                if fault > 0 {
                    corrupt_capacity(&fixture, catalog, fault == 2);
                }
                let before = fixture.snapshot();
                let mut owner = fixture.open()?;
                fixture.reset();
                if fault == 0 {
                    assert_eq!(owner.replay_batch(1)?, progress(3, 4, 1));
                    let expected = WriteBatch::default().put(key(F1, 1), checkpoint(3, &original));
                    assert_eq!(fixture.trace().batches, vec![expected.clone()]);
                    assert_eq!(fixture.snapshot(), apply_rows(&before, &expected));
                    assert_eq!(
                        fixture.store().get(&keys::clock())?,
                        Some(codec::encode(&Timestamp::from_millis(20))?)
                    );
                    assert_eq!(owner.replay_batch(1)?, progress(4, 4, 1));
                } else {
                    assert!(matches!(
                        owner.replay_batch(64),
                        Err(ReplayError::Indexed(IndexedApplyError::Domain(_)))
                    ));
                    assert!(fixture.trace().batches.is_empty());
                    assert_eq!(fixture.snapshot(), before);
                    unusable(&mut owner, &fixture);
                }
                frozen_f0(&fixture, &before);
                let physical = fixture.snapshot();
                drop(owner);
                fixture.reopen()?;
                assert_eq!(fixture.snapshot(), physical);
                let mut reopened = fixture.open()?;
                assert_eq!(reopened.applied_index()?, if fault == 0 { 4 } else { 2 });
                if fault > 0 {
                    assert!(reopened.replay_batch(1).is_err());
                    unusable(&mut reopened, &fixture);
                }
            }
        }
    }
    // An unselected path remains usable even with a corrupt/overfull rule catalog.
    let fixture = Fixture::new(durable)?;
    let mut originals = capacity_setup(&fixture, Catalog::Rules);
    corrupt_capacity(&fixture, Catalog::Rules, true);
    let unselected = noop(30);
    originals.push(unselected.clone());
    originals.push(proposal(
        &subscription(),
        40,
        CommandKind::ListRules {
            skip: 0,
            max_rules: 1,
        },
    ));
    originals.push(noop(50));
    fixture.proposals(&originals, 5);
    let before = fixture.snapshot();
    let mut owner = fixture.open()?;
    fixture.reset();
    assert_eq!(owner.replay_batch(1)?, progress(3, 5, 1));
    let unselected_batch = WriteBatch::default().put(key(F1, 1), checkpoint(3, &unselected));
    assert_eq!(fixture.trace().batches, vec![unselected_batch.clone()]);
    let before_fatal = apply_rows(&before, &unselected_batch);
    assert_eq!(fixture.snapshot(), before_fatal);
    fixture.reset();
    // Same cap variant from a noncreation selected reader is fatal, never a refusal.
    assert!(matches!(
        owner.replay_batch(64),
        Err(ReplayError::Indexed(IndexedApplyError::Domain(
            BrokerError::RuleLimitExceeded { .. }
        )))
    ));
    assert_eq!(fixture.snapshot(), before_fatal);
    assert!(fixture.trace().batches.is_empty());
    unusable(&mut owner, &fixture);
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        let mut fixture = Fixture::new(durable)?;
        let mut originals = capacity_setup(&fixture, catalog);
        let created = create(25);
        let mut writer = IndexedWriter::open(fixture.store().clone())?;
        writer.apply(3, &created)?;
        drop(writer);
        originals.push(created);
        let successful = send(30);
        originals.push(successful.clone());
        originals.push(catalog.create(40));
        originals.push(noop(50));
        fixture.proposals(&originals, 6);
        corrupt_capacity(&fixture, catalog, false);
        let before = fixture.snapshot();
        let mut owner = fixture.open()?;
        fixture.reset();
        assert!(matches!(
            owner.replay_batch(64),
            Err(ReplayError::Indexed(IndexedApplyError::Domain(_)))
        ));
        let batches = fixture.trace().batches;
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].mutations().last(),
            Some(&Mutation::Put {
                key: key(F1, 1),
                value: checkpoint(4, &successful)
            })
        );
        let prefix = apply_rows(&before, &batches[0]);
        assert_eq!(fixture.snapshot(), prefix);
        frozen_f0(&fixture, &before);
        unusable(&mut owner, &fixture);
        drop(owner);
        fixture.reopen()?;
        let mut reopened = fixture.open()?;
        assert_eq!(reopened.applied_index()?, 4);
        fixture.reset();
        assert!(reopened.replay_batch(64).is_err());
        assert_eq!(fixture.snapshot(), prefix);
        assert!(fixture.trace().batches.is_empty());
        unusable(&mut reopened, &fixture);
    }
    Ok(())
}

fn read_failures_retire(durable: bool) -> TestResult {
    for later in [false, true] {
        for fault in 1..=7 {
            let mut fixture = Fixture::new(durable)?;
            fixture.proposals(&[create(10), send(20), noop(30)], 3);
            let mut owner = fixture.open()?;
            if later {
                owner.replay_batch(1)?;
            }
            let before = fixture.snapshot();
            fixture.reset();
            fixture.control.read_fault.store(fault, Ordering::SeqCst);
            if matches!(fault, 2 | 4) {
                let caught = catch_unwind(AssertUnwindSafe(|| owner.replay_batch(64)))
                    .expect_err("raw original read unwind");
                assert!(Arc::ptr_eq(
                    caught.downcast_ref::<Arc<str>>().unwrap(),
                    &fixture.control.payload
                ));
            } else {
                let error = owner.replay_batch(64).unwrap_err();
                if fault == 1 {
                    assert_eq!(
                        error,
                        ReplayError::Journal(JournalError::Storage(injected_error()))
                    );
                } else if fault == 3 {
                    assert_eq!(
                        error,
                        ReplayError::Indexed(IndexedApplyError::Domain(BrokerError::Storage(
                            injected_error()
                        )))
                    );
                } else if fault == 7 {
                    assert!(
                        matches!(error, ReplayError::Proposal { index, .. } if index == 1 + u64::from(later))
                    );
                } else {
                    assert!(matches!(
                        error,
                        ReplayError::Journal(JournalError::Corrupt { .. })
                    ));
                }
            }
            assert_eq!(fixture.control.read_fault.load(Ordering::SeqCst), 0);
            assert!(fixture.trace().batches.is_empty());
            assert_eq!(fixture.snapshot(), before);
            frozen_f0(&fixture, &before);
            unusable(&mut owner, &fixture);
            drop(owner);
            fixture.reopen()?;
            let mut reopened = fixture.open()?;
            assert_eq!(reopened.applied_index()?, u64::from(later));
            assert_eq!(
                reopened.replay_batch(64)?,
                progress(3, 3, 3 - usize::from(later))
            );
            caught_up(&mut reopened, &fixture, 3, 3);
        }
    }
    Ok(())
}

fn ambiguity(durable: bool, raw: bool) -> TestResult {
    for later in [false, true] {
        for shape in 0..3 {
            for after in [false, true] {
                let mut fixture = Fixture::new(durable)?;
                let original = match (later, shape) {
                    (false, 0) => create(30),
                    (true, 0) => send(30),
                    (_, 1) => noop(30),
                    (false, _) => receive(30),
                    (true, _) => create(30),
                };
                let index = 1 + u64::from(later);
                let mut originals = Vec::new();
                if later {
                    originals.push(create(10));
                }
                originals.push(original.clone());
                originals.push(noop(40));
                fixture.proposals(&originals, index + 1);
                let mut owner = fixture.open()?;
                if later {
                    owner.replay_batch(1)?;
                }
                let before = fixture.snapshot();
                fixture.reset();
                fixture.control.apply_fault.store(
                    if raw {
                        3 + usize::from(after)
                    } else {
                        1 + usize::from(after)
                    },
                    Ordering::SeqCst,
                );
                if raw {
                    let caught = catch_unwind(AssertUnwindSafe(|| owner.replay_batch(64)))
                        .expect_err("original raw apply unwind");
                    assert!(Arc::ptr_eq(
                        caught.downcast_ref::<Arc<str>>().unwrap(),
                        &fixture.control.payload
                    ));
                } else {
                    assert_eq!(
                        owner.replay_batch(64),
                        Err(ReplayError::Indexed(IndexedApplyError::Storage(
                            injected_error()
                        )))
                    );
                }
                assert_eq!(fixture.control.apply_fault.load(Ordering::SeqCst), 0);
                let batches = fixture.trace().batches;
                assert_eq!(batches.len(), 1);
                assert_eq!(
                    batches[0].mutations().last(),
                    Some(&Mutation::Put {
                        key: key(F1, 1),
                        value: checkpoint(index, &original)
                    })
                );
                if shape > 0 {
                    assert!(batches[0].mutations().iter().all(|m| match m {
                        Mutation::Put { key, .. } | Mutation::Delete { key } => key.starts_with(F1),
                    }));
                }
                let complete = apply_rows(&before, &batches[0]);
                let physical = if after { &complete } else { &before };
                assert_eq!(&fixture.snapshot(), physical);
                frozen_f0(&fixture, &before);
                unusable(&mut owner, &fixture);
                assert_eq!(&fixture.snapshot(), physical);
                drop(owner);
                fixture.reopen()?;
                assert_eq!(&fixture.snapshot(), physical);
                let mut reopened = fixture.open()?;
                assert_eq!(reopened.applied_index()?, index - u64::from(!after));
                fixture.reset();
                if !after {
                    assert_eq!(reopened.replay_batch(1)?, progress(index, index + 1, 1));
                    assert_eq!(fixture.trace().batches, batches);
                    assert_eq!(fixture.snapshot(), complete);
                }
                assert_eq!(reopened.replay_batch(1)?, progress(index + 1, index + 1, 1));
                let final_rows = fixture.snapshot();
                caught_up(&mut reopened, &fixture, index + 1, index + 1);
                drop(reopened);
                fixture.reopen()?;
                let mut final_owner = fixture.open()?;
                assert_eq!(fixture.snapshot(), final_rows);
                caught_up(&mut final_owner, &fixture, index + 1, index + 1);
            }
        }
    }
    Ok(())
}

fn returned_ambiguity(durable: bool) -> TestResult {
    ambiguity(durable, false)
}
fn raw_ambiguity(durable: bool) -> TestResult {
    ambiguity(durable, true)
}

fn latest_marker_no_domain_reads(durable: bool) -> TestResult {
    for conflict in [false, true] {
        let mut fixture = Fixture::new(durable)?;
        let original = create(10);
        fixture.seed(std::slice::from_ref(&original));
        fixture.proposals(&[if conflict { create(11) } else { original }], 1);
        // Serialized corruption exists before ownership; this proves no global health.
        fixture.raw(
            WriteBatch::default()
                .put(keys::clock(), vec![255])
                .put(keys::entity_metadata(&ns(), &queue()), vec![255]),
        );
        let before = fixture.snapshot();
        fixture.control.read_fault.store(3, Ordering::SeqCst);
        match fixture.open() {
            Ok(mut owner) => {
                assert!(!conflict);
                caught_up(&mut owner, &fixture, 1, 1);
                drop(owner);
            }
            Err(error) => {
                assert!(conflict);
                assert_eq!(
                    error,
                    ReplayError::Indexed(IndexedApplyError::ConflictingProposal { index: 1 })
                );
            }
        }
        assert_eq!(fixture.control.read_fault.load(Ordering::SeqCst), 3);
        assert_eq!(fixture.snapshot(), before);
        assert!(fixture.trace().batches.is_empty());
        fixture.reopen()?;
        match fixture.open() {
            Ok(mut owner) => {
                assert!(!conflict);
                caught_up(&mut owner, &fixture, 1, 1);
            }
            Err(error) => {
                assert!(conflict);
                assert_eq!(
                    error,
                    ReplayError::Indexed(IndexedApplyError::ConflictingProposal { index: 1 })
                );
            }
        }
        assert_eq!(fixture.control.read_fault.load(Ordering::SeqCst), 3);
        assert_eq!(fixture.snapshot(), before);
    }
    Ok(())
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() -> TestResult {
            $body(false)
        }
        #[test]
        fn $fjall() -> TestResult {
            $body(true)
        }
    };
}

paired!(
    memory_empty_adoption_moves_original_and_forwards_prefix,
    fjall_empty_adoption_moves_original_and_forwards_prefix,
    adoption_and_adapter
);
paired!(
    memory_structure_frontier_and_latest_identity_refuse_without_edits,
    fjall_structure_frontier_and_latest_identity_refuse_without_edits,
    structure_and_identity
);
paired!(
    memory_all_committed_schemas_validate_before_effects,
    fjall_all_committed_schemas_validate_before_effects,
    committed_schema_pages
);
paired!(
    memory_opaque_tail_requires_new_commit_and_reopen,
    fjall_opaque_tail_requires_new_commit_and_reopen,
    opaque_tail_and_reopen
);
paired!(
    memory_bounded_batches_and_invalid_limits_preserve_progress,
    fjall_bounded_batches_and_invalid_limits_preserve_progress,
    limits_and_progress
);
paired!(
    memory_original_authority_time_and_full_batch_parity,
    fjall_original_authority_time_and_full_batch_parity,
    authority_time_and_parity
);
paired!(
    memory_healthy_capacity_only_checkpoints_proven_creation,
    fjall_healthy_capacity_only_checkpoints_proven_creation,
    capacity_origins
);
paired!(
    memory_first_read_failure_retires_every_api_until_reopen,
    fjall_first_read_failure_retires_every_api_until_reopen,
    read_failures_retire
);
paired!(
    memory_returned_apply_ambiguity_preserves_full_rows_and_reopens,
    fjall_returned_apply_ambiguity_preserves_full_rows_and_reopens,
    returned_ambiguity
);
paired!(
    memory_raw_apply_unwind_keeps_payload_and_retires_until_reopen,
    fjall_raw_apply_unwind_keeps_payload_and_retires_until_reopen,
    raw_ambiguity
);
paired!(
    memory_latest_marker_is_outcome_free_and_domain_read_free,
    fjall_latest_marker_is_outcome_free_and_domain_read_free,
    latest_marker_no_domain_reads
);
