use super::*;

const MIB: u64 = domain::QUEUE_CAPACITY_GRANULARITY_BYTES;

fn finite(max_size_bytes: u64) -> domain::QueueCapacity {
    domain::QueueCapacity::Finite { max_size_bytes }
}

fn capacity(value: domain::QueueCapacity) -> CommandKind {
    CommandKind::UpdateQueue {
        update: domain::QueueConfigUpdate {
            capacity: Some(value),
            ..domain::QueueConfigUpdate::default()
        },
    }
}

fn replay(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let other = EntityPath::new("other")?;
    let originals = [
        proposal(
            &queue(),
            0,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    capacity: finite(MIB),
                    ..QueueConfig::default()
                },
            },
        ),
        proposal(
            &queue(),
            1,
            CommandKind::Send {
                message_id: "kept".into(),
                body: b"kept".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        ),
        proposal(&queue(), 2, capacity(finite(2 * MIB))),
        // Deterministic refusals: a mode change and an unsupported finite profile.
        proposal(&queue(), 3, capacity(domain::QueueCapacity::NonFinite)),
        proposal(
            &other,
            4,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    capacity: finite(MIB),
                    ..QueueConfig::default()
                },
            },
        ),
        proposal(
            &queue(),
            5,
            CommandKind::UpdateQueue {
                update: domain::QueueConfigUpdate {
                    max_delivery_count: Some(3),
                    ..domain::QueueConfigUpdate::default()
                },
            },
        ),
    ];
    for (p, version) in originals.iter().zip([7, 1, 7, 7, 7, 1]) {
        let bytes = p.encode()?;
        assert_eq!(bytes[7], version);
        assert_eq!(DurableProposal::decode(&bytes)?, *p);
    }
    fixture.proposals(&originals, originals.len() as u64);
    let frozen = fixture.snapshot();
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(3)?, progress(3, 6, 3));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(3)?, progress(6, 6, 3));
    frozen_f0(&fixture, &frozen);
    let machine = StateMachine::new(fixture.store().clone());
    assert_eq!(
        machine.queue_config(&ns(), &queue())?,
        Some(QueueConfig {
            max_delivery_count: 3,
            capacity: finite(2 * MIB),
            ..QueueConfig::default()
        })
    );
    assert_eq!(
        fixture
            .store()
            .get(&keys::queue_config(&ns(), &queue()))?
            .map(|raw| raw[0]),
        Some(codec::queue::VALUE_FORMAT_V3)
    );
    assert_eq!(
        machine.queue_config(&ns(), &queue().dead_letter_queue()?)?,
        Some(QueueConfig {
            max_delivery_count: u32::MAX,
            ..QueueConfig::default()
        }),
        "the shadow stays non-finite"
    );
    assert!(machine.queue_config(&ns(), &other)?.is_none());
    assert_eq!(machine.ready_sequences(&ns(), &queue(), 4)?.len(), 1);
    domain::snapshot_validation::validate_complete_state(&fixture.snapshot())?;
    let expected = Fixture::new(durable)?;
    expected.seed(&originals);
    assert_eq!(
        reserved(&fixture.snapshot(), F1),
        reserved(&expected.snapshot(), F1)
    );
    assert_eq!(
        fixture.store().get(&key(F1, 1))?,
        Some(checkpoint(6, &originals[5]))
    );
    let domain_rows = |rows: Vec<(Key, Value)>| {
        rows.into_iter()
            .filter(|(key, _)| key[0] < 0xF0)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        domain_rows(fixture.snapshot()),
        domain_rows(expected.snapshot())
    );
    let complete = fixture.snapshot();
    drop(machine);
    drop(owner);
    fixture.reopen()?;
    assert_eq!(fixture.snapshot(), complete);
    let mut owner = fixture.open()?;
    caught_up(&mut owner, &fixture, 6, 6);
    Ok(complete)
}

#[test]
fn v7_capacity_replay_matches_across_backends() -> TestResult {
    let memory = replay(false)?;
    let fjall = replay(true)?;
    assert_eq!(memory, fjall);
    Ok(())
}

#[test]
fn v11_capacity_intent_refuses_during_open_before_any_committed_prefix_is_applied() -> TestResult {
    for durable in [false, true] {
        let mut fixture = Fixture::new(durable)?;
        let owner = domain::retained_accounting::CapacityOwner::new(ns(), queue(), 1)?;
        let v11 = domain::capacity_fence::CapacityIntent::capture(owner, noop(20))?.encode()?;
        fixture.log(&[create(10).encode()?, v11], 2);
        let before = fixture.snapshot();
        for _ in 0..2 {
            assert!(matches!(
                fixture.open(),
                Err(ReplayError::Proposal {
                    index: 2,
                    source: DurableProposalError::UnsupportedVersion(11),
                })
            ));
            assert_eq!(fixture.snapshot(), before);
            assert!(fixture.trace().batches.is_empty());
            fixture.reopen()?;
        }
    }
    Ok(())
}
