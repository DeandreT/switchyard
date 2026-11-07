use super::*;

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: Some(100),
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

fn policy(enabled: bool) -> domain::QueueConfigUpdate {
    domain::QueueConfigUpdate {
        dead_lettering_on_message_expiration: Some(enabled),
        ..domain::QueueConfigUpdate::default()
    }
}

fn replay(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let worker = SubscriptionName::new("worker")?;
    let originals = [
        proposal(
            &queue(),
            0,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    dead_lettering_on_message_expiration: false,
                    ..QueueConfig::default()
                },
            },
        ),
        proposal(&queue(), 1, send("dropped")),
        proposal(
            &topic(),
            2,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        proposal(
            &topic(),
            3,
            CommandKind::CreateSubscription {
                name: worker.clone(),
                config: SubscriptionConfig {
                    dead_lettering_on_message_expiration: false,
                    ..SubscriptionConfig::default()
                },
            },
        ),
        proposal(&topic(), 4, send("fanout")),
        proposal(&queue(), 200, CommandKind::ExpireMessages),
        proposal(&subscription(), 200, CommandKind::ExpireMessages),
        proposal(
            &queue(),
            300,
            CommandKind::UpdateQueue {
                update: policy(true),
            },
        ),
        proposal(&queue(), 301, send("kept")),
        proposal(&queue(), 500, CommandKind::ExpireMessages),
        proposal(
            &topic(),
            501,
            CommandKind::UpdateSubscription {
                name: worker,
                update: SubscriptionConfigUpdate {
                    dead_lettering_on_message_expiration: Some(true),
                    ..SubscriptionConfigUpdate::default()
                },
            },
        ),
    ];
    for (p, version) in originals.iter().zip([5, 1, 1, 5, 1, 1, 1, 5, 1, 1, 5]) {
        let bytes = p.encode()?;
        assert_eq!(bytes[7], version);
        assert_eq!(DurableProposal::decode(&bytes)?, *p);
    }
    fixture.proposals(&originals, originals.len() as u64);
    let frozen = fixture.snapshot();
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(5)?, progress(5, 11, 5));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(6)?, progress(11, 11, 6));
    frozen_f0(&fixture, &frozen);
    let machine = StateMachine::new(fixture.store().clone());
    for entity in [queue(), subscription()] {
        assert_eq!(
            machine
                .queue_config(&ns(), &entity)?
                .map(|config| config.dead_lettering_on_message_expiration),
            Some(true)
        );
    }
    let first = domain::SequenceNumber::new(1);
    assert!(machine.message(&ns(), &queue(), first)?.is_none());
    assert!(
        machine
            .dead_lettered_message(&ns(), &queue(), first)?
            .is_none(),
        "the disabled policy deleted the first expiry"
    );
    assert!(
        machine
            .dead_lettered_message(&ns(), &queue(), domain::SequenceNumber::new(2))?
            .is_some(),
        "the re-enabled policy dead-lettered the second"
    );
    assert!(
        machine
            .ready_sequences(&ns(), &subscription(), 4)?
            .is_empty()
    );
    assert!(
        machine
            .ready_sequences(&ns(), &subscription().dead_letter_queue()?, 4)?
            .is_empty()
    );
    domain::snapshot_validation::validate_complete_state(&fixture.snapshot())?;
    let expected = Fixture::new(durable)?;
    expected.seed(&originals);
    assert_eq!(
        reserved(&fixture.snapshot(), F1),
        reserved(&expected.snapshot(), F1)
    );
    assert_eq!(
        fixture.store().get(&key(F1, 1))?,
        Some(checkpoint(11, &originals[10]))
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
    caught_up(&mut owner, &fixture, 11, 11);
    Ok(complete)
}

#[test]
fn v5_expiry_policy_replay_matches_across_backends() -> TestResult {
    let memory = replay(false)?;
    let fjall = replay(true)?;
    assert_eq!(memory, fjall);
    Ok(())
}
