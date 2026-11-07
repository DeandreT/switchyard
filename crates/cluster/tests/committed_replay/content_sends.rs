use super::*;

/// A properties section naming `id`, optionally a scheduled-enqueue annotation,
/// and a one-byte data section: complete AMQP content.
fn amqp(id: &str, scheduled: Option<u64>) -> MessageContent {
    let mut bytes = Vec::new();
    if let Some(millis) = scheduled {
        let key = b"x-opt-scheduled-enqueue-time";
        let mut map = vec![0xa3, key.len() as u8];
        map.extend(key);
        map.push(0x83);
        map.extend(millis.to_be_bytes());
        bytes.extend([0x00, 0x53, 0x72, 0xc1, map.len() as u8 + 1, 2]);
        bytes.extend(map);
    }
    bytes.extend([
        0x00,
        0x53,
        0x73,
        0xc0,
        id.len() as u8 + 3,
        1,
        0xa1,
        id.len() as u8,
    ]);
    bytes.extend(id.as_bytes());
    bytes.extend([0x00, 0x53, 0x75, 0xa0, 1, 0x2a]);
    MessageContent::Amqp(bytes)
}

fn replay(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let content = |entity: &EntityPath, at, content| {
        proposal(entity, at, CommandKind::SendContent { content })
    };
    let originals = [
        create(0),
        proposal(
            &topic(),
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        proposal(
            &topic(),
            0,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig::default(),
            },
        ),
        content(&queue(), 1, amqp("first", None)),
        // Refused at apply, yet checkpointed and replayed identically.
        content(&queue(), 2, MessageContent::Amqp(vec![0x00, 0x53, 0x75])),
        proposal(
            &queue(),
            3,
            CommandKind::SendContentBatch {
                messages: vec![
                    amqp("second", None),
                    MessageContent::Raw(RawMessage {
                        message_id: "raw".into(),
                        body: b"raw".to_vec(),
                        ..RawMessage::default()
                    }),
                ],
            },
        ),
        proposal(
            &topic(),
            4,
            CommandKind::SendContentBatch {
                messages: vec![amqp("now", None), amqp("later", Some(20))],
            },
        ),
        receive(5),
        proposal(&topic(), 20, CommandKind::ActivateScheduled),
    ];
    for (p, version) in originals.iter().zip([1, 1, 1, 4, 4, 4, 4, 1, 1]) {
        let bytes = p.encode()?;
        assert_eq!(bytes[7], version);
        assert_eq!(DurableProposal::decode(&bytes)?, *p);
    }
    fixture.proposals(&originals, originals.len() as u64);
    let frozen = fixture.snapshot();
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(5)?, progress(5, 9, 5));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(5)?, progress(9, 9, 4));
    frozen_f0(&fixture, &frozen);

    let rows = fixture.snapshot();
    let messages = rows
        .iter()
        .filter(|(key, _)| {
            key[0] == keys::message(&ns(), &queue(), domain::SequenceNumber::new(1))[0]
        })
        .collect::<Vec<_>>();
    // Three queue messages, the activated placeholder's subscription copy and
    // the immediate subscription copy.
    assert_eq!(messages.len(), 5);
    for (_, value) in &messages {
        assert_eq!(value[0], codec::message::VALUE_FORMAT_V2);
    }
    domain::snapshot_validation::validate_complete_state(&rows)?;

    let expected = Fixture::new(durable)?;
    expected.seed(&originals);
    assert_eq!(reserved(&rows, F1), reserved(&expected.snapshot(), F1));
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
    drop(owner);
    fixture.reopen()?;
    assert_eq!(fixture.snapshot(), complete);
    let mut owner = fixture.open()?;
    caught_up(&mut owner, &fixture, 9, 9);
    Ok(complete)
}

#[test]
fn v4_content_sends_replay_as_v2_records_identically_across_backends() -> TestResult {
    assert_eq!(replay(false)?, replay(true)?);
    Ok(())
}
