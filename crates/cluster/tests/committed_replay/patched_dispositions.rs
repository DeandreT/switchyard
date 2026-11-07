use domain::{LockToken, MessageEnvelope, MessageRecord, OverlayProperty, SequenceNumber};

use super::*;

/// A properties section naming `id` and a one-byte data section.
fn amqp(id: &str) -> Vec<u8> {
    let mut bytes = vec![
        0x00,
        0x53,
        0x73,
        0xc0,
        id.len() as u8 + 3,
        1,
        0xa1,
        id.len() as u8,
    ];
    bytes.extend(id.as_bytes());
    bytes.extend([0x00, 0x53, 0x75, 0xa0, 1, 0x2a]);
    bytes
}

fn property(name: &str, value: &[u8]) -> OverlayProperty {
    OverlayProperty {
        name: name.into(),
        value: value.to_vec(),
    }
}

fn abandon(at: u64, sequence: u64, token: u64, patch: Vec<OverlayProperty>) -> DurableProposal {
    proposal(
        &queue(),
        at,
        CommandKind::AbandonWithPatch {
            sequence: SequenceNumber::new(sequence),
            lock_token: LockToken::new(token),
            patch,
        },
    )
}

fn dead_letter(at: u64, sequence: u64, token: u64, patch: Vec<OverlayProperty>) -> DurableProposal {
    proposal(
        &queue(),
        at,
        CommandKind::DeadLetterWithPatch {
            sequence: SequenceNumber::new(sequence),
            lock_token: LockToken::new(token),
            reason: "r".into(),
            description: "d".into(),
            patch,
        },
    )
}

fn replay(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let content = |id| {
        proposal(
            &queue(),
            1,
            CommandKind::SendContent {
                content: MessageContent::Amqp(amqp(id)),
            },
        )
    };
    let originals = [
        create(0),
        content("first"),
        content("second"),
        proposal(
            &queue(),
            1,
            CommandKind::Send {
                message_id: "legacy".into(),
                body: vec![0x2a],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: Some(MessageEnvelope::new(amqp("legacy"))),
            },
        ),
        receive(2),
        abandon(3, 1, 1, vec![property("p", &[0x40])]),
        receive(4),
        proposal(
            &queue(),
            5,
            CommandKind::DeferWithPatch {
                sequence: SequenceNumber::new(1),
                lock_token: LockToken::new(2),
                patch: vec![property("p", &[0x41]), property("q", &[0x40])],
            },
        ),
        receive(6),
        // Refused at apply, yet checkpointed and replayed identically.
        abandon(7, 2, 3, vec![property("p", &[0xff])]),
        dead_letter(8, 2, 3, vec![property("p", &[0x42])]),
        receive(9),
        // The legacy V1 record converts to V2 in the dead-letter queue.
        dead_letter(10, 3, 4, vec![property("p", &[0x40])]),
    ];
    for (p, version) in originals
        .iter()
        .zip([1, 4, 4, 1, 1, 4, 1, 4, 1, 4, 4, 1, 4])
    {
        let bytes = p.encode()?;
        assert_eq!(bytes[7], version);
        assert_eq!(DurableProposal::decode(&bytes)?, *p);
    }
    let total = originals.len() as u64;
    fixture.proposals(&originals, total);
    let frozen = fixture.snapshot();
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(5)?, progress(5, total, 5));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(64)?, progress(total, total, 8));
    frozen_f0(&fixture, &frozen);

    let rows = fixture.snapshot();
    let tag = keys::message(&ns(), &queue(), SequenceNumber::new(1))[0];
    let records = rows
        .iter()
        .filter(|(key, _)| key[0] == tag)
        .map(|(_, value)| {
            assert_eq!(value[0], codec::message::VALUE_FORMAT_V2);
            MessageRecord::decode(value)
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The deferred queue copy, then the two dead-lettered copies.
    let overlays = records
        .iter()
        .map(|record| {
            let envelope = record.envelope.as_ref().expect("AMQP content");
            assert!(record.body.is_empty());
            (envelope.as_bytes().to_vec(), envelope.overlay().to_vec())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        overlays,
        [
            (
                amqp("first"),
                vec![property("p", &[0x41]), property("q", &[0x40])]
            ),
            (amqp("second"), vec![property("p", &[0x42])]),
            (amqp("legacy"), vec![property("p", &[0x40])]),
        ]
    );
    domain::snapshot_validation::validate_complete_state(&rows)?;
    domain::legacy_content::validate_legacy_content(&rows)?;

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
    caught_up(&mut owner, &fixture, total, total);
    Ok(complete)
}

#[test]
fn v4_patched_dispositions_replay_identically_across_backends() -> TestResult {
    assert_eq!(replay(false)?, replay(true)?);
    Ok(())
}
