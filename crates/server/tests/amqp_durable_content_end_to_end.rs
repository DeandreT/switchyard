//! Typed message content on the durable backend, over a real AMQP socket.
//!
//! Every step runs against a broker restarted on the same Fjall directory, so
//! each read renders from the original bytes and overlay that reached disk.

use amqp::{
    ApplicationProperties, Array, Binary, Body, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, DeliveryAnnotations, Error as AmqpError, ErrorCondition, Fields,
    Footer, Header, Message, MessageAnnotations, MessageId, Modified, OrderedMap, Outcome,
    Properties, SenderSettleMode, Symbol, Uuid, Value, encode_message,
};
use domain::{CommandKind, CommandOutcome, DeadLetterReason, QueueConfig};
use serde_amqp::primitives::Timestamp;

#[allow(dead_code)]
mod durable_node;

use durable_node::{
    DurableNode, Expect, Management, TestResult, assert_nothing_more, assert_renders,
    assert_stored, entity, holds, lock_token_of, namespace, next, phase, sequence_of,
};

fn queue(name: &str, config: QueueConfig) -> (&str, CommandKind) {
    (name, CommandKind::CreateQueue { config })
}

fn string(text: &str) -> Value {
    Value::String(text.to_owned())
}

fn symbol(text: &str) -> Value {
    Value::Symbol(Symbol::from(text))
}

fn value_map(entries: Vec<(Value, Value)>) -> Value {
    let mut map = OrderedMap::new();
    for (key, value) in entries {
        map.insert(key, value);
    }
    Value::Map(map)
}

fn data_body() -> Body {
    Body::Data(vec![
        Binary::from(b"first section".to_vec()),
        Binary::from(vec![0_u8, 255, 1, 254]),
        Binary::from(b"third".to_vec()),
    ])
}

fn sequence_body() -> Body {
    Body::Sequence(vec![
        vec![Value::Int(1), string("two"), Value::Null],
        vec![
            value_map(vec![(symbol("k"), Value::Long(-3))]),
            Value::Array(Array::from(vec![Value::Uint(1), Value::Uint(2)])),
        ],
    ])
}

fn value_body() -> Body {
    Body::Value(value_map(vec![
        (string("order"), Value::Ulong(42)),
        (
            string("lines"),
            Value::List(vec![
                value_map(vec![
                    (string("sku"), string("A-1")),
                    (string("qty"), Value::Int(2)),
                ]),
                value_map(vec![
                    (string("sku"), symbol("B-2")),
                    (string("qty"), Value::Int(5)),
                ]),
            ]),
        ),
    ]))
}

/// A message filling every section a client may send, with compound values.
fn rich(label: &str, body: Body) -> Message {
    let mut delivery_annotations = DeliveryAnnotations::default();
    delivery_annotations.insert("x-trace", string("hop-1"));
    delivery_annotations.insert(77_u64, Value::Long(-1));
    // Broker-owned fields a sender cannot set: the broker's own win.
    delivery_annotations.insert(
        "x-opt-lock-token",
        Value::Uuid(Uuid::from(durable_node::FORGED_LOCK)),
    );
    let mut message_annotations = MessageAnnotations::default();
    message_annotations.insert("x-partner-route", symbol("eu-west"));
    message_annotations.insert("x-opt-sequence-number", Value::Long(-7));
    message_annotations.insert(
        "x-opt-locked-until",
        Value::Timestamp(Timestamp::from_milliseconds(-7)),
    );
    message_annotations.insert("x-opt-deadletter-source", string("forged"));
    message_annotations.insert("x-opt-message-state", Value::Int(9));
    message_annotations.insert(0x1234_u64, Value::List(vec![Value::Int(1), string("two")]));
    message_annotations.insert(
        "x-tags",
        Value::Array(Array::from(vec![symbol("a"), symbol("b")])),
    );
    let mut application = ApplicationProperties::default();
    application.insert("label", label);
    application.insert("count", Value::Int(7));
    application.insert("big", Value::Long(-9_000_000_000));
    application.insert("flag", Value::Bool(true));
    application.insert("ratio", Value::from(0.25_f64));
    application.insert("symbolic", symbol("orders"));
    application.insert(
        "stamp",
        Value::Timestamp(Timestamp::from_milliseconds(1_700_000_000_000)),
    );
    application.insert("id", Value::Uuid(Uuid::from([7_u8; 16])));
    application.insert("raw", Value::Binary(Binary::from(vec![1_u8, 2, 3])));
    application.insert(
        "list",
        Value::List(vec![
            Value::Int(1),
            string("two"),
            Value::Null,
            Value::List(vec![Value::Bool(false)]),
        ]),
    );
    application.insert(
        "map",
        value_map(vec![
            (string("a"), Value::Long(1)),
            (symbol("b"), Value::List(vec![])),
        ]),
    );
    application.insert(
        "ints",
        Value::Array(Array::from(vec![
            Value::Int(-1),
            Value::Int(0),
            Value::Int(1000),
        ])),
    );
    application.insert(
        "names",
        Value::Array(Array::from(vec![string("ab"), string("c")])),
    );
    let mut footer = Footer::default();
    footer.insert(
        "x-checksum",
        Value::Binary(Binary::from(vec![0xde_u8, 0xad])),
    );
    footer.insert(9_u64, Value::Uint(3));
    Message {
        header: Some(Header {
            durable: true,
            priority: 7,
            ttl: None,
            first_acquirer: true,
            delivery_count: 0,
        }),
        delivery_annotations: Some(delivery_annotations),
        message_annotations: Some(message_annotations),
        properties: Some(Properties {
            message_id: Some(MessageId::String(label.to_owned())),
            user_id: Some(Binary::from(b"tester".to_vec())),
            to: Some(String::from("orders")),
            subject: Some(String::from("typed-content")),
            reply_to: Some(String::from("replies")),
            correlation_id: Some(MessageId::Ulong(42)),
            content_type: Some(Symbol::from("application/x-test")),
            content_encoding: Some(Symbol::from("identity")),
            absolute_expiry_time: None,
            creation_time: Some(1_700_000_000_000),
            group_id: None,
            group_sequence: Some(3),
            reply_to_group_id: Some(String::from("reply-group")),
        }),
        application_properties: Some(application),
        body,
        footer: Some(footer),
    }
}

fn bodies(prefix: &str) -> Vec<Message> {
    vec![
        rich(&format!("{prefix}-data"), data_body()),
        rich(&format!("{prefix}-sequence"), sequence_body()),
        rich(&format!("{prefix}-value"), value_body()),
    ]
}

fn scheduled(mut message: Message, at: i64) -> Message {
    message
        .message_annotations
        .get_or_insert_with(MessageAnnotations::default)
        .insert(
            "x-opt-scheduled-enqueue-time",
            Value::Timestamp(Timestamp::from_milliseconds(at)),
        );
    message
}

fn batch(children: &[Message]) -> TestResult<Message> {
    let sections = children
        .iter()
        .map(|child| encode_message(child).map(Binary::from))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Message::builder().body(Body::Data(sections)).build())
}

fn fields(properties: &[(&str, Value)]) -> Fields {
    let mut fields = Fields::new();
    for (name, value) in properties {
        fields.insert(Symbol::from(*name), value.clone());
    }
    fields
}

async fn abandon_with(
    receiver: &Receiver,
    delivery: &amqp::ClientDelivery,
    properties: &[(&str, Value)],
    undeliverable_here: bool,
) -> TestResult {
    receiver
        .modify(
            delivery,
            Modified {
                undeliverable_here: undeliverable_here.then_some(true),
                message_annotations: Some(fields(properties)),
                ..Modified::default()
            },
        )
        .await?;
    Ok(())
}

fn ready(machine: &domain::StateMachine<storage::FjallStore>, path: &str) -> TestResult<Vec<u64>> {
    Ok(machine
        .ready_sequences(&namespace(), &entity(path), 100)?
        .into_iter()
        .map(|sequence| sequence.as_u64())
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sends_batches_peeks_and_both_receive_modes_render_the_original_after_reopen() -> TestResult
{
    let mut node = DurableNode::create(vec![
        queue("orders", QueueConfig::default()),
        queue("batches", QueueConfig::default()),
    ])
    .await?;
    let singles = bodies("single");
    let children = bodies("child");
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "single-sender", "orders").await?;
        for message in &singles {
            assert!(matches!(
                sender.send(message.clone()).await?,
                Outcome::Accepted(_)
            ));
        }
        let mut batcher = Sender::attach(&mut session, "batch-sender", "batches").await?;
        assert!(matches!(
            batcher
                .send_with_format(
                    batch(&children)?,
                    protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT
                )
                .await?,
            Outcome::Accepted(_)
        ));
        connection.close().await?;
    }

    node.reopen(|machine| {
        for (path, messages) in [("orders", &singles), ("batches", &children)] {
            assert_eq!(ready(machine, path)?, vec![1, 2, 3]);
            for (index, message) in messages.iter().enumerate() {
                assert_stored(
                    machine,
                    path,
                    index as i64 + 1,
                    &encode_message(message)?,
                    &[],
                )?;
            }
        }
        Ok(())
    })
    .await?;

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        for (path, messages) in [("orders", &singles), ("batches", &children)] {
            let mut management = Management::attach(&mut session, path).await?;
            let peeked = management.peek(1, 10).await?;
            assert_eq!(peeked.len(), messages.len());
            for (entry, original) in peeked.iter().zip(messages) {
                assert_renders(&entry.message, &entry.encoded, original, Expect::default());
            }
        }
        connection.close().await?;
    }
    node.restart().await?;

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut locked = Receiver::attach(&mut session, "peek-lock", "orders").await?;
        let mut deliveries = Vec::new();
        for original in &singles {
            let delivery = next(&mut locked).await?;
            assert_renders(
                delivery.message(),
                delivery.encoded_message(),
                original,
                Expect {
                    locked: true,
                    ..Expect::default()
                },
            );
            deliveries.push(delivery);
        }
        for delivery in &deliveries {
            locked.accept(delivery).await?;
        }
        Management::attach(&mut session, "orders")
            .await?
            .wait_until("the completions apply", <[_]>::is_empty)
            .await?;
        let mut deleting = Receiver::builder()
            .name("receive-and-delete")
            .source("batches")
            .sender_settle_mode(SenderSettleMode::Settled)
            .attach(&mut session)
            .await?;
        for original in &children {
            let delivery = next(&mut deleting).await?;
            assert_renders(
                delivery.message(),
                delivery.encoded_message(),
                original,
                Expect::default(),
            );
        }
        assert_nothing_more(&mut locked).await;
        assert_nothing_more(&mut deleting).await;
        connection.close().await?;
    }

    node.reopen(|machine| {
        for path in ["orders", "batches"] {
            assert!(ready(machine, path)?.is_empty());
            for sequence in 1..=3 {
                assert!(
                    machine
                        .message(
                            &namespace(),
                            &entity(path),
                            durable_node::sequence(sequence)
                        )?
                        .is_none(),
                    "{path}/{sequence} survived settlement"
                );
            }
        }
        Ok(())
    })
    .await?;
    node.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_sends_activate_with_their_original_content_after_reopen() -> TestResult {
    let mut node = DurableNode::create(vec![queue("orders", QueueConfig::default())]).await?;
    let transfer = scheduled(rich("scheduled-transfer", data_body()), 5_000);
    let managed = scheduled(rich("scheduled-management", sequence_body()), 5_000);
    let managed_bytes = encode_message(&managed)?;
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "scheduler", "orders").await?;
        assert!(matches!(
            sender.send(transfer.clone()).await?,
            Outcome::Accepted(_)
        ));
        let mut management = Management::attach(&mut session, "orders").await?;
        let (status, response) = management
            .schedule(&[("scheduled-management", managed_bytes.clone())])
            .await?;
        assert_eq!(status, 200, "{response:?}");
        connection.close().await?;
    }

    let originals = [transfer, managed];
    node.reopen(|machine| {
        let scheduled: Vec<u64> = machine
            .scheduled_sequences(&namespace(), &entity("orders"), 10)?
            .into_iter()
            .map(|sequence| sequence.as_u64())
            .collect();
        assert_eq!(scheduled, vec![1, 2]);
        assert!(ready(machine, "orders")?.is_empty());
        assert_stored(machine, "orders", 1, &encode_message(&originals[0])?, &[])?;
        assert_stored(machine, "orders", 2, &managed_bytes, &[])?;
        Ok(())
    })
    .await?;

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        let peeked = management.peek(1, 10).await?;
        assert_eq!(peeked.len(), 2);
        for (entry, original) in peeked.iter().zip(&originals) {
            assert_renders(
                &entry.message,
                &entry.encoded,
                original,
                Expect {
                    state: Some(2),
                    ..Expect::default()
                },
            );
        }
        connection.close().await?;
    }

    node.advance_to(5_000);
    assert!(matches!(
        node.submit("orders", CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2, .. }
    ));
    let activated = node
        .reopen(|machine| {
            let activated = ready(machine, "orders")?;
            assert_eq!(activated.len(), 2);
            for (sequence, original) in activated.iter().zip(&originals) {
                assert_stored(
                    machine,
                    "orders",
                    *sequence as i64,
                    &encode_message(original)?,
                    &[],
                )?;
            }
            Ok(activated)
        })
        .await?;

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut receiver = Receiver::attach(&mut session, "activated", "orders").await?;
        for (sequence, original) in activated.iter().zip(&originals) {
            let delivery = next(&mut receiver).await?;
            assert_eq!(sequence_of(delivery.message()), *sequence as i64);
            assert_renders(
                delivery.message(),
                delivery.encoded_message(),
                original,
                Expect {
                    locked: true,
                    ..Expect::default()
                },
            );
            assert_eq!(
                durable_node::annotation(delivery.message(), "x-opt-scheduled-enqueue-time"),
                Some(&Value::Timestamp(Timestamp::from_milliseconds(5_000))),
                "the broker renders the requested enqueue time"
            );
            receiver.accept(&delivery).await?;
        }
        Management::attach(&mut session, "orders")
            .await?
            .wait_until("the completions apply", <[_]>::is_empty)
            .await?;
        connection.close().await?;
    }
    node.reopen(|machine| {
        assert!(ready(machine, "orders")?.is_empty());
        Ok(())
    })
    .await?;
    node.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deferred_copy_keeps_its_content_and_patches_through_receive_by_sequence() -> TestResult {
    let mut node = DurableNode::create(vec![queue("orders", QueueConfig::default())]).await?;
    let original = rich("deferred", value_body());
    let parked = [("stage", string("parked")), ("attempt", Value::Int(1))];
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "sender", "orders").await?;
        sender.send(original.clone()).await?;
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let first = next(&mut receiver).await?;
        abandon_with(&receiver, &first, &parked, true).await?;
        Management::attach(&mut session, "orders")
            .await?
            .wait_until("the deferral applies", |entries| holds(entries, 1, Some(1)))
            .await?;
        connection.close().await?;
    }

    node.reopen(|machine| {
        let deferred: Vec<u64> = machine
            .deferred_sequences(&namespace(), &entity("orders"), 10)?
            .into_iter()
            .map(|sequence| sequence.as_u64())
            .collect();
        assert_eq!(deferred, vec![1]);
        assert_stored(machine, "orders", 1, &encode_message(&original)?, &parked)?;
        Ok(())
    })
    .await?;

    let retried = [
        ("stage", string("parked")),
        ("attempt", Value::Int(2)),
        ("note", Value::Null),
    ];
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut receiver = Receiver::attach(&mut session, "ordinary", "orders").await?;
        assert_nothing_more(&mut receiver).await;
        let mut management = Management::attach(&mut session, "orders").await?;
        let (status, entries) = management.receive_deferred(1).await?;
        assert_eq!(status, 200);
        let [entry] = entries.try_into().map_err(|_| "one deferred message")?;
        assert_renders(
            &entry.message,
            &entry.encoded,
            &original,
            Expect {
                delivery_count: 1,
                locked: true,
                overlay: &parked,
                state: Some(1),
                ..Expect::default()
            },
        );
        let lock = entry.lock.ok_or("a locked deferred entry")?;
        let (status, response) = management
            .update_disposition(
                "unused",
                lock,
                "abandoned",
                &[("attempt", Value::Int(2)), ("note", Value::Null)],
            )
            .await?;
        assert_eq!(status, 200, "{response:?}");
        connection.close().await?;
    }

    node.reopen(|machine| {
        assert_stored(machine, "orders", 1, &encode_message(&original)?, &retried)?;
        Ok(())
    })
    .await?;

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        let (status, entries) = management.receive_deferred(1).await?;
        assert_eq!(status, 200);
        let [entry] = entries.try_into().map_err(|_| "one deferred message")?;
        assert_renders(
            &entry.message,
            &entry.encoded,
            &original,
            Expect {
                delivery_count: 2,
                locked: true,
                overlay: &retried,
                state: Some(1),
                ..Expect::default()
            },
        );
        let lock = entry.lock.ok_or("a locked deferred entry")?;
        let (status, response) = management
            .update_disposition("unused", lock, "completed", &[])
            .await?;
        assert_eq!(status, 200, "{response:?}");
        connection.close().await?;
    }
    node.reopen(|machine| {
        assert!(
            machine
                .deferred_sequences(&namespace(), &entity("orders"), 10)?
                .is_empty()
        );
        assert!(
            machine
                .message(&namespace(), &entity("orders"), durable_node::sequence(1))?
                .is_none()
        );
        Ok(())
    })
    .await?;
    node.shutdown().await
}

/// Drains the one dead-lettered copy and checks it against the original.
async fn receive_dead_letter(
    node: &DurableNode,
    original: &Message,
    expect: Expect<'_>,
) -> TestResult {
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut dead_letters =
        Receiver::attach(&mut session, "dead-letters", "orders/$deadletterqueue").await?;
    let delivery = next(&mut dead_letters).await?;
    assert_renders(
        delivery.message(),
        delivery.encoded_message(),
        original,
        expect,
    );
    dead_letters.accept(&delivery).await?;
    Management::attach(&mut session, "orders/$deadletterqueue")
        .await?
        .wait_until("the completion applies", <[_]>::is_empty)
        .await?;
    assert_nothing_more(&mut dead_letters).await;
    connection.close().await?;
    Ok(())
}

fn dead_letter_reason(
    machine: &domain::StateMachine<storage::FjallStore>,
    sequence: i64,
    original: &Message,
    overlay: &[(&str, Value)],
) -> TestResult<DeadLetterReason> {
    let record = assert_stored(
        machine,
        "orders/$deadletterqueue",
        sequence,
        &encode_message(original)?,
        overlay,
    )?;
    assert!(
        machine
            .message(
                &namespace(),
                &entity("orders"),
                durable_node::sequence(sequence)
            )?
            .is_none(),
        "the dead-lettered copy left its queue"
    );
    Ok(record
        .dead_letter
        .ok_or("the copy is dead-lettered")?
        .reason)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_and_automatic_dead_letters_keep_their_content_after_reopen() -> TestResult {
    let mut node = DurableNode::create(vec![queue(
        "orders",
        QueueConfig {
            max_delivery_count: 2,
            ..QueueConfig::default()
        },
    )])
    .await?;

    // Explicit: a dead-letter rejection carrying a patch.
    let explicit = rich("explicit", data_body());
    let rejected = [
        ("stage", string("dead")),
        (
            protocol_amqp::DEAD_LETTER_REASON_PROPERTY,
            string("InvalidOrder"),
        ),
        (
            protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
            string("the order has no customer"),
        ),
    ];
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "sender", "orders").await?;
        sender.send(explicit.clone()).await?;
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let delivery = next(&mut receiver).await?;
        receiver
            .reject(
                &delivery,
                Some(AmqpError {
                    condition: ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                    description: None,
                    info: Some(fields(&rejected)),
                }),
            )
            .await?;
        Management::attach(&mut session, "orders/$deadletterqueue")
            .await?
            .wait_until("the dead-letter applies", |entries| holds(entries, 1, None))
            .await?;
        connection.close().await?;
    }
    let reason = node
        .reopen(|machine| dead_letter_reason(machine, 1, &explicit, &rejected))
        .await?;
    assert_eq!(
        reason,
        DeadLetterReason::Application(String::from("InvalidOrder"))
    );
    receive_dead_letter(
        &node,
        &explicit,
        Expect {
            delivery_count: 1,
            locked: true,
            overlay: &rejected,
            dead_letter: Some(("InvalidOrder", "the order has no customer")),
            ..Expect::default()
        },
    )
    .await?;

    // Automatic: the maximum delivery count. An abandon redelivers to the
    // attached link at once, so both attempts share one connection, between
    // a restart after the send and one after the move.
    let exhausted = rich("exhausted", sequence_body());
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "sender", "orders").await?;
        sender.send(exhausted.clone()).await?;
        connection.close().await?;
    }
    node.restart().await?;
    let attempts = [("attempt", Value::Int(2))];
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let first = next(&mut receiver).await?;
        abandon_with(&receiver, &first, &[("attempt", Value::Int(1))], false).await?;
        let second = next(&mut receiver).await?;
        assert_renders(
            second.message(),
            second.encoded_message(),
            &exhausted,
            Expect {
                delivery_count: 1,
                locked: true,
                overlay: &[("attempt", Value::Int(1))],
                ..Expect::default()
            },
        );
        abandon_with(&receiver, &second, &attempts, false).await?;
        Management::attach(&mut session, "orders/$deadletterqueue")
            .await?
            .wait_until("the dead-letter applies", |entries| holds(entries, 2, None))
            .await?;
        assert_nothing_more(&mut receiver).await;
        connection.close().await?;
    }
    let reason = node
        .reopen(|machine| dead_letter_reason(machine, 2, &exhausted, &attempts))
        .await?;
    assert_eq!(reason, DeadLetterReason::MaxDeliveryCountExceeded);
    receive_dead_letter(
        &node,
        &exhausted,
        Expect {
            delivery_count: 2,
            locked: true,
            overlay: &attempts,
            dead_letter: Some((
                "MaxDeliveryCountExceeded",
                "the message reached its maximum delivery count",
            )),
            ..Expect::default()
        },
    )
    .await?;

    // Automatic: time to live, expired by the broker after a restart.
    let mut expiring = rich("expiring", value_body());
    if let Some(header) = expiring.header.as_mut() {
        header.ttl = Some(1_000);
    }
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "sender", "orders").await?;
        sender.send(expiring.clone()).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        let [entry] = management
            .peek(3, 1)
            .await?
            .try_into()
            .map_err(|_| "one peek")?;
        assert_renders(
            &entry.message,
            &entry.encoded,
            &expiring,
            Expect {
                ttl: Some(1_000),
                ..Expect::default()
            },
        );
        connection.close().await?;
    }
    node.restart().await?;
    node.advance_to(3_000);
    assert!(matches!(
        node.submit("orders", CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { .. }
    ));
    let reason = node
        .reopen(|machine| dead_letter_reason(machine, 3, &expiring, &[]))
        .await?;
    assert_eq!(reason, DeadLetterReason::TimeToLiveExpired);
    receive_dead_letter(
        &node,
        &expiring,
        Expect {
            locked: true,
            dead_letter: Some((
                "TTLExpiredException",
                "the message exceeded its time to live",
            )),
            ..Expect::default()
        },
    )
    .await?;
    node.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn property_patches_render_over_the_untouched_original_after_reopen() -> TestResult {
    let mut node = DurableNode::create(vec![queue("orders", QueueConfig::default())]).await?;
    let original = rich("patched", data_body());
    let first_patch = [
        ("label", string("patched")),
        (
            "list",
            value_map(vec![(string("replaced"), Value::List(vec![Value::Int(1)]))]),
        ),
        (
            "added-array",
            Value::Array(Array::from(vec![Value::Long(-1), Value::Long(i64::MAX)])),
        ),
        ("added-null", Value::Null),
        (protocol_amqp::DEAD_LETTER_REASON_PROPERTY, string("forged")),
    ];
    // An abandon redelivers to the attached link at once, so each step checks
    // that redelivery and leaves it locked; after the restart the lock
    // expires and the copy is ready again. Every lock counts one delivery.
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "sender", "orders").await?;
        sender.send(original.clone()).await?;
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let first = next(&mut receiver).await?;
        abandon_with(&receiver, &first, &first_patch, false).await?;
        let redelivered = next(&mut receiver).await?;
        assert_renders(
            redelivered.message(),
            redelivered.encoded_message(),
            &original,
            Expect {
                delivery_count: 1,
                locked: true,
                overlay: &first_patch,
                ..Expect::default()
            },
        );
        connection.close().await?;
    }
    node.restart().await?;
    node.expire_locks("orders")?;
    let bytes = encode_message(&original)?;
    node.reopen(|machine| {
        assert_stored(machine, "orders", 1, &bytes, &first_patch)?;
        Ok(())
    })
    .await?;

    let second_patch = [
        ("added-null", symbol("now-a-symbol")),
        (
            "nested",
            Value::List(vec![value_map(vec![(
                symbol("deep"),
                Value::List(vec![Value::Array(Array::from(vec![Value::Bool(true)]))]),
            )])]),
        ),
    ];
    let merged = [
        first_patch[0].clone(),
        first_patch[1].clone(),
        first_patch[2].clone(),
        second_patch[0].clone(),
        second_patch[1].clone(),
    ];
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        let [peeked] = management
            .peek(1, 10)
            .await?
            .try_into()
            .map_err(|_| "one peek")?;
        assert_renders(
            &peeked.message,
            &peeked.encoded,
            &original,
            Expect {
                delivery_count: 2,
                overlay: &first_patch,
                ..Expect::default()
            },
        );
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let third = next(&mut receiver).await?;
        assert_renders(
            third.message(),
            third.encoded_message(),
            &original,
            Expect {
                delivery_count: 2,
                locked: true,
                overlay: &first_patch,
                ..Expect::default()
            },
        );
        let (status, response) = management
            .update_disposition(
                "receiver",
                lock_token_of(&third),
                "abandoned",
                &second_patch,
            )
            .await?;
        assert_eq!(status, 200, "{response:?}");
        let fourth = next(&mut receiver).await?;
        assert_renders(
            fourth.message(),
            fourth.encoded_message(),
            &original,
            Expect {
                delivery_count: 3,
                locked: true,
                overlay: &merged,
                ..Expect::default()
            },
        );
        connection.close().await?;
    }
    node.restart().await?;
    node.expire_locks("orders")?;
    node.reopen(|machine| {
        assert_stored(machine, "orders", 1, &bytes, &merged)?;
        Ok(())
    })
    .await?;

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let fifth = next(&mut receiver).await?;
        assert_renders(
            fifth.message(),
            fifth.encoded_message(),
            &original,
            Expect {
                delivery_count: 4,
                locked: true,
                overlay: &merged,
                ..Expect::default()
            },
        );
        receiver.accept(&fifth).await?;
        Management::attach(&mut session, "orders")
            .await?
            .wait_until("the completion applies", <[_]>::is_empty)
            .await?;
        connection.close().await?;
    }
    node.reopen(|machine| {
        assert!(
            machine
                .message(&namespace(), &entity("orders"), durable_node::sequence(1))?
                .is_none()
        );
        Ok(())
    })
    .await?;
    node.shutdown().await
}

fn rejected_with(outcome: Outcome, condition: &str) {
    let Outcome::Rejected(rejected) = &outcome else {
        panic!("expected a rejection naming {condition}, got {outcome:?}");
    };
    let named = rejected
        .error
        .as_ref()
        .map(|error| error.condition.as_symbol().as_str().to_owned());
    assert_eq!(named.as_deref(), Some(condition), "{outcome:?}");
}

/// Sends one message on a link of its own connection: a transfer that fails
/// the received-message checks ends its link, so it must not share one.
async fn send_alone(
    node: &DurableNode,
    message: Message,
    format: u32,
) -> TestResult<Result<Outcome, amqp::EngineError>> {
    phase("send_alone: connect");
    let mut connection = node.connect().await?;
    phase("send_alone: session begin");
    let mut session = Session::begin(&mut connection).await?;
    phase("send_alone: sender attach");
    let mut sender = Sender::attach(&mut session, "refused", "orders").await?;
    phase("send_alone: original send wait");
    let outcome = sender.send_with_format(message, format).await;
    phase("send_alone: original close wait");
    let _ = connection.close().await;
    phase("send_alone: complete");
    Ok(outcome)
}

/// Receives the deferred copy at sequence 1 and returns its lock.
async fn lock_deferred(management: &mut Management) -> TestResult<amqp::Uuid> {
    let (status, entries) = management.receive_deferred(1).await?;
    assert_eq!(status, 200);
    let [entry] = entries.try_into().map_err(|_| "one deferred message")?;
    Ok(entry.lock.ok_or("a locked deferred entry")?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_and_oversized_content_refuses_and_leaves_the_store_unchanged() -> TestResult {
    phase("malformed content: setup");
    // A small limit keeps an oversized message under the links' own limits.
    const LIMIT: usize = 2_048;
    let mut node = DurableNode::create(vec![queue(
        "orders",
        QueueConfig {
            max_message_bytes: LIMIT,
            ..QueueConfig::default()
        },
    )])
    .await?;
    let held = rich("held", data_body());
    let held_bytes = encode_message(&held)?;
    assert!(held_bytes.len() < LIMIT);
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "sender", "orders").await?;
        sender.send(held.clone()).await?;
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let delivery = next(&mut receiver).await?;
        abandon_with(&receiver, &delivery, &[], true).await?;
        Management::attach(&mut session, "orders")
            .await?
            .wait_until("the deferral applies", |entries| holds(entries, 1, Some(1)))
            .await?;
        connection.close().await?;
    }
    let before = node.snapshot().await?;
    phase("malformed content: initial snapshot complete");

    let oversized = rich(
        "oversized",
        Body::Data(vec![Binary::from(vec![b'x'; LIMIT])]),
    );
    // An array whose elements are compound values fails the received checks.
    let compound_array = Value::Array(Array::from(vec![Value::List(vec![Value::Int(1)])]));
    let mut malformed = rich("malformed", data_body());
    malformed
        .application_properties
        .get_or_insert_with(ApplicationProperties::default)
        .insert("compound-array", compound_array.clone());
    // Well-formed AMQP the content derivation refuses.
    let invalid = rich(
        &"i".repeat(domain::MAX_MESSAGE_ID_CHARACTERS + 1),
        data_body(),
    );
    let fitting = rich("fitting-child", data_body());

    // A malformed transfer fails the received checks and ends its link.
    assert!(
        send_alone(&node, malformed.clone(), 0).await?.is_err(),
        "a malformed transfer was accepted"
    );
    for (label, message, format, condition) in [
        (
            "oversized send",
            oversized.clone(),
            0,
            protocol_amqp::MESSAGE_SIZE_EXCEEDED,
        ),
        (
            "invalid send",
            invalid.clone(),
            0,
            protocol_amqp::INVALID_FIELD,
        ),
        (
            "oversized batch child",
            batch(&[fitting.clone(), oversized.clone()])?,
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            protocol_amqp::MESSAGE_SIZE_EXCEEDED,
        ),
        (
            "malformed batch child",
            batch(&[fitting.clone(), malformed.clone()])?,
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            protocol_amqp::INVALID_FIELD,
        ),
        (
            "invalid batch child",
            batch(&[fitting.clone(), invalid.clone()])?,
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            protocol_amqp::INVALID_FIELD,
        ),
    ] {
        phase(format_args!("malformed content: refusal {label}"));
        let outcome = send_alone(&node, message, format).await?;
        rejected_with(
            outcome.map_err(|error| format!("{label}: {error:?}"))?,
            condition,
        );
    }

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        // One refused message fails the whole request.
        for (label, message) in [
            ("oversized", &oversized),
            ("malformed", &malformed),
            ("invalid", &invalid),
        ] {
            let (status, response) = management
                .schedule(&[
                    (
                        "fitting",
                        encode_message(&scheduled(fitting.clone(), 9_000))?,
                    ),
                    (
                        "refused",
                        encode_message(&scheduled(message.clone(), 9_000))?,
                    ),
                ])
                .await?;
            assert_eq!(status, 400, "{label} schedule: {response:?}");
        }
        let (status, response) = management
            .schedule(&[("not-amqp", b"not an AMQP message".to_vec())])
            .await?;
        assert_eq!(status, 400, "{response:?}");
        connection.close().await?;
    }

    let after = node.snapshot().await?;
    phase("malformed content: refusal snapshot complete; patch boundaries");
    assert!(
        after == before,
        "a refusal changed stored state across a restart"
    );

    // The copy is charged its original bytes plus each overlay name and
    // value, where a binary value over 255 bytes has a five-byte prefix. One
    // byte past the allowance refuses and leaves the lock usable; exactly the
    // allowance is accepted. A lock taken by sequence ends with its
    // connection, so one holds it.
    let fitting_pad =
        LIMIT + domain::PROPERTY_PATCH_HEADROOM_BYTES - held_bytes.len() - "pad".len() - 5;
    let kept = [("pad", Value::Binary(Binary::from(vec![0_u8; fitting_pad])))];
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        let lock = lock_deferred(&mut management).await?;
        let (status, response) = management
            .update_disposition(
                "unused",
                lock.clone(),
                "abandoned",
                &[(
                    "pad",
                    Value::Binary(Binary::from(vec![0_u8; fitting_pad + 1])),
                )],
            )
            .await?;
        assert_eq!(status, 403, "{response:?}");
        assert_eq!(
            durable_node::error_condition(&response),
            Some(symbol(protocol_amqp::MESSAGE_SIZE_EXCEEDED))
        );
        let (status, response) = management
            .update_disposition("unused", lock, "abandoned", &kept)
            .await?;
        assert_eq!(
            status, 200,
            "the refusal kept the lock and the allowance fits: {response:?}"
        );
        connection.close().await?;
    }
    // A patch value failing the received checks fails the request frame
    // itself, which ends the management link before any patch is read.
    {
        phase("malformed content: malformed patch");
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        let lock = lock_deferred(&mut management).await?;
        assert!(
            management
                .update_disposition(
                    "unused",
                    lock,
                    "abandoned",
                    &[("compound-array", compound_array.clone())],
                )
                .await
                .is_err(),
            "a malformed patch value was accepted"
        );
        let _ = connection.close().await;
    }
    node.restart().await?;
    node.expire_locks("orders")?;
    node.reopen(|machine| {
        // Only the accepted patch reached the stored copy.
        let record = assert_stored(machine, "orders", 1, &held_bytes, &kept)?;
        assert_eq!(record.delivery_count, 3);
        Ok(())
    })
    .await?;

    // The copy still renders and the refusals consumed no sequence numbers.
    {
        phase("malformed content: final rendering and sequence");
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = Management::attach(&mut session, "orders").await?;
        let (status, entries) = management.receive_deferred(1).await?;
        assert_eq!(status, 200);
        let [entry] = entries.try_into().map_err(|_| "one deferred message")?;
        assert_renders(
            &entry.message,
            &entry.encoded,
            &held,
            Expect {
                delivery_count: 3,
                locked: true,
                overlay: &kept,
                state: Some(1),
                ..Expect::default()
            },
        );
        let (status, _) = management
            .update_disposition("unused", entry.lock.ok_or("a lock")?, "completed", &[])
            .await?;
        assert_eq!(status, 200);
        let mut sender = Sender::attach(&mut session, "sender", "orders").await?;
        sender.send(rich("next", data_body())).await?;
        let mut receiver = Receiver::attach(&mut session, "receiver", "orders").await?;
        let following = next(&mut receiver).await?;
        assert_eq!(sequence_of(following.message()), 2);
        receiver.accept(&following).await?;
        phase("malformed content: final connection close");
        connection.close().await?;
    }
    phase("malformed content: final original-owner shutdown");
    node.shutdown().await?;
    phase("malformed content: complete; destructors follow");
    Ok(())
}
