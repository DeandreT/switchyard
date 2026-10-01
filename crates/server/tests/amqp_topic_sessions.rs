//! Session ownership belongs to a subscription, not to the publishing topic.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection, ClientDelivery, ClientReceiver,
    ClientSender, ClientSession, Header, Message, Modified, OrderedMap, Outcome, Properties,
    Symbol, Value, encode_message,
};
use domain::{
    Command, CommandKind, CommandOutcome, EntityPath, NamespaceName, SequenceNumber, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, keys,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::{StateStore, StoreSnapshot};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(8);

#[path = "amqp_topic_sessions/fixture.rs"]
mod fixture;
use fixture::*;

async fn required_subscription_locks_are_independent_and_preserve_fifo<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "publisher", node.topic.as_str()),
    )
    .await??;
    for (text, id) in [("other-first", "B"), ("first", "A"), ("second", "A")] {
        accepted(timeout(DEADLINE, sender.send(message(text, Some(id)))).await??);
    }
    let mut alpha = receiving(&mut session, "alpha-A", node.alpha.as_str(), Some("A")).await?;
    let mut beta = receiving(&mut session, "beta-A", node.beta.as_str(), Some("A")).await?;
    assert_eq!(granted(&alpha), Some("A"));
    assert_eq!(granted(&beta), Some("A"));
    for (index, expected) in ["first", "second"].into_iter().enumerate() {
        let a = recv(&mut alpha).await?;
        let b = recv(&mut beta).await?;
        for delivery in [&a, &b] {
            assert_eq!(body(delivery.message()), expected.as_bytes());
            assert_eq!(group(delivery.message()), Some("A"));
            assert_eq!(sequence(delivery.message()), index as u64 + 2);
        }
        timeout(DEADLINE, alpha.accept(&a)).await??;
        timeout(DEADLINE, beta.accept(&b)).await??;
        node.wait_removed(&node.alpha, index as u64 + 2).await?;
        node.wait_removed(&node.beta, index as u64 + 2).await?;
    }
    assert!(
        timeout(Duration::from_millis(100), alpha.recv())
            .await
            .is_err()
    );
    let mut rival_session = timeout(DEADLINE, connection.begin()).await??;
    let mut rival = receiving(
        &mut rival_session,
        "rival-A",
        node.alpha.as_str(),
        Some("A"),
    )
    .await?;
    assert!(
        timeout(DEADLINE, rival.recv()).await?.is_err(),
        "the same subscription granted A twice"
    );
    timeout(DEADLINE, alpha.close()).await??;
    let mut replacement = receiving(
        &mut session,
        "replacement-A",
        node.alpha.as_str(),
        Some("A"),
    )
    .await?;
    assert_eq!(granted(&replacement), Some("A"));
    assert!(
        timeout(Duration::from_millis(100), replacement.recv())
            .await
            .is_err()
    );
    timeout(DEADLINE, replacement.close()).await??;
    timeout(DEADLINE, beta.close()).await??;
    for (name, entity) in [("alpha-next", &node.alpha), ("beta-next", &node.beta)] {
        let mut next = receiving(&mut session, name, entity.as_str(), None).await?;
        assert_eq!(
            granted(&next),
            Some("B"),
            "next-available must echo its grant"
        );
        let delivery = recv(&mut next).await?;
        assert_eq!(body(delivery.message()), b"other-first");
        assert_eq!(group(delivery.message()), Some("B"));
        assert_eq!(sequence(delivery.message()), 1);
        timeout(DEADLINE, next.accept(&delivery)).await??;
        node.wait_removed(entity, 1).await?;
        timeout(DEADLINE, next.close()).await??;
    }
    let mut ordinary = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "ordinary", node.ordinary.as_str()),
    )
    .await??;
    for (number, text, id) in [
        (1, "other-first", "B"),
        (2, "first", "A"),
        (3, "second", "A"),
    ] {
        let delivery = recv(&mut ordinary).await?;
        assert_eq!(sequence(delivery.message()), number);
        assert_eq!(body(delivery.message()), text.as_bytes());
        assert_eq!(
            group(delivery.message()),
            Some(id),
            "an ordinary subscription must retain group-id"
        );
        timeout(DEADLINE, ordinary.accept(&delivery)).await??;
        node.wait_removed(&node.ordinary, number).await?;
    }
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn management_and_deferred_receipts_cannot_cross_subscription_owners<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "publisher", node.topic.as_str()),
    )
    .await??;
    accepted(timeout(DEADLINE, sender.send(message("owned", Some("A")))).await??);
    let mut alpha = receiving(&mut session, "alpha-owner", node.alpha.as_str(), Some("A")).await?;
    let mut beta = receiving(&mut session, "beta-owner", node.beta.as_str(), Some("A")).await?;
    let mut alpha_management = Management::attach(
        &mut session,
        "alpha",
        "Orders/SUBSCRIPTIONS/Alpha/$Management",
    )
    .await?;
    let mut beta_management = Management::attach(
        &mut session,
        "beta",
        "Orders/Subscriptions/beta/$management",
    )
    .await?;
    for (management, link, state) in [
        (
            &mut alpha_management,
            "alpha-owner",
            b"alpha-state".as_slice(),
        ),
        (&mut beta_management, "beta-owner", b"beta-state".as_slice()),
    ] {
        let mut request = session_body("A");
        request.insert(
            Value::String(protocol_amqp::SESSION_STATE.into()),
            Value::Binary(state.to_vec().into()),
        );
        let response = management
            .request(
                "set-state",
                protocol_amqp::SET_SESSION_STATE_OPERATION,
                link,
                request,
            )
            .await?;
        status(&response, 200);
        let response = management
            .request(
                "get-state",
                protocol_amqp::GET_SESSION_STATE_OPERATION,
                link,
                session_body("A"),
            )
            .await?;
        status(&response, 200);
        assert_eq!(
            response_map(&response).get(&Value::String(protocol_amqp::SESSION_STATE.into())),
            Some(&Value::Binary(state.to_vec().into()))
        );
    }
    let a = recv(&mut alpha).await?;
    let b = recv(&mut beta).await?;
    assert_eq!(sequence(a.message()), sequence(b.message()));
    // Both links have delivered their only copy, so unrelated receive polling
    // cannot conceal a management command submitted with the wrong authority.
    for (operation, link, id) in [
        (
            protocol_amqp::GET_SESSION_STATE_OPERATION,
            "alpha-owner",
            "A",
        ),
        (
            protocol_amqp::SET_SESSION_STATE_OPERATION,
            "alpha-owner",
            "A",
        ),
        (
            protocol_amqp::RENEW_SESSION_LOCK_OPERATION,
            "alpha-owner",
            "A",
        ),
        (
            protocol_amqp::GET_SESSION_STATE_OPERATION,
            "beta-owner",
            "wrong",
        ),
    ] {
        let before = node.snapshot()?;
        let submits = node.submissions();
        let mut request = session_body(id);
        request.insert(
            Value::String(protocol_amqp::SESSION_STATE.into()),
            Value::Binary(b"forged".to_vec().into()),
        );
        let response = beta_management
            .request("wrong-owner", operation, link, request)
            .await?;
        status(&response, 410);
        assert_eq!(
            node.submissions(),
            submits,
            "cross-owner management must not submit a command"
        );
        assert_eq!(node.snapshot()?, before);
    }
    node.clock.set(2_000);
    let response = alpha_management
        .request(
            "renew-alpha",
            protocol_amqp::RENEW_SESSION_LOCK_OPERATION,
            "alpha-owner",
            session_body("A"),
        )
        .await?;
    status(&response, 200);
    assert!(
        matches!(response_map(&response).get(&Value::String(protocol_amqp::EXPIRATION.into())),
        Some(Value::Timestamp(time)) if time.milliseconds() == 32_000)
    );
    assert_eq!(
        node.session(&node.alpha, "A")?
            .lock
            .expect("alpha hold")
            .locked_until,
        Timestamp::from_millis(32_000)
    );
    assert_eq!(
        node.session(&node.beta, "A")?
            .lock
            .expect("beta hold")
            .locked_until,
        Timestamp::from_millis(31_000)
    );
    timeout(
        DEADLINE,
        alpha.modify(
            &a,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        ),
    )
    .await??;
    node.wait_deferred(&node.alpha, 1).await?;
    node.wait_waiting(&node.alpha).await?;
    let before = node.snapshot()?;
    let submits = node.submissions();
    let wrong = beta_management
        .request(
            "wrong-deferred",
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            "alpha-owner",
            sequences(1, "A"),
        )
        .await?;
    status(&wrong, 410);
    assert_eq!(node.submissions(), submits);
    assert_eq!(node.snapshot()?, before);
    let deferred = alpha_management
        .request(
            "receive-deferred",
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            "alpha-owner",
            sequences(1, "A"),
        )
        .await?;
    status(&deferred, 200);
    assert_eq!(group(&decoded(&deferred)?), Some("A"));
    assert_eq!(body(&decoded(&deferred)?), b"owned");
    let Some(Value::Uuid(lock)) =
        first_entry(&deferred).get(&Value::String(protocol_amqp::LOCK_TOKEN.into()))
    else {
        panic!("deferred lock token")
    };
    let completion = map([
        (
            protocol_amqp::LOCK_TOKENS,
            Value::Array(Array::from(vec![Value::Uuid(lock.clone())])),
        ),
        (
            protocol_amqp::DISPOSITION_STATUS,
            Value::String("completed".into()),
        ),
    ]);
    let before = node.snapshot()?;
    let submits = node.submissions();
    let wrong = beta_management
        .request(
            "wrong-completion",
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "alpha-owner",
            completion.clone(),
        )
        .await?;
    status(&wrong, 410);
    assert_eq!(node.submissions(), submits);
    assert_eq!(node.snapshot()?, before);
    let response = alpha_management
        .request(
            "complete-deferred",
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "alpha-owner",
            completion,
        )
        .await?;
    status(&response, 200);
    node.wait_removed(&node.alpha, 1).await?;
    assert!(
        node.record(&node.beta, 1)?.is_some(),
        "one completion removed its sibling"
    );
    timeout(DEADLINE, beta.reject(&b, None)).await??;
    node.wait_removed(&node.beta, 1).await?;
    let shadow = node.beta.dead_letter_queue()?;
    let mut dead_letters = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "beta-deadletters", shadow.as_str()),
    )
    .await??;
    let dead = recv(&mut dead_letters).await?;
    assert_eq!(body(dead.message()), b"owned");
    assert_eq!(group(dead.message()), None);
    assert_eq!(sequence(dead.message()), 1);
    assert_eq!(
        dead.message().header.as_ref().and_then(|header| header.ttl),
        None
    );
    assert_eq!(
        dead.message()
            .properties
            .as_ref()
            .and_then(|properties| properties.absolute_expiry_time),
        None
    );
    timeout(DEADLINE, dead_letters.accept(&dead)).await??;
    node.wait_removed(&shadow, 1).await?;
    timeout(DEADLINE, alpha.close()).await??;
    timeout(DEADLINE, beta.close()).await??;
    let reopened = receiving(
        &mut session,
        "alpha-reopened",
        node.alpha.as_str(),
        Some("A"),
    )
    .await?;
    let response = alpha_management
        .request(
            "persistent-state",
            protocol_amqp::GET_SESSION_STATE_OPERATION,
            "alpha-reopened",
            session_body("A"),
        )
        .await?;
    status(&response, 200);
    assert_eq!(
        response_map(&response).get(&Value::String(protocol_amqp::SESSION_STATE.into())),
        Some(&Value::Binary(b"alpha-state".to_vec().into()))
    );
    timeout(DEADLINE, reopened.close()).await??;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn mixed_topic_batch_dead_letters_only_the_missing_session_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "mixed-publisher", node.topic.as_str()),
    )
    .await??;
    let messages = [
        message("group-A", Some("A")),
        message("group-B", Some("B")),
        message("missing", None),
    ];
    let batch = Message {
        body: Body::Data(
            messages
                .iter()
                .map(encode_message)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(Into::into)
                .collect(),
        ),
        ..Message::default()
    };
    accepted(
        timeout(
            DEADLINE,
            sender.send_with_message_format(batch, protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT),
        )
        .await??,
    );
    for (name, entity) in [("alpha", &node.alpha), ("beta", &node.beta)] {
        for (id, expected, number) in [("A", "group-A", 1), ("B", "group-B", 2)] {
            let mut receiver = receiving(
                &mut session,
                &format!("{name}-{id}"),
                entity.as_str(),
                Some(id),
            )
            .await?;
            let delivery = recv(&mut receiver).await?;
            assert_eq!(body(delivery.message()), expected.as_bytes());
            assert_eq!(group(delivery.message()), Some(id));
            assert_eq!(sequence(delivery.message()), number);
            timeout(DEADLINE, receiver.accept(&delivery)).await??;
            node.wait_removed(entity, number).await?;
            timeout(DEADLINE, receiver.close()).await??;
        }
        assert!(
            node.record(entity, 3)?.is_none(),
            "a missing-session copy reached the primary subscription"
        );
        let shadow = entity.dead_letter_queue()?;
        let retained = node
            .record(&shadow, 3)?
            .expect("missing-session dead letter");
        assert_eq!(retained.session_id, None);
        assert_eq!(retained.expires_at, None);
        assert_eq!(retained.delivery_count, 0);
        let mut dead_letters = timeout(
            DEADLINE,
            ClientReceiver::attach(&mut session, format!("{name}-deadletters"), shadow.as_str()),
        )
        .await??;
        let delivery = recv(&mut dead_letters).await?;
        assert_eq!(body(delivery.message()), b"missing");
        assert_eq!(group(delivery.message()), None);
        assert_eq!(sequence(delivery.message()), 3);
        let properties = delivery
            .message()
            .application_properties
            .as_ref()
            .expect("DLQ reason");
        assert_eq!(
            properties.get("DeadLetterReason"),
            Some(&Value::String("Session ID is null".into()))
        );
        assert_eq!(
            properties.get("DeadLetterErrorDescription"),
            Some(&Value::String(
                "Session enabled entity doesn't allow a message whose session identifier is null."
                    .into()
            ))
        );
        assert_eq!(
            delivery
                .message()
                .header
                .as_ref()
                .and_then(|header| header.ttl),
            None
        );
        assert_eq!(
            delivery
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.absolute_expiry_time),
            None
        );
        assert_eq!(
            delivery
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.message_id.clone()),
            Some("id-missing".into())
        );
        timeout(DEADLINE, dead_letters.accept(&delivery)).await??;
        node.wait_removed(&shadow, 3).await?;
    }
    let mut ordinary = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "ordinary-batch", node.ordinary.as_str()),
    )
    .await??;
    for (index, original) in messages.iter().enumerate() {
        let delivery = recv(&mut ordinary).await?;
        assert_eq!(body(delivery.message()), body(original));
        assert_eq!(group(delivery.message()), group(original));
        assert_eq!(sequence(delivery.message()), index as u64 + 1);
        assert_eq!(
            delivery
                .message()
                .header
                .as_ref()
                .and_then(|header| header.ttl),
            Some(50_000)
        );
        timeout(DEADLINE, ordinary.accept(&delivery)).await??;
        node.wait_removed(&node.ordinary, index as u64 + 1).await?;
    }
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::MemoryProvider::new())).await?
            })+
        }
        mod durable {
            $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::DurableProvider::temporary()?)).await?
            })+
        }
    };
}

for_each_backend!(
    required_subscription_locks_are_independent_and_preserve_fifo,
    management_and_deferred_receipts_cannot_cross_subscription_owners,
    mixed_topic_batch_dead_letters_only_the_missing_session_copies
);

#[path = "amqp_topic_sessions/refusal.rs"]
mod refusal;
