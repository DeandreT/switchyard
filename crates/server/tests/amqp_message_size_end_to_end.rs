//! A receiving peer's link `max-message-size` bounds every outbound transfer.
//!
//! Each test measures the exact rendered size with an unlimited link first,
//! using an identical twin message, then attaches links at that size and one
//! byte under it. A refused delivery must never be lost: its lock expires and
//! the message is delivered again, or dead-letters once its refusals reach the
//! queue's max delivery count.

use std::{error::Error, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientDelivery, ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session,
    Message, Modified, OrderedMap, Outcome, Properties, SenderSettleMode, Symbol, Value,
};
use domain::{
    CommandKind, DEFAULT_LOCK_DURATION_MILLIS, EntityPath, NamespaceName, QueueConfig, StateMachine,
};
use serde_amqp::{described::Described, descriptor::Descriptor};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

#[path = "../src/test_deadline.rs"]
mod test_deadline;

use test_deadline::WAIT;

/// Long enough for a delivery that is going to arrive to arrive.
const QUIET: Duration = Duration::from_millis(300);

struct Node {
    broker: Broker,
    clock: ManualClock,
    address: String,
}

impl Node {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            clock.clone(),
        ));
        for (name, config) in [
            ("orders", QueueConfig::default()),
            (
                SESSIONS,
                QueueConfig {
                    requires_session: true,
                    ..QueueConfig::default()
                },
            ),
            (
                BRITTLE,
                QueueConfig {
                    max_delivery_count: 2,
                    ..QueueConfig::default()
                },
            ),
        ] {
            broker.handle().submit_blocking(
                namespace(),
                EntityPath::new(name)?,
                CommandKind::CreateQueue { config },
            )?;
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let handle = broker.handle();
        tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace())
                .serve(listener)
                .await;
        });
        Ok(Self {
            broker,
            clock,
            address,
        })
    }

    async fn session(&self) -> Result<(Connection, Session), Box<dyn Error>> {
        let mut connection = Connection::builder()
            .container_id("message-size-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?;
        let session = Session::begin(&mut connection).await?;
        Ok((connection, session))
    }

    /// Lets every outstanding lock lapse, returning its message.
    async fn expire_locks(&self) -> Result<(), Box<dyn Error>> {
        self.clock.advance(DEFAULT_LOCK_DURATION_MILLIS + 1);
        for name in ["orders", SESSIONS, BRITTLE] {
            let queue = EntityPath::new(name)?;
            for entity in [queue.dead_letter_queue()?, queue] {
                self.broker
                    .handle()
                    .submit(namespace(), entity, CommandKind::ExpireLocks)
                    .await?;
            }
        }
        Ok(())
    }
}

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").expect("a valid namespace")
}

/// A session-enabled queue.
const SESSIONS: &str = "carts";
/// A queue that dead-letters on a message's second delivery.
const BRITTLE: &str = "brittle";
/// Smaller than any twin and larger than any error response.
const TOO_SMALL: u64 = 600;

/// Identical bodies render to identical sizes: sequence numbers, lock tokens
/// and timestamps all encode at fixed widths here.
fn twin() -> Message {
    Message::builder()
        .body(Body::Data(vec![vec![b'x'; 900].into()]))
        .build()
}

async fn send_twins(session: &mut Session, count: usize) -> Result<(), Box<dyn Error>> {
    send_twins_to(session, "orders", None, count).await
}

async fn send_twins_to(
    session: &mut Session,
    address: &str,
    group: Option<&str>,
    count: usize,
) -> Result<(), Box<dyn Error>> {
    let mut sender = Sender::attach(session, "twin-sender", address).await?;
    for _ in 0..count {
        let mut message = twin();
        message.properties = group.map(|group| Properties {
            group_id: Some(group.to_owned()),
            ..Properties::default()
        });
        assert!(matches!(sender.send(message).await?, Outcome::Accepted(_)));
    }
    sender.close().await?;
    Ok(())
}

/// A receiver holding the named session of the session-enabled queue.
async fn session_receiver(
    session: &mut Session,
    name: &str,
    session_id: &str,
    mode: SenderSettleMode,
    max_message_size: Option<u64>,
) -> Result<Receiver, Box<dyn Error>> {
    let mut filters = amqp::FilterSet::default();
    filters.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::Described(Box::new(Described {
            descriptor: Descriptor::Name(Symbol::from(protocol_amqp::SESSION_FILTER)),
            value: Value::String(session_id.to_owned()),
        })),
    );
    Ok(Receiver::builder()
        .name(name)
        .source(amqp::Source {
            address: Some(SESSIONS.to_owned()),
            filter: Some(filters),
            ..Default::default()
        })
        .sender_settle_mode(mode)
        .max_message_size(max_message_size)
        .attach(session)
        .await?)
}

async fn receiver(
    session: &mut Session,
    name: &str,
    mode: SenderSettleMode,
    max_message_size: Option<u64>,
) -> Result<Receiver, Box<dyn Error>> {
    receiver_from(session, "orders", name, mode, max_message_size).await
}

async fn receiver_from(
    session: &mut Session,
    source: &str,
    name: &str,
    mode: SenderSettleMode,
    max_message_size: Option<u64>,
) -> Result<Receiver, Box<dyn Error>> {
    Ok(Receiver::builder()
        .name(name)
        .source(source)
        .sender_settle_mode(mode)
        .max_message_size(max_message_size)
        .attach(session)
        .await?)
}

async fn recv(receiver: &mut Receiver) -> Result<ClientDelivery, Box<dyn Error>> {
    Ok(tokio::time::timeout(WAIT, receiver.recv()).await??)
}

/// The link is detached by the broker and no delivery reached the peer.
async fn assert_refused(receiver: &mut Receiver) {
    let received = tokio::time::timeout(WAIT, receiver.recv())
        .await
        .expect("an oversized delivery detaches the link");
    assert!(
        matches!(received, Err(amqp::EngineError::RemoteDetached)),
        "no oversized delivery may reach the peer: {:?}",
        received.map(|delivery| delivery.encoded_message().len())
    );
    let error = receiver
        .detach_error()
        .expect("the broker's detach explains the refusal");
    assert_eq!(
        error.condition.as_symbol().as_str(),
        protocol_amqp::MESSAGE_SIZE_EXCEEDED
    );
}

async fn assert_no_delivery(receiver: &mut Receiver) {
    assert!(
        tokio::time::timeout(QUIET, receiver.recv()).await.is_err(),
        "the queue should be empty"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn peek_lock_is_bounded_at_the_exact_limit_and_never_loses_the_message()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    // Receivers fetch ahead of the client, so the probe runs alone.
    send_twins(&mut session, 1).await?;
    let mut probe = receiver(&mut session, "probe", SenderSettleMode::Unsettled, None).await?;
    let probed = recv(&mut probe).await?;
    let size = probed.encoded_message().len() as u64;
    probe.accept(&probed).await?;
    probe.close().await?;
    send_twins(&mut session, 2).await?;

    let mut over = receiver(
        &mut session,
        "one-byte-under",
        SenderSettleMode::Unsettled,
        Some(size - 1),
    )
    .await?;
    assert_refused(&mut over).await;

    // The refused delivery kept its lock, so only the third twin is ready.
    let mut exact = receiver(
        &mut session,
        "exact",
        SenderSettleMode::Unsettled,
        Some(size),
    )
    .await?;
    let third = recv(&mut exact).await?;
    assert_eq!(third.encoded_message().len() as u64, size);
    exact.accept(&third).await?;
    assert_no_delivery(&mut exact).await;
    exact.close().await?;

    // Its lock lapses rather than dead-lettering; an unlimited (zero) peer
    // then receives it. The redelivery counts one more delivery, which can
    // grow the header, so it need not fit the exact limit any more.
    node.expire_locks().await?;
    let mut unlimited = receiver(
        &mut session,
        "unlimited",
        SenderSettleMode::Unsettled,
        Some(0),
    )
    .await?;
    let refused = recv(&mut unlimited).await?;
    assert_eq!(refused.message().body, twin().body);
    unlimited.accept(&refused).await?;
    assert_no_delivery(&mut unlimited).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_and_delete_measures_before_deleting() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    send_twins(&mut session, 1).await?;
    let mut probe = receiver(&mut session, "probe", SenderSettleMode::Settled, None).await?;
    let size = recv(&mut probe).await?.encoded_message().len() as u64;
    probe.close().await?;
    send_twins(&mut session, 2).await?;

    let mut over = receiver(
        &mut session,
        "one-byte-under",
        SenderSettleMode::Settled,
        Some(size - 1),
    )
    .await?;
    assert_refused(&mut over).await;

    // A bounded receive-and-delete renders exactly as an unbounded one.
    let mut exact = receiver(&mut session, "exact", SenderSettleMode::Settled, Some(size)).await?;
    let third = recv(&mut exact).await?;
    assert_eq!(third.encoded_message().len() as u64, size);
    assert!(
        third
            .message()
            .delivery_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-lock-token")))
            .is_none(),
        "a receive-and-delete delivery carries no lock"
    );
    assert_no_delivery(&mut exact).await;
    exact.close().await?;

    // The refused message was measured, not deleted, while the one delivered
    // at the limit was deleted. A redelivery counts one more delivery, which
    // can grow its header, so an unlimited peer collects it.
    node.expire_locks().await?;
    let mut after = receiver(&mut session, "after", SenderSettleMode::Settled, None).await?;
    let refused = recv(&mut after).await?;
    assert_eq!(refused.message().body, twin().body);
    assert_no_delivery(&mut after).await;
    Ok(())
}

/// Moves every ready twin to the dead-letter queue.
async fn dead_letter_twins(session: &mut Session, count: usize) -> Result<(), Box<dyn Error>> {
    send_twins(session, count).await?;
    let mut receiver = receiver(session, "rejecter", SenderSettleMode::Unsettled, None).await?;
    for _ in 0..count {
        let delivery = recv(&mut receiver).await?;
        receiver.reject(&delivery, None).await?;
    }
    receiver.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_letter_receive_is_bounded_and_keeps_the_message() -> Result<(), Box<dyn Error>> {
    const DEAD_LETTERS: &str = "orders/$deadletterqueue";
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    dead_letter_twins(&mut session, 1).await?;
    let mut probe = receiver_from(
        &mut session,
        DEAD_LETTERS,
        "dlq-probe",
        SenderSettleMode::Settled,
        None,
    )
    .await?;
    let size = recv(&mut probe).await?.encoded_message().len() as u64;
    probe.close().await?;
    dead_letter_twins(&mut session, 2).await?;

    let mut over = receiver_from(
        &mut session,
        DEAD_LETTERS,
        "dlq-under",
        SenderSettleMode::Settled,
        Some(size - 1),
    )
    .await?;
    assert_refused(&mut over).await;
    let mut exact = receiver_from(
        &mut session,
        DEAD_LETTERS,
        "dlq-exact",
        SenderSettleMode::Settled,
        Some(size),
    )
    .await?;
    assert_eq!(recv(&mut exact).await?.encoded_message().len() as u64, size);
    assert_no_delivery(&mut exact).await;
    exact.close().await?;

    node.expire_locks().await?;
    let mut after = receiver_from(
        &mut session,
        DEAD_LETTERS,
        "dlq-after",
        SenderSettleMode::Settled,
        None,
    )
    .await?;
    assert_eq!(recv(&mut after).await?.message().body, twin().body);
    assert_no_delivery(&mut after).await;
    Ok(())
}

struct Management {
    requests: Sender,
    next_id: u32,
}

impl Management {
    async fn attach(session: &mut Session, queue: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            requests: Sender::attach(session, "size-requests", format!("{queue}/$management"))
                .await?,
            next_id: 0,
        })
    }

    async fn reply_link(
        session: &mut Session,
        reply_to: &str,
        max_message_size: Option<u64>,
    ) -> Result<Receiver, Box<dyn Error>> {
        Self::reply_link_on(session, "orders", reply_to, max_message_size).await
    }

    async fn reply_link_on(
        session: &mut Session,
        queue: &str,
        reply_to: &str,
        max_message_size: Option<u64>,
    ) -> Result<Receiver, Box<dyn Error>> {
        Ok(Receiver::builder()
            .name(format!("{reply_to}-link"))
            .source(format!("{queue}/$management"))
            .target(reply_to)
            .max_message_size(max_message_size)
            .attach(session)
            .await?)
    }

    /// Sends one request; returns the response's status, size and message.
    async fn request(
        &mut self,
        replies: &mut Receiver,
        reply_to: &str,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> Result<(i32, u64, Message), Box<dyn Error>> {
        self.send(reply_to, operation, body).await?;
        Self::response(replies).await
    }

    async fn send(
        &mut self,
        reply_to: &str,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> Result<(), Box<dyn Error>> {
        // Fixed-width ids keep every response's correlation id the same size.
        self.next_id += 1;
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(format!("request-{:04}", self.next_id).into()),
                reply_to: Some(reply_to.to_owned()),
                ..Properties::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                    .build(),
            )
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        Ok(())
    }

    async fn response(replies: &mut Receiver) -> Result<(i32, u64, Message), Box<dyn Error>> {
        let response = recv(replies).await?;
        replies.accept(&response).await?;
        let status = match response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY))
        {
            Some(Value::Int(status)) => *status,
            other => return Err(format!("a response without a status: {other:?}").into()),
        };
        Ok((
            status,
            response.encoded_message().len() as u64,
            response.message().clone(),
        ))
    }
}

fn error_condition(message: &Message) -> Option<String> {
    match message
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY))
    {
        Some(Value::Symbol(symbol)) => Some(symbol.as_str().to_owned()),
        Some(Value::String(text)) => Some(text.clone()),
        _ => None,
    }
}

fn message_count(response: &Message) -> usize {
    let Body::Value(Value::Map(body)) = &response.body else {
        return 0;
    };
    match body.get(&Value::String(String::from("messages"))) {
        Some(Value::List(entries)) => entries.len(),
        _ => 0,
    }
}

fn peek_body(from_sequence: i64, count: i32) -> OrderedMap<Value, Value> {
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(String::from("from-sequence-number")),
        Value::Long(from_sequence),
    );
    body.insert(
        Value::String(String::from("message-count")),
        Value::Int(count),
    );
    body
}

fn receive_body(sequence: i64, settle_mode: u32) -> OrderedMap<Value, Value> {
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(String::from("sequence-numbers")),
        Value::Array(Array::from(vec![Value::Long(sequence)])),
    );
    body.insert(
        Value::String(String::from("receiver-settle-mode")),
        Value::Uint(settle_mode),
    );
    body
}

const PEEK: &str = "com.microsoft:peek-message";

#[tokio::test(flavor = "multi_thread")]
async fn peek_returns_the_longest_prefix_that_fits_the_reply_link() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    send_twins(&mut session, 3).await?;
    let mut management = Management::attach(&mut session, "orders").await?;

    let mut unlimited = Management::reply_link(&mut session, "unlimited", None).await?;
    let (status, three, _) = management
        .request(&mut unlimited, "unlimited", PEEK, peek_body(0, 3))
        .await?;
    assert_eq!(status, 200);
    let (_, one, _) = management
        .request(&mut unlimited, "unlimited", PEEK, peek_body(0, 1))
        .await?;

    let mut exact = Management::reply_link(&mut session, "exactly", Some(three)).await?;
    let (status, size, response) = management
        .request(&mut exact, "exactly", PEEK, peek_body(0, 3))
        .await?;
    assert_eq!((status, size, message_count(&response)), (200, three, 3));

    let mut short = Management::reply_link(&mut session, "shorter", Some(three - 1)).await?;
    let (status, size, response) = management
        .request(&mut short, "shorter", PEEK, peek_body(0, 3))
        .await?;
    assert_eq!((status, message_count(&response)), (200, 2));
    assert!(size < three);

    let mut tiny = Management::reply_link(&mut session, "tinier", Some(one - 1)).await?;
    let (status, _, response) = management
        .request(&mut tiny, "tinier", PEEK, peek_body(0, 3))
        .await?;
    assert_eq!(status, 403);
    assert_eq!(
        error_condition(&response).as_deref(),
        Some(protocol_amqp::MESSAGE_SIZE_EXCEEDED)
    );

    // A reply link attached only after the request was processed still never
    // receives an oversized reply.
    management.send("late", PEEK, peek_body(0, 3)).await?;
    let mut late = Management::reply_link(&mut session, "late", Some(three - 1)).await?;
    let (status, _, response) = Management::response(&mut late).await?;
    assert_eq!(status, 403);
    assert_eq!(
        error_condition(&response).as_deref(),
        Some(protocol_amqp::MESSAGE_SIZE_EXCEEDED)
    );
    Ok(())
}

/// Defers every twin through one ordinary receiver and returns their sequences.
async fn defer_twins(session: &mut Session, count: usize) -> Result<Vec<i64>, Box<dyn Error>> {
    defer_twins_in(session, "orders", count).await
}

async fn defer_twins_in(
    session: &mut Session,
    queue: &str,
    count: usize,
) -> Result<Vec<i64>, Box<dyn Error>> {
    send_twins_to(session, queue, None, count).await?;
    let mut receiver = receiver_from(
        session,
        queue,
        "deferrer",
        SenderSettleMode::Unsettled,
        None,
    )
    .await?;
    let mut sequences = Vec::with_capacity(count);
    for _ in 0..count {
        let delivery = recv(&mut receiver).await?;
        match delivery
            .message()
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-sequence-number")))
        {
            Some(Value::Long(sequence)) => sequences.push(*sequence),
            other => return Err(format!("no sequence annotation: {other:?}").into()),
        }
        receiver
            .modify(
                &delivery,
                Modified {
                    undeliverable_here: Some(true),
                    ..Modified::default()
                },
            )
            .await?;
    }
    receiver.close().await?;
    Ok(sequences)
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_by_sequence_is_all_or_nothing_against_the_reply_link() -> Result<(), Box<dyn Error>>
{
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    let sequences = defer_twins(&mut session, 4).await?;
    let mut management = Management::attach(&mut session, "orders").await?;
    let receive = protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION;

    // receiver-settle-mode 1 is peek-lock, 0 receive-and-delete.
    for (mode, probe, refused) in [
        (1, sequences[0], sequences[1]),
        (0, sequences[2], sequences[3]),
    ] {
        let mut unlimited =
            Management::reply_link(&mut session, &format!("unlimited-{mode}"), None).await?;
        let (status, size, _) = management
            .request(
                &mut unlimited,
                &format!("unlimited-{mode}"),
                receive,
                receive_body(probe, mode),
            )
            .await?;
        assert_eq!(status, 200);

        let under = format!("under-{mode}");
        let mut short = Management::reply_link(&mut session, &under, Some(size - 1)).await?;
        let (status, _, response) = management
            .request(&mut short, &under, receive, receive_body(refused, mode))
            .await?;
        assert_eq!(status, 403);
        assert_eq!(
            error_condition(&response).as_deref(),
            Some(protocol_amqp::MESSAGE_SIZE_EXCEEDED)
        );

        // Nothing was deleted: once the lock lapses, the message is returned
        // at exactly the limit.
        node.expire_locks().await?;
        let exact = format!("exact-{mode}");
        let mut fits = Management::reply_link(&mut session, &exact, Some(size)).await?;
        let (status, returned, response) = management
            .request(&mut fits, &exact, receive, receive_body(refused, mode))
            .await?;
        assert_eq!((status, returned, message_count(&response)), (200, size, 1));

        if mode == 0 {
            // Returned in receive-and-delete mode means deleted.
            node.expire_locks().await?;
            let (status, _, _) = management
                .request(&mut fits, &exact, receive, receive_body(refused, mode))
                .await?;
            assert_ne!(status, 200, "a deleted message was returned twice");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn session_receives_are_bounded_in_both_modes() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    for (mode, cart) in [
        (SenderSettleMode::Unsettled, "cart-lock"),
        (SenderSettleMode::Settled, "cart-delete"),
    ] {
        send_twins_to(&mut session, SESSIONS, Some(cart), 1).await?;
        let mut probe = session_receiver(
            &mut session,
            &format!("{cart}-probe"),
            cart,
            mode.clone(),
            None,
        )
        .await?;
        let probed = recv(&mut probe).await?;
        let size = probed.encoded_message().len() as u64;
        probe.accept(&probed).await?;
        probe.close().await?;
        send_twins_to(&mut session, SESSIONS, Some(cart), 2).await?;

        let mut over = session_receiver(
            &mut session,
            &format!("{cart}-under"),
            cart,
            mode.clone(),
            Some(size - 1),
        )
        .await?;
        assert_refused(&mut over).await;

        // Detaching released the session; the refused message stays locked.
        let mut exact = session_receiver(
            &mut session,
            &format!("{cart}-exact"),
            cart,
            mode.clone(),
            Some(size),
        )
        .await?;
        let third = recv(&mut exact).await?;
        assert_eq!(third.encoded_message().len() as u64, size);
        exact.accept(&third).await?;
        assert_no_delivery(&mut exact).await;
        exact.close().await?;

        node.expire_locks().await?;
        let mut after =
            session_receiver(&mut session, &format!("{cart}-after"), cart, mode, None).await?;
        let refused = recv(&mut after).await?;
        assert_eq!(refused.message().body, twin().body);
        after.accept(&refused).await?;
        assert_no_delivery(&mut after).await;
        after.close().await?;
    }
    Ok(())
}

fn dead_letter_reason(delivery: &ClientDelivery) -> Option<String> {
    match delivery
        .message()
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get(protocol_amqp::DEAD_LETTER_REASON_PROPERTY))
    {
        Some(Value::String(reason)) => Some(reason.clone()),
        _ => None,
    }
}

async fn assert_dead_lettered_once(session: &mut Session) -> Result<(), Box<dyn Error>> {
    let mut dead_letters = receiver_from(
        session,
        &format!("{BRITTLE}/$deadletterqueue"),
        "brittle-dead-letters",
        SenderSettleMode::Unsettled,
        None,
    )
    .await?;
    let dead = recv(&mut dead_letters).await?;
    assert_eq!(dead.message().body, twin().body);
    assert_eq!(
        dead_letter_reason(&dead).as_deref(),
        Some("MaxDeliveryCountExceeded")
    );
    dead_letters.accept(&dead).await?;
    assert_no_delivery(&mut dead_letters).await;
    dead_letters.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_refusal_dead_letters_at_the_max_delivery_count() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    // Staged receive-and-delete locks count deliveries like peek-lock ones.
    for mode in [SenderSettleMode::Unsettled, SenderSettleMode::Settled] {
        send_twins_to(&mut session, BRITTLE, None, 1).await?;
        for attempt in 0..2 {
            let mut refused = receiver_from(
                &mut session,
                BRITTLE,
                &format!("brittle-{attempt}"),
                mode.clone(),
                Some(TOO_SMALL),
            )
            .await?;
            assert_refused(&mut refused).await;
            node.expire_locks().await?;
        }
        let mut ready = receiver_from(
            &mut session,
            BRITTLE,
            "brittle-ready",
            SenderSettleMode::Unsettled,
            None,
        )
        .await?;
        assert_no_delivery(&mut ready).await;
        ready.close().await?;
        assert_dead_lettered_once(&mut session).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_receive_by_sequence_lock_dead_letters_at_the_max_delivery_count()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let (_connection, mut session) = node.session().await?;
    let mut management = Management::attach(&mut session, BRITTLE).await?;
    let mut short =
        Management::reply_link_on(&mut session, BRITTLE, "short", Some(TOO_SMALL)).await?;
    for mode in [1, 0] {
        // Deferral took the first delivery; the refused lock is the second.
        let [sequence] = defer_twins_in(&mut session, BRITTLE, 1).await?[..] else {
            return Err("one deferred twin".into());
        };
        let (status, _, _) = management
            .request(
                &mut short,
                "short",
                protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
                receive_body(sequence, mode),
            )
            .await?;
        assert_eq!(status, 403);
        node.expire_locks().await?;
        assert_dead_lettered_once(&mut session).await?;
    }
    Ok(())
}
