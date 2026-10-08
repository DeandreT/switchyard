//! Capacity remains bounded by externally held Second-mode confirmations.

use std::{
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, Binary, DeliveryConfirmation, DeliveryState, Detach, Disposition, End,
    EngineError, Flow, Frame, LinkEndpoint, Message, Open, PendingDelivery, Performative,
    ProtocolHeader, ReceiverSettleMode, Role, Sender, SenderSettleMode, ServerConnection,
    ServerSession, Source, decode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
    time::timeout,
};

const LIMIT: Duration = Duration::from_secs(3);
const CAPACITY: usize = 256;
const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;

struct WitnessIo {
    inner: DuplexStream,
    dropped: Arc<AtomicBool>,
}

impl Drop for WitnessIo {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl AsyncRead for WitnessIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WitnessIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

fn message(marker: u32) -> Message {
    Message::data(marker.to_be_bytes().to_vec())
}

fn tag(marker: u32) -> Binary {
    Binary::from(marker.to_be_bytes().to_vec())
}

async fn control(peer: &mut DuplexStream) -> (u16, Performative) {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(LIMIT, read_frame(peer))
        .await
        .expect("control frame deadline")
        .expect("valid control frame")
    else {
        panic!("expected AMQP performative")
    };
    assert!(payload.is_empty(), "control frames carry no message");
    (channel, performative)
}

async fn attach(session: &mut ServerSession, peer: &mut DuplexStream) -> Sender {
    let attach = Attach {
        name: String::from("capacity-link"),
        handle: HANDLE,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::Second,
        source: Some(Source::new("orders")),
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    write_frame(
        peer,
        &frame(CHANNEL, Performative::Attach(Box::new(attach))),
    )
    .await
    .expect("peer Attach");
    let incoming = timeout(LIMIT, session.next_incoming_attach())
        .await
        .expect("incoming Attach deadline")
        .expect("Attach offered");
    let endpoint = timeout(LIMIT, session.accept_attach(incoming, 1024 * 1024))
        .await
        .expect("accept link deadline")
        .expect("link accepted");
    let (channel, response) = control(peer).await;
    let Performative::Attach(response) = response else {
        panic!("server must answer Attach")
    };
    assert_eq!(channel, CHANNEL);
    assert_eq!(response.handle, HANDLE);
    assert_eq!(response.name, "capacity-link");
    assert_eq!(response.rcv_settle_mode, ReceiverSettleMode::Second);
    write_frame(
        peer,
        &frame(
            CHANNEL,
            Performative::Flow(Flow {
                handle: Some(HANDLE),
                delivery_count: Some(0),
                link_credit: Some(CAPACITY as u32 + 3),
                incoming_window: 2048,
                outgoing_window: 2048,
                ..Flow::default()
            }),
        ),
    )
    .await
    .expect("spare remote credit");
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("peer receiver creates native sender")
    };
    sender
}

struct Fixture {
    connection: ServerConnection,
    session: ServerSession,
    sender: Sender,
    peer: DuplexStream,
    dropped: Arc<AtomicBool>,
}

impl Fixture {
    async fn new() -> Self {
        let (inner, mut peer) = duplex(64 * 1024);
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = WitnessIo {
            inner,
            dropped: Arc::clone(&dropped),
        };
        let (connection, ()) = timeout(LIMIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "capacity-server", None),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .expect("peer header");
                    assert_eq!(
                        read_protocol_header(&mut peer)
                            .await
                            .expect("server header"),
                        ProtocolHeader::AMQP
                    );
                    write_frame(
                        &mut peer,
                        &frame(0, Performative::Open(Open::new("capacity-peer"))),
                    )
                    .await
                    .expect("peer Open");
                    assert!(matches!(
                        control(&mut peer).await,
                        (0, Performative::Open(_))
                    ));
                }
            )
        })
        .await
        .expect("native Open deadline");
        let mut connection = connection.expect("native accepted connection");
        write_frame(
            &mut peer,
            &frame(CHANNEL, Performative::Begin(Begin::default())),
        )
        .await
        .expect("peer Begin");
        let incoming = timeout(LIMIT, connection.next_incoming_session())
            .await
            .expect("incoming Begin deadline")
            .expect("Begin offered");
        let mut session = timeout(LIMIT, connection.accept_session(incoming))
            .await
            .expect("accept session deadline")
            .expect("session accepted");
        assert!(matches!(
            control(&mut peer).await,
            (CHANNEL, Performative::Begin(_))
        ));
        let sender = attach(&mut session, &mut peer).await;
        Self {
            connection,
            session,
            sender,
            peer,
            dropped,
        }
    }

    async fn fill(&mut self) -> Vec<DeliveryConfirmation> {
        let mut held = Vec::with_capacity(CAPACITY);
        for marker in 0..CAPACITY as u32 {
            let pending = sent(&self.sender, &mut self.peer, marker).await;
            held.push(retain(pending, &mut self.peer).await);
        }
        assert_eq!(held.len(), CAPACITY);
        held
    }

    async fn shutdown(&mut self) {
        timeout(LIMIT, self.connection.shutdown())
            .await
            .expect("joined stop deadline")
            .expect("both original tasks joined");
        assert!(
            self.dropped.load(Ordering::SeqCst),
            "owned IO drops before joined stop returns"
        );
    }
}

async fn transfer(peer: &mut DuplexStream, marker: u32) -> u32 {
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Transfer(transfer)),
        payload,
    } = timeout(LIMIT, read_frame(peer))
        .await
        .expect("Transfer deadline")
        .expect("valid Transfer")
    else {
        panic!("expected the actual message Transfer")
    };
    assert_eq!(channel, CHANNEL);
    assert_eq!(transfer.handle, HANDLE);
    assert_eq!(transfer.delivery_tag, Some(tag(marker)));
    assert_eq!(transfer.settled, Some(false));
    assert!(!transfer.more);
    assert_eq!(
        decode_message(&payload).expect("message decodes"),
        message(marker)
    );
    transfer.delivery_id.expect("Transfer identity")
}

async fn sent(sender: &Sender, peer: &mut DuplexStream, marker: u32) -> PendingDelivery {
    let (pending, identity) = timeout(LIMIT, async {
        tokio::join!(
            sender.send_pending(message(marker), tag(marker)),
            transfer(peer, marker)
        )
    })
    .await
    .expect("send and wire Transfer deadline");
    let pending = pending.expect("native send starts");
    assert_eq!(pending.identity().delivery_id(), identity);
    pending
}

async fn retain(pending: PendingDelivery, peer: &mut DuplexStream) -> DeliveryConfirmation {
    let identity = pending.identity();
    write_frame(
        peer,
        &frame(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: identity.delivery_id(),
                last: None,
                settled: false,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
        ),
    )
    .await
    .expect("unsettled Second-mode outcome");
    let outcome = timeout(LIMIT, pending)
        .await
        .expect("outcome deadline")
        .expect("remote Accepted");
    assert!(outcome.needs_confirmation());
    let (returned, _, confirmation) = outcome.into_parts();
    assert_eq!(returned, identity);
    let confirmation = confirmation.expect("external confirmation owns the capacity permit");
    assert_eq!(confirmation.identity(), identity);
    confirmation
}

async fn assert_capacity_waiting<F>(
    sender: &Sender,
    peer: &mut DuplexStream,
    mut admission: Pin<&mut F>,
) where
    F: Future<Output = Result<PendingDelivery, EngineError>>,
{
    assert!(
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(admission.as_mut().poll(cx))))
            .await
            .is_pending(),
        "256 external confirmations must block capacity, not just command start"
    );
    // This response proves that any Send submitted by the earlier poll ran first.
    let reservation = timeout(LIMIT, sender.on_credit())
        .await
        .expect("positive command FIFO barrier deadline")
        .expect("spare remote credit is reservable");
    let mut byte = [0; 1];
    let mut buffer = ReadBuf::new(&mut byte);
    let read = tokio::task::unconstrained(poll_fn(|cx| {
        Poll::Ready(Pin::new(&mut *peer).poll_read(cx, &mut buffer))
    }))
    .await;
    assert!(
        read.is_pending(),
        "no earlier Send may have written an unexpected Transfer"
    );
    assert!(buffer.filled().is_empty());
    timeout(LIMIT, reservation.release())
        .await
        .expect("reservation release deadline")
        .expect("actual release response restores the spare credit");
}

#[tokio::test]
async fn saturated_sender_wakes_on_detach_and_replacement_has_independent_capacity() {
    let mut fixture = Fixture::new().await;
    let held = fixture.fill().await;
    let mut waiting = Box::pin(fixture.sender.send_pending(message(256), tag(256)));
    assert_capacity_waiting(&fixture.sender, &mut fixture.peer, waiting.as_mut()).await;
    write_frame(
        &mut fixture.peer,
        &frame(
            CHANNEL,
            Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }),
        ),
    )
    .await
    .expect("peer Detach");
    let (channel, response) = control(&mut fixture.peer).await;
    assert!(matches!(
        response,
        Performative::Detach(Detach {
            handle: HANDLE,
            closed: true,
            ..
        })
    ));
    assert_eq!(channel, CHANNEL);
    assert!(matches!(
        timeout(LIMIT, waiting)
            .await
            .expect("capacity waiter must wake on Detach"),
        Err(EngineError::RemoteDetached)
    ));
    assert_eq!(
        held.len(),
        CAPACITY,
        "old confirmations were not released to wake the waiter"
    );

    let replacement = attach(&mut fixture.session, &mut fixture.peer).await;
    let pending = sent(&replacement, &mut fixture.peer, 1000).await;
    let replacement_confirmation = retain(pending, &mut fixture.peer).await;
    assert!(matches!(
        timeout(LIMIT, fixture.sender.send_pending(message(257), tag(257)))
            .await
            .expect("old sender remains detached"),
        Err(EngineError::RemoteDetached)
    ));
    fixture.shutdown().await;
    assert_eq!(held.len(), CAPACITY);
    assert_eq!(replacement_confirmation.identity().handle(), HANDLE);
    drop(replacement_confirmation);
    drop(held);
}

#[tokio::test]
async fn saturated_sender_wakes_on_peer_end_without_releasing_external_confirmations() {
    let mut fixture = Fixture::new().await;
    let held = fixture.fill().await;
    let mut waiting = Box::pin(fixture.sender.send_pending(message(256), tag(256)));
    assert_capacity_waiting(&fixture.sender, &mut fixture.peer, waiting.as_mut()).await;
    write_frame(
        &mut fixture.peer,
        &frame(CHANNEL, Performative::End(End::default())),
    )
    .await
    .expect("peer End");
    assert!(matches!(
        control(&mut fixture.peer).await,
        (CHANNEL, Performative::End(_))
    ));
    assert!(matches!(
        timeout(LIMIT, waiting)
            .await
            .expect("capacity waiter must wake on End"),
        Err(EngineError::RemoteDetached)
    ));
    fixture.shutdown().await;
    assert_eq!(held.len(), CAPACITY);
    drop(held);
}

#[tokio::test]
async fn saturated_sender_wakes_after_joined_stop_while_confirmations_remain_owned() {
    let mut fixture = Fixture::new().await;
    let held = fixture.fill().await;
    let mut waiting = Box::pin(fixture.sender.send_pending(message(256), tag(256)));
    assert_capacity_waiting(&fixture.sender, &mut fixture.peer, waiting.as_mut()).await;
    fixture.connection.stop();
    timeout(LIMIT, fixture.connection.shutdown())
        .await
        .expect("joined stop deadline")
        .expect("both original tasks joined");
    assert!(fixture.dropped.load(Ordering::SeqCst));
    assert_eq!(
        held.len(),
        CAPACITY,
        "joining does not reclaim external confirmation permits"
    );
    assert!(matches!(
        timeout(LIMIT, waiting)
            .await
            .expect("capacity waiter must wake after joined stop"),
        Err(EngineError::RemoteDetached)
    ));
    fixture.shutdown().await;
    assert_eq!(held.len(), CAPACITY);
    drop(held);
}

#[tokio::test]
async fn confirming_one_delivery_admits_one_waiter_and_preserves_the_capacity_bound() {
    let mut fixture = Fixture::new().await;
    let mut held = fixture.fill().await;
    let mut waiting = Box::pin(fixture.sender.send_pending(message(256), tag(256)));
    assert_capacity_waiting(&fixture.sender, &mut fixture.peer, waiting.as_mut()).await;
    let confirmation = held.pop().expect("one real confirmation");
    let released = confirmation.identity();
    let (confirmed, (channel, response)) = timeout(LIMIT, async {
        tokio::join!(
            confirmation.confirm(DeliveryState::Accepted(Accepted)),
            control(&mut fixture.peer)
        )
    })
    .await
    .expect("confirmation and positive wire settlement deadline");
    confirmed.expect("healthy confirmation succeeds");
    assert_eq!(channel, CHANNEL);
    let Performative::Disposition(response) = response else {
        panic!("server must confirm disposition")
    };
    assert_eq!(response.role, Role::Sender);
    assert_eq!(response.first, released.delivery_id());
    assert!(response.settled);
    assert_eq!(response.state, Some(DeliveryState::Accepted(Accepted)));

    let (pending, transferred) = timeout(LIMIT, async {
        tokio::join!(waiting.as_mut(), transfer(&mut fixture.peer, 256))
    })
    .await
    .expect("released permit admits the waiting actual Transfer");
    let pending = pending.expect("waiting send starts");
    assert_eq!(pending.identity().delivery_id(), transferred);
    drop(waiting);
    held.push(retain(pending, &mut fixture.peer).await);
    assert_eq!(held.len(), CAPACITY);
    let mut next = Box::pin(fixture.sender.send_pending(message(257), tag(257)));
    assert_capacity_waiting(&fixture.sender, &mut fixture.peer, next.as_mut()).await;
    fixture.connection.stop();
    timeout(LIMIT, fixture.connection.shutdown())
        .await
        .expect("joined stop deadline")
        .expect("both original tasks joined");
    assert!(matches!(
        timeout(LIMIT, next).await.expect("last waiter cleanup"),
        Err(EngineError::RemoteDetached)
    ));
    fixture.shutdown().await;
    assert_eq!(held.len(), CAPACITY);
    drop(held);
}
