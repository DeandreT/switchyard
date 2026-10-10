//! OPEN-only wire fixture: every Begin is admitted by the connection pump.

use super::*;
use crate::listener::connection_session_family::{FamilyFacts, FamilyObserver, FamilyPoint};
use amqp::{Body, MessageId, Properties, Target, Transfer, encode_message};
use std::panic::panic_any;
use std::sync::atomic::AtomicUsize;

pub(super) struct ConnectionWire {
    pub(super) peer: DuplexStream,
    pub(super) writes: Arc<WriteGate>,
}

pub(super) fn connection_frame(channel: u16, control: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(control),
        payload: Vec::new(),
    }
}
impl ConnectionWire {
    pub(super) async fn open() -> (Self, ServerConnection) {
        let (inner, mut peer) = duplex(64 * 1024);
        let writes = Arc::new(WriteGate::default());
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(
                    GatedIo {
                        inner,
                        gate: Arc::clone(&writes)
                    },
                    "connection-family",
                    None
                ),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut peer).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                    write_frame(
                        &mut peer,
                        &connection_frame(
                            0,
                            Performative::Open(Open::new("connection-family-peer")),
                        ),
                    )
                    .await
                    .unwrap();
                    let Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Open(_)),
                        ..
                    } = read_frame(&mut peer).await.unwrap()
                    else {
                        panic!("actual Open answer");
                    };
                }
            )
        })
        .await
        .unwrap();
        (Self { peer, writes }, connection.unwrap())
    }
    pub(super) async fn write(&mut self, channel: u16, control: Performative) {
        write_frame(&mut self.peer, &connection_frame(channel, control))
            .await
            .unwrap();
    }
    pub(super) async fn control(&mut self, expected: u16) -> Performative {
        let Frame::Amqp {
            channel,
            performative: Some(control),
            payload,
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("actual control frame");
        };
        assert_eq!(channel, expected);
        assert!(payload.is_empty());
        control
    }
    pub(super) async fn begin(&mut self, channel: u16) {
        self.write(channel, Performative::Begin(Begin::default()))
            .await;
    }
    pub(super) async fn begin_answer(&mut self, channel: u16) {
        let Performative::Begin(begin) = self.control(channel).await else {
            panic!("actual Begin answer");
        };
        assert_eq!(begin.remote_channel, Some(channel));
    }
    pub(super) async fn buffered_begin(&mut self, channel: u16) {
        self.begin(channel).await;
        // The unrelated answer proves the prior Begin reached the incoming queue.
        self.write(900, Performative::End(End::default())).await;
        assert!(matches!(self.control(900).await, Performative::End(_)));
    }
    pub(super) async fn end(&mut self, channel: u16) {
        self.write(channel, Performative::End(End::default())).await;
    }
    pub(super) async fn end_answer(&mut self, channel: u16) {
        assert!(matches!(self.control(channel).await, Performative::End(_)));
    }
    pub(super) async fn offer(&mut self, channel: u16, handle: u32, invalid: bool) {
        self.write(
            channel,
            Performative::Attach(Box::new(Attach {
                name: format!("connection-{channel}-{handle}"),
                handle,
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: None,
                target: Some(Target::new(if invalid { "" } else { "orders" })),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
        )
        .await;
    }
    pub(super) async fn accepted(&mut self, channel: u16, handle: u32) {
        let Performative::Attach(attach) = self.control(channel).await else {
            panic!("actual Attach answer");
        };
        assert_eq!(attach.name, format!("connection-{channel}-{handle}"));
        assert_eq!(attach.handle, handle);
        assert_eq!(attach.role, Role::Receiver);
        let Performative::Flow(flow) = self.control(channel).await else {
            panic!("role-exact actual receiver credit");
        };
        assert_eq!(flow.handle, Some(handle));
        assert!(flow.link_credit.unwrap() > 0);
    }
    pub(super) async fn refused(&mut self, channel: u16, handle: u32) {
        self.accepted(channel, handle).await;
        let Performative::Detach(detach) = self.control(channel).await else {
            panic!("strict actual refusal Detach");
        };
        assert_eq!(detach.handle, handle);
        assert!(detach.closed && detach.error.is_some());
    }
    pub(super) async fn send(&mut self, channel: u16, handle: u32, marker: u64) {
        let message = Message {
            properties: Some(Properties {
                message_id: Some(MessageId::Ulong(marker)),
                ..Properties::default()
            }),
            body: Body::Data(vec![marker.to_be_bytes().to_vec().into()]),
            ..Message::default()
        };
        write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel,
                performative: Some(Performative::Transfer(Transfer {
                    handle,
                    delivery_id: Some(0),
                    delivery_tag: Some(vec![0].into()),
                    message_format: Some(0),
                    settled: Some(false),
                    more: false,
                    rcv_settle_mode: None,
                    state: None,
                    resume: false,
                    aborted: false,
                    batchable: false,
                })),
                payload: encode_message(&message).unwrap(),
            },
        )
        .await
        .unwrap();
    }
    pub(super) async fn accepted_send(&mut self, channel: u16) {
        let Performative::Disposition(disposition) = self.control(channel).await else {
            panic!("actual Send acknowledgement");
        };
        assert_eq!((disposition.role, disposition.first), (Role::Receiver, 0));
        assert!(disposition.settled);
        assert!(matches!(
            disposition.state,
            Some(amqp::DeliveryState::Accepted(_))
        ));
    }
}

pub(super) async fn drive<F: Future, T>(
    mut pump: Pin<&mut F>,
    stage: impl Future<Output = T>,
) -> T {
    timeout(WAIT, async { tokio::select! { biased; result = stage => result, _ = pump.as_mut() => panic!("positive stage precedes original parent completion"), } }).await.expect("actual parent stage reached")
}
pub(super) async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    assert!(
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))))
            .await
            .is_pending(),
        "same original remains held"
    );
}
pub(super) async fn observed(observer: &FamilyObserver, predicate: impl Fn(&FamilyFacts) -> bool) {
    timeout(WAIT, async {
        loop {
            let changed = observer.reached.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if observer.records.lock().unwrap().iter().any(&predicate) {
                return;
            }
            changed.await;
        }
    })
    .await
    .expect("original connection cache checkpoint positively reached");
}
pub(super) fn facts(observer: &FamilyObserver, point: FamilyPoint, index: usize) -> FamilyFacts {
    *observer
        .records
        .lock()
        .unwrap()
        .iter()
        .filter(|record| record.point == point)
        .nth(index)
        .expect("recorded cache event")
}
pub(super) async fn checkpoint_count(observer: &FamilyObserver, point: FamilyPoint, count: usize) {
    timeout(WAIT, async {
        loop {
            let changed = observer.reached.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if observer
                .records
                .lock()
                .unwrap()
                .iter()
                .filter(|record| record.point == point)
                .count()
                >= count
            {
                return;
            }
            changed.await;
        }
    })
    .await
    .expect("same original cache checkpoints reached");
}
pub(super) async fn adopted(observer: &FamilyObserver, count: usize) {
    checkpoint_count(observer, FamilyPoint::Adopted, count).await;
}
pub(super) async fn reaped(observer: &FamilyObserver, id: Id) {
    observed(observer, |record| {
        record.point == FamilyPoint::LiveReaped && record.id == Some(id)
    })
    .await;
}
pub(super) fn packet_address(
    packet: &crate::listener::connection_session_family::JoinedSessionTask,
) -> usize {
    packet as *const _ as usize
}
pub(super) fn invocations(actor: &Actor) -> usize {
    actor
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| !entry.returned && matches!(entry.kind, CommandKind::Send { .. }))
        .count()
}
pub(super) async fn returned(actor: &Actor) {
    timeout(WAIT, async {
        loop {
            if actor
                .log
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry.returned && matches!(entry.kind, CommandKind::Send { .. }))
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("same actual Send returned");
}

pub(super) struct CloneFaultBroker {
    pub(super) actual: ActualBroker,
    pub(super) armed: Arc<AtomicBool>,
    pub(super) calls: Arc<AtomicUsize>,
    pub(super) payload: Arc<str>,
}
impl Clone for CloneFaultBroker {
    fn clone(&self) -> Self {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.armed.swap(false, Ordering::SeqCst) {
            panic_any(Arc::clone(&self.payload));
        }
        Self {
            actual: self.actual.clone(),
            armed: Arc::clone(&self.armed),
            calls: Arc::clone(&self.calls),
            payload: Arc::clone(&self.payload),
        }
    }
}
impl Broker for CloneFaultBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.actual.submit(namespace, entity, kind).await
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

pub(super) struct ReportFault {
    pub(super) attachment_reached: Arc<AtomicUsize>,
    pub(super) family_reached: Arc<AtomicUsize>,
    pub(super) attachment: Arc<str>,
    pub(super) family: Arc<str>,
}
impl tracing::Subscriber for ReportFault {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata
            .target()
            .ends_with("::listener::attachments::custody")
            || metadata.target().ends_with("::connection_session_family")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event
            .metadata()
            .target()
            .ends_with("::listener::attachments::custody")
        {
            self.attachment_reached.fetch_add(1, Ordering::SeqCst);
            panic_any(Arc::clone(&self.attachment));
        }
        self.family_reached.fetch_add(1, Ordering::SeqCst);
        panic_any(Arc::clone(&self.family));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
