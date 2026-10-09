//! Shared real-wire/store witnesses for the actual session-family controls.
//! ReleaseBarrier pauses a qualified proxy frontier before ActualBroker delegation.

use super::super::*;
use crate::SharedAccessAuthentication;
use crate::authorization::ConnectionAuthorization;
use crate::listener::control_attachment_custody::RoutePacket;
use crate::listener::session_custody::{AdoptionObserver, LeafKind, SessionCustody};
use amqp::{
    Body, DeliveryState, MessageId, Properties, Target, Transfer, decode_message, encode_message,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use serde_amqp::{
    Value as AmqpValue,
    primitives::{Binary, Symbol},
};
use std::{panic::panic_any, sync::atomic::AtomicUsize};

pub(super) const FAMILY_CHANNEL: u16 = 33;
pub(super) const REPLIES: &str = "family-replies";

#[derive(Clone, Copy, Debug)]
pub(super) enum Admission {
    DataSend,
    DataReceive,
    CbsRequest,
    CbsReply,
    ManagementRequest,
    ManagementReply,
}

pub(super) const ADMISSIONS: [Admission; 6] = [
    Admission::DataSend,
    Admission::DataReceive,
    Admission::CbsRequest,
    Admission::CbsReply,
    Admission::ManagementRequest,
    Admission::ManagementReply,
];

impl Admission {
    pub(super) fn kind(self) -> LeafKind {
        match self {
            Self::DataSend => LeafKind::DataSend,
            Self::DataReceive => LeafKind::DataReceive,
            Self::CbsRequest => LeafKind::CbsRequest,
            Self::CbsReply => LeafKind::CbsReply,
            Self::ManagementRequest => LeafKind::ManagementRequest,
            Self::ManagementReply => LeafKind::ManagementReply,
        }
    }
    pub(super) fn sending(self) -> bool {
        matches!(
            self,
            Self::DataSend | Self::CbsRequest | Self::ManagementRequest
        )
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::DataSend => "family-data-send",
            Self::DataReceive => "family-data-receive",
            Self::CbsRequest => "family-cbs-request",
            Self::CbsReply => "family-cbs-reply",
            Self::ManagementRequest => "family-management-request",
            Self::ManagementReply => "family-management-reply",
        }
    }

    pub(super) fn attach(
        self,
        handle: u32,
        mode: ReceiverSettleMode,
        session: Option<&str>,
    ) -> Attach {
        let service = match self {
            Self::DataSend | Self::DataReceive => "orders",
            Self::CbsRequest | Self::CbsReply => crate::CBS_NODE,
            Self::ManagementRequest | Self::ManagementReply => "orders/$management",
        };
        let mut source = (!self.sending()).then(|| Source::new(service));
        if let Some(session) = session {
            source
                .as_mut()
                .unwrap()
                .filter
                .get_or_insert_with(Default::default)
                .insert(
                    Symbol::from(crate::SESSION_FILTER),
                    AmqpValue::String(session.to_owned()),
                );
        }
        Attach {
            name: self.name().to_owned(),
            handle,
            role: if self.sending() {
                Role::Sender
            } else {
                Role::Receiver
            },
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: mode,
            source,
            target: Some(Target::new(if self.sending() { service } else { REPLIES })),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: self.sending().then_some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }
}

pub(super) fn family_frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

pub(super) fn authorization() -> Arc<ConnectionAuthorization> {
    let rule = SharedAccessRule::new(
        "family",
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::MANAGE,
    )
    .unwrap();
    let policy = SharedAccessPolicy::new([rule]).unwrap();
    let grant = policy.authenticate_plain("family", "secret").unwrap();
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net").unwrap(),
        Some(grant),
    )
}

pub(super) struct AdmissionWire {
    pub(super) connection: ServerConnection,
    pub(super) peer: DuplexStream,
    pub(super) writes: Arc<WriteGate>,
}

impl AdmissionWire {
    pub(super) async fn new() -> (Self, ServerSession) {
        let (stream, mut peer) = duplex(64 * 1024);
        let writes = Arc::new(WriteGate::default());
        let stream = GatedIo {
            inner: stream,
            gate: Arc::clone(&writes),
        };
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "family-server", None),
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
                        &family_frame(0, Performative::Open(Open::new("family-peer"))),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(
                        performative(&mut peer).await,
                        Performative::Open(_)
                    ));
                }
            )
        })
        .await
        .unwrap();
        let mut wire = Self {
            connection: connection.unwrap(),
            peer,
            writes,
        };
        let session = wire.begin(FAMILY_CHANNEL).await;
        (wire, session)
    }

    pub(super) async fn begin(&mut self, channel: u16) -> ServerSession {
        write_frame(
            &mut self.peer,
            &family_frame(channel, Performative::Begin(Begin::default())),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let session = timeout(WAIT, self.connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            self.control(channel).await,
            Performative::Begin(_)
        ));
        session
    }

    pub(super) async fn control(&mut self, channel: u16) -> Performative {
        let Frame::Amqp {
            channel: actual,
            performative: Some(control),
            payload,
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("actual control frame")
        };
        assert_eq!(actual, channel);
        assert!(payload.is_empty());
        control
    }

    pub(super) async fn offer(
        &mut self,
        role: Admission,
        handle: u32,
        mode: ReceiverSettleMode,
        session: Option<&str>,
    ) {
        write_frame(
            &mut self.peer,
            &family_frame(
                FAMILY_CHANNEL,
                Performative::Attach(Box::new(role.attach(handle, mode, session))),
            ),
        )
        .await
        .unwrap();
    }

    pub(super) async fn accepted(&mut self, role: Admission, handle: u32) {
        let Performative::Attach(attach) = self.control(FAMILY_CHANNEL).await else {
            panic!("actual Attach answer")
        };
        assert_eq!(attach.name, role.name());
        assert_eq!(attach.handle, handle);
        assert_eq!(
            attach.role,
            if role.sending() {
                Role::Receiver
            } else {
                Role::Sender
            }
        );
        if role.sending() {
            let Performative::Flow(flow) = self.control(FAMILY_CHANNEL).await else {
                panic!("actual receiver credit")
            };
            assert_eq!(flow.handle, Some(handle));
            assert!(flow.link_credit.unwrap() > 0);
        }
    }

    pub(super) async fn credit(&mut self, handle: u32) {
        write_frame(
            &mut self.peer,
            &family_frame(
                FAMILY_CHANNEL,
                Performative::Flow(Flow {
                    handle: Some(handle),
                    delivery_count: Some(0),
                    link_credit: Some(8),
                    incoming_window: 2048,
                    outgoing_window: 2048,
                    ..Flow::default()
                }),
            ),
        )
        .await
        .unwrap();
    }

    pub(super) async fn request(&mut self, handle: u32, id: u64, message: Message, format: u32) {
        write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel: FAMILY_CHANNEL,
                performative: Some(Performative::Transfer(Transfer {
                    handle,
                    delivery_id: Some(id as u32),
                    delivery_tag: Some(Binary::from(id.to_be_bytes().to_vec())),
                    message_format: Some(format),
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

    pub(super) async fn transfer(&mut self, handle: u32) -> (Transfer, Message) {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(transfer)),
            payload,
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("actual child Transfer")
        };
        assert_eq!(channel, FAMILY_CHANNEL);
        assert_eq!(transfer.handle, handle);
        (transfer, decode_message(&payload).unwrap())
    }

    pub(super) async fn outcome(&mut self, id: u32, mode: ReceiverSettleMode) {
        write_frame(
            &mut self.peer,
            &family_frame(
                FAMILY_CHANNEL,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: id,
                    last: None,
                    settled: mode == ReceiverSettleMode::First,
                    state: Some(DeliveryState::Accepted(Accepted)),
                    batchable: false,
                }),
            ),
        )
        .await
        .unwrap();
    }

    pub(super) async fn detach(&mut self, handle: u32) {
        write_frame(
            &mut self.peer,
            &family_frame(
                FAMILY_CHANNEL,
                Performative::Detach(Detach {
                    handle,
                    closed: true,
                    error: None,
                }),
            ),
        )
        .await
        .unwrap();
        let Performative::Detach(detach) = self.control(FAMILY_CHANNEL).await else {
            panic!("actual Detach answer")
        };
        assert_eq!(detach.handle, handle);
    }

    pub(super) async fn end(&mut self) {
        write_frame(
            &mut self.peer,
            &family_frame(FAMILY_CHANNEL, Performative::End(End::default())),
        )
        .await
        .unwrap();
    }

    pub(super) async fn stop(&mut self) {
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

pub(super) fn data_message(id: u64, body: &[u8]) -> Message {
    Message {
        properties: Some(Properties {
            message_id: Some(MessageId::Ulong(id)),
            ..Properties::default()
        }),
        body: Body::Data(vec![Binary::from(body.to_vec())]),
        ..Message::default()
    }
}

pub(super) async fn adopted(observer: &AdoptionObserver, count: usize) {
    timeout(WAIT, async {
        loop {
            let notified = observer.reached.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if observer.records.lock().unwrap().len() >= count {
                return;
            }
            notified.await;
        }
    })
    .await
    .expect("actual session adopted original receipt");
}

pub(super) async fn drive<F: Future, T>(
    mut original: Pin<&mut F>,
    stage: impl Future<Output = T>,
) -> T {
    tokio::select! {
        biased;
        result = stage => result,
        _ = original.as_mut() => panic!("positive stage precedes original session completion"),
    }
}

pub(super) async fn partial_finish(custody: &mut SessionCustody) {
    timeout(WAIT, async {
        loop {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
            drop(finish);
            if !custody.family().finished().is_empty() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unblocked original positively joined while sibling remains held");
}

pub(super) async fn returned(actor: &Actor, matches: impl Fn(&CommandKind) -> bool) {
    timeout(WAIT, async {
        loop {
            if actor
                .log
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry.returned && matches(&entry.kind))
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original actual broker result returned");
}

pub(super) fn invocations(actor: &Actor, matches: impl Fn(&CommandKind) -> bool) -> usize {
    actor
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| !entry.returned && matches(&entry.kind))
        .count()
}

pub(super) fn request_message(id: u64) -> Message {
    Message {
        properties: Some(Properties {
            message_id: Some(MessageId::Ulong(id)),
            reply_to: Some(REPLIES.to_owned()),
            ..Properties::default()
        }),
        ..Message::default()
    }
}

pub(super) async fn accepted_request(wire: &mut AdmissionWire, id: u32) {
    let Performative::Disposition(disposition) = wire.control(FAMILY_CHANNEL).await else {
        panic!("actual request acceptance")
    };
    assert_eq!((disposition.role, disposition.first), (Role::Receiver, id));
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
}

pub(super) async fn reply(
    wire: &mut AdmissionWire,
    handle: u32,
    correlation: u64,
    cbs: bool,
    mode: ReceiverSettleMode,
) {
    let (transfer, message) = wire.transfer(handle).await;
    assert_eq!(
        message.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(correlation))
    );
    let key = if cbs {
        "status-code"
    } else {
        crate::management::STATUS_CODE_PROPERTY
    };
    assert_eq!(
        message.application_properties.unwrap().get(key),
        Some(&AmqpValue::Int(400))
    );
    let id = transfer.delivery_id.unwrap();
    assert_ne!(transfer.settled, Some(true));
    wire.outcome(id, mode.clone()).await;
    if mode == ReceiverSettleMode::Second {
        let Performative::Disposition(disposition) = wire.control(FAMILY_CHANNEL).await else {
            panic!("actual reply confirmation")
        };
        assert_eq!((disposition.role, disposition.first), (Role::Sender, id));
        assert!(disposition.settled);
        assert!(matches!(
            disposition.state,
            Some(DeliveryState::Accepted(_))
        ));
    }
}

pub(super) async fn replacement_route(
    cbs: bool,
    authorization: &Arc<ConnectionAuthorization>,
    management: &Arc<ConnectionManagement>,
) -> RoutePacket {
    if cbs {
        let (route, responses) = authorization.register_reply_route(REPLIES.to_owned()).await;
        RoutePacket::Cbs {
            registry: Arc::clone(authorization),
            address: REPLIES.to_owned(),
            route,
            responses,
        }
    } else {
        let (route, responses) = management.register_reply_route(REPLIES.to_owned()).await;
        RoutePacket::Management {
            registry: Arc::clone(management),
            address: REPLIES.to_owned(),
            route,
            responses,
        }
    }
}

pub(super) async fn received_replacement(packet: &mut RoutePacket) {
    // CBS response constructors stay private; the empty queue plus real request
    // proves delivery to this exact replacement without fabricating a packet.
    match packet {
        RoutePacket::Cbs {
            route, responses, ..
        } => {
            assert!(!route.is_closed());
            assert!(timeout(WAIT, responses.recv()).await.unwrap().is_some());
            assert!(matches!(
                responses.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
        }
        RoutePacket::Management {
            route, responses, ..
        } => {
            assert!(!route.is_closed());
            assert!(timeout(WAIT, responses.recv()).await.unwrap().is_some());
            assert!(matches!(
                responses.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
        }
    }
}

pub(super) async fn remove_replacement(packet: RoutePacket) {
    match packet {
        RoutePacket::Cbs {
            registry,
            address,
            route,
            mut responses,
        } => {
            responses.close();
            registry.unregister_reply_route(&address, &route).await;
        }
        RoutePacket::Management {
            registry,
            address,
            route,
            mut responses,
        } => {
            responses.close();
            registry.unregister_reply_route(&address, &route).await;
        }
    }
}

#[derive(Default)]
pub(super) struct ReleaseBarrier {
    calls: Mutex<Vec<Id>>,
    entered: Notify,
    release: Notify,
}

impl ReleaseBarrier {
    pub(super) async fn reached(&self) {
        timeout(WAIT, async {
            loop {
                let notified = self.entered.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !self.calls.lock().unwrap().is_empty() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("same lazy ReleaseSession proxy original positively entered");
    }

    pub(super) fn calls(&self) -> Vec<Id> {
        self.calls.lock().unwrap().clone()
    }
    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Clone)]
pub(super) struct ReleaseGateBroker {
    actual: ActualBroker,
    pub(super) barrier: Arc<ReleaseBarrier>,
}

impl ReleaseGateBroker {
    pub(super) fn new(actual: ActualBroker) -> Self {
        Self {
            actual,
            barrier: Arc::new(ReleaseBarrier::default()),
        }
    }
}

impl Broker for ReleaseGateBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if matches!(&kind, CommandKind::ReleaseSession { .. }) {
            // This witness is a proxy first-poll frontier, not a store apply.
            self.barrier.calls.lock().unwrap().push(tokio::task::id());
            self.barrier.entered.notify_waiters();
            self.barrier.release.notified().await;
        }
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

pub(super) struct AttachmentReportFault {
    pub(super) reached: Arc<AtomicUsize>,
    pub(super) payload: Arc<str>,
}

impl tracing::Subscriber for AttachmentReportFault {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata
            .target()
            .ends_with("::listener::attachments::custody")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.reached.fetch_add(1, Ordering::SeqCst);
        panic_any(Arc::clone(&self.payload));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

pub(super) async fn independent_session_usable(
    wire: &mut AdmissionWire,
    mut session: ServerSession,
    channel: u16,
) {
    assert!(!session.is_ended());
    let mut ended = Box::pin(session.on_end_owned());
    write_frame(
        &mut wire.peer,
        &family_frame(
            channel,
            Performative::Attach(Box::new(Admission::DataSend.attach(
                1,
                ReceiverSettleMode::First,
                None,
            ))),
        ),
    )
    .await
    .unwrap();
    let incoming = timeout(WAIT, session.next_incoming_attach())
        .await
        .unwrap()
        .unwrap();
    let LinkEndpoint::Receiver(mut receiver) =
        timeout(WAIT, session.accept_attach(incoming, 1024 * 1024))
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("independent native session accepts original endpoint")
    };
    assert!(matches!(
        wire.control(channel).await,
        Performative::Attach(_)
    ));
    let Performative::Flow(flow) = wire.control(channel).await else {
        panic!("independent receiver credit")
    };
    assert!(flow.link_credit.unwrap() > 0);
    let message = data_message(44, b"independent-session-live");
    write_frame(
        &mut wire.peer,
        &Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(Transfer {
                handle: 1,
                delivery_id: Some(0),
                delivery_tag: Some(Binary::from(vec![44])),
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
    let delivery = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
    assert_eq!(delivery.message(), &message);
    timeout(WAIT, receiver.accept(&delivery))
        .await
        .unwrap()
        .unwrap();
    let Performative::Disposition(disposition) = wire.control(channel).await else {
        panic!("independent native acceptance")
    };
    assert_eq!((disposition.role, disposition.first), (Role::Receiver, 0));
    assert!(disposition.settled && matches!(disposition.state, Some(DeliveryState::Accepted(_))));
    write_frame(
        &mut wire.peer,
        &family_frame(channel, Performative::End(End::default())),
    )
    .await
    .unwrap();
    timeout(WAIT, ended.as_mut()).await.unwrap();
    assert!(matches!(wire.control(channel).await, Performative::End(_)));
}
