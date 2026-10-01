use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use super::*;
use crate::{Modified, Rejected, Released};

const CHANNEL: u16 = 3;
const HANDLE: u32 = 7;
const ID: u32 = 11;

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    blocked_flush: AtomicBool,
    failed_flush: AtomicBool,
    waiter: Mutex<Option<Waker>>,
}

impl Output {
    fn assert_silent(&self) {
        assert!(self.bytes.lock().expect("captured output").is_empty());
    }

    fn release_flush(&self) {
        self.blocked_flush.store(false, Ordering::Release);
        if let Some(waiter) = self.waiter.lock().expect("flush waiter").take() {
            waiter.wake();
        }
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = self.bytes.lock().expect("captured output").clone();
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            frames.push(read_frame(&mut input).await.expect("complete frame"));
        }
        frames
    }
}

struct Writer(Arc<Output>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .bytes
            .lock()
            .expect("captured output")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.failed_flush.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected final Transfer flush failure",
            )));
        }
        if self.0.blocked_flush.load(Ordering::Acquire) {
            *self.0.waiter.lock().expect("flush waiter") = Some(context.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn sending(
    identity: LinkIdentity,
    default_outcome: Option<Outcome>,
    automatic: bool,
) -> SendingLink {
    let (detached, _) = watch::channel(false);
    SendingLink {
        identity,
        auto_acknowledge: automatic,
        default_outcome,
        outstanding_tags: HashSet::new(),
        max_message_size: None,
        receiver_settle_mode: ReceiverSettleMode::Second,
        settle_mode: SenderSettleMode::Unsettled,
        credit: LinkCredit::new(0),
        queued: VecDeque::new(),
        active: None,
        unsettled: HashMap::new(),
        pending_acknowledgements: HashMap::new(),
        detached,
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    output: Arc<Output>,
    writer: FrameWriter<Writer>,
}

impl Fixture {
    fn new(default_outcome: Option<Outcome>, automatic: bool) -> Self {
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        session.links.insert(
            HANDLE,
            LinkState::Sending(Box::new(sending(
                LinkIdentity::new(),
                default_outcome,
                automatic,
            ))),
        );
        let output = Arc::new(Output::default());
        Self {
            sessions: HashMap::from([(CHANNEL, session)]),
            writer: FrameWriter::new(Writer(output.clone()), 512).expect("frame writer"),
            output,
        }
    }

    fn link(&self) -> &SendingLink {
        sending_ref(&self.sessions[&CHANNEL], HANDLE)
    }

    fn link_mut(&mut self) -> &mut SendingLink {
        sending_mut(self.sessions.get_mut(&CHANNEL).expect("session"), HANDLE)
    }

    fn delivery(
        &mut self,
        id: u32,
        active: bool,
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let (reply, result) = oneshot::channel();
        self.link_mut()
            .outstanding_tags
            .insert(id.to_be_bytes().to_vec());
        assert!(
            self.link_mut()
                .unsettled
                .insert(
                    id,
                    OutgoingDelivery {
                        reply,
                        delivery_tag: id.to_be_bytes().to_vec().into(),
                        outcome: None,
                        receiver_settled: false,
                    }
                )
                .is_none()
        );
        if active {
            let content_lease = self
                .writer
                .content_budget()
                .try_reserve(2)
                .expect("active content");
            self.link_mut().active = Some(ActiveSend {
                payload: vec![1, 2],
                content_lease,
                offset: 1,
                first_frame_sent: true,
                delivery_id: id,
                delivery_tag: id.to_be_bytes().to_vec().into(),
                message_format: 0,
                settled: false,
                settled_reply: None,
            });
        }
        result
    }

    async fn update(
        &mut self,
        first: u32,
        last: Option<u32>,
        settled: bool,
        state: Option<DeliveryState>,
    ) {
        apply_disposition(
            CHANNEL,
            Disposition {
                role: Role::Receiver,
                first,
                last,
                settled,
                state,
                batchable: false,
            },
            &mut self.writer,
            &mut self.sessions,
        )
        .await
        .expect("receiver update does not stop the connection");
    }

    async fn finish(&mut self) {
        send_fragment(
            CHANNEL,
            HANDLE,
            self.sessions.get_mut(&CHANNEL).expect("session"),
            &mut self.writer,
        )
        .await
        .expect("final outgoing Transfer");
    }

    async fn settle(
        &mut self,
        owner: &LinkIdentity,
        identity: &AckIdentity,
        state: DeliveryState,
    ) -> Result<(), EngineError> {
        let (reply, result) = oneshot::channel();
        settle_outgoing(
            CHANNEL,
            HANDLE,
            owner.clone(),
            Some(identity.clone()),
            state,
            reply,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("local settlement leaves the connection usable");
        result.await.expect("settlement result")
    }
}

fn sending_ref(session: &SessionState, handle: u32) -> &SendingLink {
    let LinkState::Sending(link) = &session.links[&handle] else {
        panic!("sending link");
    };
    link
}

fn sending_mut(session: &mut SessionState, handle: u32) -> &mut SendingLink {
    let LinkState::Sending(link) = session.links.get_mut(&handle).expect("sending handle") else {
        panic!("sending link");
    };
    link
}

fn received() -> DeliveryState {
    DeliveryState::Received {
        section_number: 2,
        section_offset: 17,
    }
}

fn rejected() -> Rejected {
    Rejected {
        error: Some(Error::new(
            crate::AmqpError::InternalError,
            "actual rejection",
            None,
        )),
    }
}

fn assert_waiting(result: &mut oneshot::Receiver<Result<SendOutcome, EngineError>>) {
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
}

fn assert_final_only(frames: &[Frame]) {
    assert!(matches!(frames, [Frame::Amqp {
        channel: CHANNEL, performative: Some(Performative::Transfer(transfer)), payload,
    }] if transfer.handle == HANDLE && !transfer.more && transfer.delivery_id.is_none() && payload == &[2]));
}

#[tokio::test]
async fn later_remote_settlement_preserves_first_terminal_outcome_and_suppresses_obsolete_ack() {
    for automatic in [false, true] {
        for later_state in [
            None,
            Some(received()),
            Some(DeliveryState::Accepted(Accepted)),
        ] {
            let mut fixture = Fixture::new(Some(Outcome::Released(Released)), automatic);
            let mut result = fixture.delivery(ID, true);
            fixture
                .update(ID, None, false, Some(DeliveryState::Rejected(rejected())))
                .await;
            assert_waiting(&mut result);
            fixture.update(ID, None, true, later_state).await;
            fixture.output.assert_silent();
            assert_waiting(&mut result);
            let pending = &fixture.link().unsettled[&ID];
            assert_eq!(pending.outcome, Some(Outcome::Rejected(rejected())));
            assert!(pending.receiver_settled);
            assert!(fixture.link().pending_acknowledgements.is_empty());
            fixture.finish().await;
            let sent = result
                .await
                .expect("send result")
                .expect("known terminal outcome");
            assert_eq!(sent.outcome, Outcome::Rejected(rejected()));
            assert!(sent.acknowledgement.is_none());
            assert_final_only(&fixture.output.frames().await);
            assert!(fixture.link().unsettled.is_empty());
        }
    }
}

#[tokio::test]
async fn remote_settlement_without_terminal_uses_actual_default_or_an_explicit_no_outcome_error() {
    for default in [
        Some(Outcome::Released(Released)),
        Some(Outcome::Rejected(rejected())),
        None,
    ] {
        for state in [None, Some(received())] {
            for active in [false, true] {
                let mut fixture = Fixture::new(default.clone(), true);
                let mut result = fixture.delivery(ID, active);
                fixture.update(ID, None, true, state.clone()).await;
                fixture.output.assert_silent();
                if active {
                    assert_waiting(&mut result);
                    assert!(fixture.link().unsettled[&ID].receiver_settled);
                    assert!(fixture.link().unsettled[&ID].outcome.is_none());
                    fixture.finish().await;
                    assert_final_only(&fixture.output.frames().await);
                }
                let sent = result.await.expect("local send result");
                match &default {
                    Some(expected) => {
                        let sent = sent.expect("the actual source default is available");
                        assert_eq!(&sent.outcome, expected);
                        assert!(sent.acknowledgement.is_none());
                    }
                    None => assert!(matches!(
                        sent,
                        Err(EngineError::RemoteSettledWithoutOutcome)
                    )),
                }
                assert!(fixture.link().unsettled.is_empty());
                assert!(fixture.link().pending_acknowledgements.is_empty());
                assert!(!fixture.link().identity.is_retired());
            }
        }
    }
}

#[tokio::test]
async fn terminal_observed_after_early_settlement_overrides_default_before_final_transfer() {
    for default in [Some(Outcome::Released(Released)), None] {
        for state in [None, Some(received())] {
            let mut fixture = Fixture::new(default.clone(), true);
            let mut result = fixture.delivery(ID, true);
            fixture.update(ID, None, true, state.clone()).await;
            assert_waiting(&mut result);
            assert!(fixture.link().unsettled[&ID].receiver_settled);
            assert!(fixture.link().unsettled[&ID].outcome.is_none());
            let modified = Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(false),
                message_annotations: None,
            };
            fixture
                .update(
                    ID,
                    None,
                    false,
                    Some(DeliveryState::Modified(modified.clone())),
                )
                .await;
            fixture
                .update(ID, None, false, Some(DeliveryState::Rejected(rejected())))
                .await;
            fixture.output.assert_silent();
            assert_waiting(&mut result);
            let pending = &fixture.link().unsettled[&ID];
            assert!(
                pending.receiver_settled,
                "later false cannot undo settlement"
            );
            assert_eq!(pending.outcome, Some(Outcome::Modified(modified.clone())));
            fixture.finish().await;
            let sent = result
                .await
                .expect("send result")
                .expect("observed terminal outcome");
            assert_eq!(sent.outcome, Outcome::Modified(modified));
            assert!(sent.acknowledgement.is_none());
            assert_final_only(&fixture.output.frames().await);
        }
    }
}

#[tokio::test]
async fn unsettled_nonterminal_updates_do_not_complete_a_send_or_apply_the_source_default() {
    for active in [false, true] {
        let mut fixture = Fixture::new(Some(Outcome::Released(Released)), true);
        let mut result = fixture.delivery(ID, active);
        for state in [None, Some(received())] {
            fixture.update(ID, None, false, state).await;
            assert_waiting(&mut result);
            fixture.output.assert_silent();
            assert!(!fixture.link().unsettled[&ID].receiver_settled);
            assert!(fixture.link().unsettled[&ID].outcome.is_none());
        }
        fixture
            .update(ID, None, false, Some(DeliveryState::Accepted(Accepted)))
            .await;
        if active {
            assert_waiting(&mut result);
            fixture.output.assert_silent();
            fixture.finish().await;
        }
        let sent = result
            .await
            .expect("send reply")
            .expect("explicit terminal outcome");
        assert_eq!(sent.outcome, Outcome::Accepted(Accepted));
        assert!(sent.acknowledgement.is_none());
        let frames = fixture.output.frames().await;
        assert_eq!(frames.len(), 1 + usize::from(active));
        if active {
            assert_final_only(&frames[..1]);
        }
        assert!(
            matches!(frames.last().expect("automatic acknowledgement"), Frame::Amqp {
            channel: CHANNEL,
            performative: Some(Performative::Disposition(disposition)),
            payload,
        } if disposition.role == Role::Sender && disposition.first == ID
            && disposition.last.is_none() && disposition.settled
            && disposition.state.is_none() && payload.is_empty())
        );
    }
}

#[tokio::test]
async fn remote_settlement_retires_a_pending_manual_ack_and_owned_repeats_never_encode_or_write() {
    for state in [
        None,
        Some(received()),
        Some(DeliveryState::Rejected(rejected())),
    ] {
        let mut fixture = Fixture::new(None, false);
        let owner = fixture.link().identity.clone();
        let token = AckIdentity::new(&owner, ID, &[]);
        let clone = token.clone();
        fixture
            .link_mut()
            .pending_acknowledgements
            .insert(ID, token.clone());
        fixture.update(ID, None, true, state).await;
        assert!(token.is_settled());
        assert!(clone.is_settled());
        assert!(fixture.link().pending_acknowledgements.is_empty());
        fixture.output.assert_silent();
        let oversized = DeliveryState::Rejected(Rejected {
            error: Some(Error::new(
                crate::AmqpError::InternalError,
                "x".repeat(1024),
                None,
            )),
        });
        for state in [oversized, DeliveryState::Accepted(Accepted)] {
            fixture
                .settle(&owner, &clone, state)
                .await
                .expect("owned remote-settled receipt is a no-op before encoding");
            fixture.output.assert_silent();
        }
        assert!(matches!(
            fixture
                .settle(
                    &LinkIdentity::new(),
                    &token,
                    DeliveryState::Accepted(Accepted)
                )
                .await,
            Err(EngineError::InvalidState(_))
        ));
        fixture.output.assert_silent();
    }
}

#[tokio::test]
async fn remote_retired_token_cannot_consume_reused_numeric_alias_or_cross_owner_replacement() {
    let mut fixture = Fixture::new(None, false);
    let owner = fixture.link().identity.clone();
    let old = AckIdentity::new(&owner, ID, &[]);
    fixture
        .link_mut()
        .pending_acknowledgements
        .insert(ID, old.clone());
    fixture.update(ID, None, true, None).await;
    assert!(old.is_settled());
    let fresh = AckIdentity::new(&owner, ID, &[]);
    fixture
        .link_mut()
        .pending_acknowledgements
        .insert(ID, fresh.clone());
    fixture
        .settle(&owner, &old, DeliveryState::Accepted(Accepted))
        .await
        .expect("old terminal token is an owned no-op");
    fixture.output.assert_silent();
    assert!(!fresh.is_settled());
    assert!(fixture.link().pending_acknowledgements[&ID].same_ack(&fresh));
    let impostor = AckIdentity::new(&owner, ID, &[]);
    assert!(matches!(
        fixture
            .settle(&owner, &impostor, DeliveryState::Accepted(Accepted))
            .await,
        Err(EngineError::InvalidState(_))
    ));
    fixture.output.assert_silent();
    assert!(fixture.link().pending_acknowledgements[&ID].same_ack(&fresh));

    let mut retired = fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .remove(&HANDLE)
        .expect("original endpoint");
    stop_link(&mut retired);
    let replacement_owner = LinkIdentity::new();
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .insert(
            HANDLE,
            LinkState::Sending(Box::new(sending(replacement_owner.clone(), None, false))),
        );
    let replacement = AckIdentity::new(&replacement_owner, ID, &[]);
    fixture
        .link_mut()
        .pending_acknowledgements
        .insert(ID, replacement.clone());
    assert!(matches!(
        fixture
            .settle(&owner, &old, DeliveryState::Accepted(Accepted))
            .await,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        fixture
            .settle(&replacement_owner, &old, DeliveryState::Accepted(Accepted))
            .await,
        Err(EngineError::InvalidState(_))
    ));
    fixture.output.assert_silent();
    assert!(!replacement.is_settled());
    assert!(fixture.link().pending_acknowledgements[&ID].same_ack(&replacement));
    fixture.update(ID, None, true, None).await;
    assert!(replacement.is_settled());
    assert!(fixture.link().pending_acknowledgements.is_empty());
    fixture.output.assert_silent();
}

#[tokio::test]
async fn remote_ranges_leave_foreign_wrong_key_or_retired_pending_aliases_untouched() {
    for fault in 0..4 {
        let mut fixture = Fixture::new(None, false);
        let owner = fixture.link().identity.clone();
        let token = match fault {
            0 => AckIdentity::new(&LinkIdentity::new(), ID, &[]),
            1 => AckIdentity::new(&owner, ID + 1, &[]),
            2 | 3 => AckIdentity::new(&owner, ID, &[]),
            _ => unreachable!("four invalid alias cases"),
        };
        fixture
            .link_mut()
            .pending_acknowledgements
            .insert(ID, token.clone());
        match fault {
            2 => owner.retire(),
            3 => fixture.sessions[&CHANNEL].identity.retire(),
            _ => {}
        }
        fixture.update(ID, None, true, None).await;
        assert!(!token.is_settled());
        assert!(fixture.link().pending_acknowledgements[&ID].same_ack(&token));
        fixture.output.assert_silent();
    }
}

fn delivery_at(
    session: &mut SessionState,
    handle: u32,
    id: u32,
) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
    let (reply, result) = oneshot::channel();
    sending_mut(session, handle)
        .outstanding_tags
        .insert(id.to_be_bytes().to_vec());
    assert!(
        sending_mut(session, handle)
            .unsettled
            .insert(
                id,
                OutgoingDelivery {
                    reply,
                    delivery_tag: id.to_be_bytes().to_vec().into(),
                    outcome: None,
                    receiver_settled: false
                }
            )
            .is_none()
    );
    result
}

#[tokio::test]
async fn wrapping_and_full_span_ranges_only_scan_owned_outgoing_aliases_in_the_named_session() {
    let mut fixture = Fixture::new(Some(Outcome::Released(Released)), true);
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .insert(
            HANDLE + 1,
            LinkState::Sending(Box::new(sending(
                LinkIdentity::new(),
                Some(Outcome::Released(Released)),
                false,
            ))),
        );
    let mut other = SessionState::new(&Begin::default());
    other.local_begin_sent = true;
    other.links.insert(
        HANDLE,
        LinkState::Sending(Box::new(sending(
            LinkIdentity::new(),
            Some(Outcome::Released(Released)),
            true,
        ))),
    );
    fixture.sessions.insert(CHANNEL + 1, other);

    let mut selected = Vec::new();
    for (handle, id) in [(HANDLE, u32::MAX - 1), (HANDLE + 1, 0)] {
        selected.push(delivery_at(
            fixture.sessions.get_mut(&CHANNEL).expect("session"),
            handle,
            id,
        ));
    }
    let mut outside = delivery_at(
        fixture.sessions.get_mut(&CHANNEL).expect("session"),
        HANDLE,
        2,
    );
    let mut other_result = delivery_at(
        fixture
            .sessions
            .get_mut(&(CHANNEL + 1))
            .expect("other session"),
        HANDLE,
        0,
    );
    let mut selected_tokens = Vec::new();
    for (handle, id) in [(HANDLE, u32::MAX), (HANDLE + 1, 1)] {
        let link = sending_mut(fixture.sessions.get_mut(&CHANNEL).expect("session"), handle);
        let token = AckIdentity::new(&link.identity, id, &[]);
        link.pending_acknowledgements.insert(id, token.clone());
        selected_tokens.push(token);
    }
    let other_token = {
        let link = sending_mut(
            fixture
                .sessions
                .get_mut(&(CHANNEL + 1))
                .expect("other session"),
            HANDLE,
        );
        let token = AckIdentity::new(&link.identity, u32::MAX, &[]);
        link.pending_acknowledgements
            .insert(u32::MAX, token.clone());
        token
    };
    let incoming_owner = LinkIdentity::new();
    let incoming = {
        let ledger = &mut fixture
            .sessions
            .get_mut(&CHANNEL)
            .expect("session")
            .incoming;
        let incoming = ledger
            .reserve(&incoming_owner, 0, &[0])
            .expect("incoming ID may match outgoing");
        ledger
            .complete(&incoming, false, ReceiverSettleMode::Second)
            .expect("incoming complete");
        incoming
    };

    fixture.update(u32::MAX - 1, Some(1), true, None).await;
    for result in selected {
        let sent = result
            .await
            .expect("matching send reply")
            .expect("actual default outcome");
        assert_eq!(sent.outcome, Outcome::Released(Released));
        assert!(sent.acknowledgement.is_none());
    }
    assert!(selected_tokens.iter().all(AckIdentity::is_settled));
    assert_waiting(&mut outside);
    assert_waiting(&mut other_result);
    assert!(!other_token.is_settled());
    assert!(!fixture.link().unsettled[&2].receiver_settled);
    assert!(!sending_ref(&fixture.sessions[&(CHANNEL + 1)], HANDLE).unsettled[&0].receiver_settled);
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .settlement(&incoming_owner, &incoming),
        Ok(SettlementAction::SendDisposition { settled: false })
    );
    fixture.output.assert_silent();

    let full_span_token = {
        let link = sending_mut(
            fixture.sessions.get_mut(&CHANNEL).expect("session"),
            HANDLE + 1,
        );
        let token = AckIdentity::new(&link.identity, u32::MAX / 2, &[]);
        link.pending_acknowledgements
            .insert(token.id(), token.clone());
        token
    };
    // This range covers every serial number but only a few live map entries.
    fixture.update(0, Some(u32::MAX), true, None).await;
    let sent = outside
        .await
        .expect("remaining send reply")
        .expect("actual default outcome");
    assert_eq!(sent.outcome, Outcome::Released(Released));
    assert!(sent.acknowledgement.is_none());
    assert!(full_span_token.is_settled());
    for handle in [HANDLE, HANDLE + 1] {
        let link = sending_ref(&fixture.sessions[&CHANNEL], handle);
        assert!(link.unsettled.is_empty());
        assert!(link.pending_acknowledgements.is_empty());
        assert!(!link.identity.is_retired());
    }
    assert_waiting(&mut other_result);
    assert!(!other_token.is_settled());
    assert!(
        sending_ref(&fixture.sessions[&(CHANNEL + 1)], HANDLE).pending_acknowledgements[&u32::MAX]
            .same_ack(&other_token)
    );
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .settlement(&incoming_owner, &incoming),
        Ok(SettlementAction::SendDisposition { settled: false })
    );
    fixture.output.assert_silent();

    let mut untouched = fixture.delivery(3, false);
    let untouched_token = AckIdentity::new(&fixture.link().identity, 4, &[]);
    fixture
        .link_mut()
        .pending_acknowledgements
        .insert(4, untouched_token.clone());
    apply_disposition(
        CHANNEL,
        Disposition {
            role: Role::Sender,
            first: 0,
            last: Some(u32::MAX),
            settled: true,
            state: None,
            batchable: false,
        },
        &mut fixture.writer,
        &mut fixture.sessions,
    )
    .await
    .expect("sender disposition belongs to the opposite direction");
    assert_waiting(&mut untouched);
    assert!(!untouched_token.is_settled());
    assert!(!fixture.link().unsettled[&3].receiver_settled);
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .sender_is_settled(&incoming),
        Ok(true)
    );
    fixture.output.assert_silent();
}

#[tokio::test]
async fn remote_settled_send_still_waits_for_successful_final_transfer_flush() {
    for failed in [false, true] {
        let mut fixture = Fixture::new(Some(Outcome::Released(Released)), true);
        let mut result = fixture.delivery(ID, true);
        fixture
            .update(ID, None, false, Some(DeliveryState::Rejected(rejected())))
            .await;
        fixture.update(ID, None, true, None).await;
        assert_waiting(&mut result);
        fixture.output.assert_silent();
        if failed {
            fixture.output.failed_flush.store(true, Ordering::Release);
            let sent = send_fragment(
                CHANNEL,
                HANDLE,
                fixture.sessions.get_mut(&CHANNEL).expect("session"),
                &mut fixture.writer,
            )
            .await;
            assert!(matches!(sent, Err(EngineError::Io(_))));
            assert_waiting(&mut result);
            assert!(fixture.link().active.is_some());
            assert!(fixture.link().unsettled[&ID].receiver_settled);
            assert_eq!(
                fixture.link().unsettled[&ID].outcome,
                Some(Outcome::Rejected(rejected()))
            );
            assert_final_only(&fixture.output.frames().await);
        } else {
            fixture.output.blocked_flush.store(true, Ordering::Release);
            let mut processing = Box::pin(send_fragment(
                CHANNEL,
                HANDLE,
                fixture.sessions.get_mut(&CHANNEL).expect("session"),
                &mut fixture.writer,
            ));
            poll_fn(|context| {
                assert!(processing.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_waiting(&mut result);
            assert_final_only(&fixture.output.frames().await);
            fixture.output.release_flush();
            processing.await.expect("successful final flush");
            let sent = result
                .await
                .expect("post-flush reply")
                .expect("known terminal outcome");
            assert_eq!(sent.outcome, Outcome::Rejected(rejected()));
            assert!(sent.acknowledgement.is_none());
            assert!(fixture.link().active.is_none());
            assert!(fixture.link().unsettled.is_empty());
            assert_final_only(&fixture.output.frames().await);
        }
        assert!(fixture.link().pending_acknowledgements.is_empty());
    }
}
