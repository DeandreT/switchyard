use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use tokio::time::timeout;

use super::outgoing_identity::AckIdentity;
use super::*;

const CHANNEL: u16 = 3;
const HANDLE: u32 = 7;
const ID: u32 = 11;
const DEADLINE: Duration = Duration::from_secs(2);

#[derive(Default)]
struct CapturedOutput {
    bytes: Mutex<Vec<u8>>,
    blocked_flush: AtomicBool,
    failed_flush: AtomicBool,
    waiter: Mutex<Option<Waker>>,
}

impl CapturedOutput {
    fn release_flush(&self) {
        self.blocked_flush.store(false, Ordering::Release);
        if let Some(waiter) = self.waiter.lock().expect("flush waiter").take() {
            waiter.wake();
        }
    }

    fn assert_silent(&self) {
        assert!(self.bytes.lock().expect("captured output").is_empty());
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = self.bytes.lock().expect("captured output").clone();
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            frames.push(read_frame(&mut input).await.expect("complete output frame"));
        }
        frames
    }

    fn clear(&self) {
        self.bytes.lock().expect("captured output").clear();
    }
}

struct CapturedWriter(Arc<CapturedOutput>);

impl AsyncWrite for CapturedWriter {
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

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.failed_flush.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected outgoing acknowledgement flush failure",
            )));
        }
        if self.0.blocked_flush.load(Ordering::Acquire) {
            *self.0.waiter.lock().expect("flush waiter") = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn sending_link(identity: LinkIdentity, auto_acknowledge: bool) -> SendingLink {
    let (detached, _) = watch::channel(false);
    SendingLink {
        identity,
        auto_acknowledge,
        max_message_size: None,
        receiver_settle_mode: ReceiverSettleMode::Second,
        default_outcome: None,
        outstanding_tags: HashSet::new(),
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
    owner: LinkIdentity,
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<CapturedWriter>,
    output: Arc<CapturedOutput>,
}

impl Fixture {
    fn new(auto_acknowledge: bool) -> Self {
        let owner = LinkIdentity::new();
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        session.links.insert(
            HANDLE,
            LinkState::Sending(Box::new(sending_link(owner.clone(), auto_acknowledge))),
        );
        let output = Arc::new(CapturedOutput::default());
        Self {
            owner,
            sessions: HashMap::from([(CHANNEL, session)]),
            writer: FrameWriter::new(CapturedWriter(output.clone()), 512).expect("frame writer"),
            output,
        }
    }

    fn sending(&self) -> &SendingLink {
        let LinkState::Sending(link) = &self.sessions[&CHANNEL].links[&HANDLE] else {
            panic!("sending endpoint remains installed");
        };
        link
    }

    fn sending_mut(&mut self) -> &mut SendingLink {
        let LinkState::Sending(link) = self
            .sessions
            .get_mut(&CHANNEL)
            .expect("session")
            .links
            .get_mut(&HANDLE)
            .expect("endpoint")
        else {
            panic!("sending endpoint remains installed");
        };
        link
    }

    fn pending(&mut self, id: u32) -> AckIdentity {
        let tag = id.to_be_bytes();
        let token = AckIdentity::new(&self.owner, id, &tag);
        self.sending_mut().outstanding_tags.insert(tag.to_vec());
        assert!(
            self.sending_mut()
                .pending_acknowledgements
                .insert(id, token.clone())
                .is_none()
        );
        token
    }

    async fn process(&mut self, command: Command) {
        let action = timeout(
            DEADLINE,
            handle_command(command, &mut self.writer, &mut self.sessions, 512),
        )
        .await
        .expect("prompt outgoing command")
        .expect("local refusal does not stop the connection");
        assert!(matches!(action, CommandAction::Continue));
    }

    async fn settle(
        &mut self,
        owner: &LinkIdentity,
        identity: Option<AckIdentity>,
        state: DeliveryState,
    ) -> Result<(), EngineError> {
        let (reply, result) = oneshot::channel();
        self.process(Command::SettleOutgoing {
            channel: CHANNEL,
            handle: HANDLE,
            owner: owner.clone(),
            identity,
            state,
            reply,
        })
        .await;
        result.await.expect("outgoing settlement result")
    }
}

fn accepted() -> DeliveryState {
    DeliveryState::Accepted(Accepted)
}

fn assert_ack(frame: &Frame, id: u32, state: Option<DeliveryState>) {
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Disposition(disposition)),
        payload,
    } = frame
    else {
        panic!("Sender acknowledgement expected: {frame:?}");
    };
    assert_eq!(*channel, CHANNEL);
    assert!(payload.is_empty());
    assert_eq!(disposition.role, Role::Sender);
    assert_eq!(disposition.first, id);
    assert_eq!(disposition.last, None);
    assert!(disposition.settled);
    assert_eq!(disposition.state, state);
}

#[tokio::test]
async fn manual_acknowledgement_is_exact_and_terminal_reuse_cannot_consume_a_new_alias() {
    let mut fixture = Fixture::new(false);
    let owner = fixture.owner.clone();
    let old = fixture.pending(ID);
    fixture
        .settle(&owner, Some(old.clone()), accepted())
        .await
        .expect("owned acknowledgement");
    assert!(old.is_settled());
    assert!(fixture.sending().pending_acknowledgements.is_empty());
    let frames = fixture.output.frames().await;
    assert_eq!(frames.len(), 1);
    assert_ack(&frames[0], ID, Some(accepted()));
    fixture.output.clear();

    let replacement = fixture.pending(ID);
    fixture
        .settle(&owner, Some(old), accepted())
        .await
        .expect("owned terminal acknowledgement is idempotent");
    fixture.output.assert_silent();
    assert!(!replacement.is_settled());
    assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&replacement));
    fixture
        .settle(&owner, Some(replacement.clone()), accepted())
        .await
        .expect("replacement remains independently settleable");
    assert!(replacement.is_settled());
    assert!(fixture.sending().pending_acknowledgements.is_empty());
    let frames = fixture.output.frames().await;
    assert_eq!(frames.len(), 1);
    assert_ack(&frames[0], ID, Some(accepted()));
}

#[tokio::test]
async fn fresh_tokens_with_the_same_numeric_id_never_claim_the_pending_acknowledgement() {
    let mut fixture = Fixture::new(false);
    let owner = fixture.owner.clone();
    let pending = fixture.pending(ID);
    for token_owner in [owner.clone(), LinkIdentity::new()] {
        let impostor = AckIdentity::new(&token_owner, ID, &[]);
        assert!(
            fixture
                .settle(&owner, Some(impostor.clone()), accepted())
                .await
                .is_err()
        );
        assert!(!impostor.is_settled());
        assert!(!pending.is_settled());
        assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&pending));
        fixture.output.assert_silent();
    }
    fixture
        .settle(&owner, Some(pending), accepted())
        .await
        .expect("correct receipt remains valid after forged attempts");
    assert_eq!(fixture.output.frames().await.len(), 1);
}

#[tokio::test]
async fn borrowed_pending_settlement_preserves_oversized_rejection_for_valid_retry() {
    let mut fixture = Fixture::new(false);
    let token = fixture.pending(ID);
    let (commands, mut incoming) = mpsc::channel(1);
    let pending = PendingSettlement {
        outcome: Outcome::Accepted(Accepted),
        identity: fixture.owner.clone(),
        acknowledgement: Some(token.clone()),
        channel: CHANNEL,
        handle: HANDLE,
        commands,
    };
    let error = Error::new(crate::AmqpError::InternalError, "x".repeat(1024), None);
    let (rejected, ()) = timeout(DEADLINE, async {
        tokio::join!(pending.reject(error), async {
            fixture
                .process(incoming.recv().await.expect("borrowed rejection command"))
                .await;
        })
    })
    .await
    .expect("oversized rejection remains a local failure");
    assert!(matches!(rejected, Err(EngineError::Io(_))));
    fixture.output.assert_silent();
    assert!(!token.is_settled());
    assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&token));

    for attempt in 0..2 {
        let (result, ()) = timeout(DEADLINE, async {
            tokio::join!(pending.accept(), async {
                fixture
                    .process(incoming.recv().await.expect("borrowed acceptance command"))
                    .await;
            })
        })
        .await
        .expect("valid borrowed acceptance is prompt");
        result.expect("retry and repeated acceptance succeed");
        assert!(token.is_settled());
        assert!(fixture.sending().pending_acknowledgements.is_empty());
        let frames = fixture.output.frames().await;
        assert_eq!(frames.len(), usize::from(attempt == 0));
        if let Some(frame) = frames.first() {
            assert_ack(frame, ID, Some(accepted()));
        }
        fixture.output.clear();
    }
}

#[tokio::test]
async fn owner_validation_precedes_no_ack_and_terminal_noops() {
    let mut fixture = Fixture::new(false);
    let owner = fixture.owner.clone();
    let pending = fixture.pending(ID);
    let foreign_owner = LinkIdentity::new();
    let foreign_terminal = AckIdentity::new(&foreign_owner, ID, &[]);
    foreign_terminal.mark_settled();
    assert!(
        fixture
            .settle(&owner, Some(foreign_terminal), accepted())
            .await
            .is_err()
    );
    assert!(!pending.is_settled());
    assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&pending));
    fixture.output.assert_silent();
    let terminal = AckIdentity::new(&owner, ID, &[]);
    terminal.mark_settled();
    for acknowledgement in [None, Some(terminal)] {
        assert!(
            fixture
                .settle(&LinkIdentity::new(), acknowledgement.clone(), accepted())
                .await
                .is_err()
        );
        fixture.output.assert_silent();
        fixture
            .settle(&owner, acknowledgement, accepted())
            .await
            .expect("owned no-op needs no acknowledgement frame");
        assert!(!pending.is_settled());
        assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&pending));
        fixture.output.assert_silent();
    }
    fixture
        .settle(&owner, Some(pending), accepted())
        .await
        .expect("owned pending acknowledgement remains valid");
    assert_eq!(fixture.output.frames().await.len(), 1);
}

#[tokio::test]
async fn retired_owner_without_an_acknowledgement_never_becomes_a_successful_noop() {
    for remove_link in [false, true] {
        let mut fixture = Fixture::new(false);
        let owner = fixture.owner.clone();
        let pending = fixture.pending(ID);
        if remove_link {
            let mut link = fixture
                .sessions
                .get_mut(&CHANNEL)
                .expect("session")
                .links
                .remove(&HANDLE)
                .expect("sending endpoint");
            stop_link(&mut link);
        } else {
            owner.retire();
        }
        let (commands, mut incoming) = mpsc::channel(1);
        let receipt = PendingSettlement {
            outcome: Outcome::Accepted(Accepted),
            identity: owner,
            acknowledgement: None,
            channel: CHANNEL,
            handle: HANDLE,
            commands,
        };
        let (result, ()) = timeout(DEADLINE, async {
            tokio::join!(receipt.accept(), async {
                fixture
                    .process(incoming.recv().await.expect("no-ack ownership command"))
                    .await;
            })
        })
        .await
        .expect("retired no-ack receipt refusal is prompt");
        assert!(matches!(result, Err(EngineError::RemoteDetached)));
        assert!(!pending.is_settled());
        if !remove_link {
            assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&pending));
        }
        fixture.output.assert_silent();
    }
}

#[tokio::test]
async fn retired_pending_and_terminal_tokens_cannot_settle_reused_handles_or_channels() {
    for replace_session in [false, true] {
        let mut fixture = Fixture::new(false);
        let old_owner = fixture.owner.clone();
        let old_pending = fixture.pending(ID);
        let old_terminal = AckIdentity::new(&old_owner, ID.wrapping_add(1), &[]);
        old_terminal.mark_settled();
        let mut retired = fixture
            .sessions
            .get_mut(&CHANNEL)
            .expect("original session")
            .links
            .remove(&HANDLE)
            .expect("original endpoint");
        stop_link(&mut retired);
        assert!(old_owner.is_retired());
        let replacement_owner = LinkIdentity::new();
        if replace_session {
            let mut session = SessionState::new(&Begin::default());
            session.local_begin_sent = true;
            fixture.sessions.insert(CHANNEL, session);
        }
        fixture
            .sessions
            .get_mut(&CHANNEL)
            .expect("replacement session")
            .links
            .insert(
                HANDLE,
                LinkState::Sending(Box::new(sending_link(replacement_owner.clone(), false))),
            );
        fixture.owner = replacement_owner.clone();
        let replacement = fixture.pending(ID);
        for old in [None, Some(old_pending), Some(old_terminal)] {
            assert!(matches!(
                fixture.settle(&old_owner, old, accepted()).await,
                Err(EngineError::RemoteDetached)
            ));
            assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&replacement));
            assert!(!replacement.is_settled());
            fixture.output.assert_silent();
        }
        fixture
            .settle(&replacement_owner, Some(replacement), accepted())
            .await
            .expect("replacement generation remains healthy");
        assert_eq!(fixture.output.frames().await.len(), 1);
    }
}

#[tokio::test]
async fn stale_send_and_endpoint_close_cannot_mutate_replacement_generations() {
    for receiving_replacement in [false, true] {
        for retired in [false, true] {
            let mut fixture = Fixture::new(false);
            let stranger = LinkIdentity::new();
            if retired {
                stranger.retire();
            }
            if receiving_replacement {
                let (deliveries, _) = mpsc::channel(1);
                let (detached, _) = watch::channel(false);
                let consumption = Arc::new(Consumption::new(Arc::new(Notify::new())));
                fixture
                    .sessions
                    .get_mut(&CHANNEL)
                    .expect("session")
                    .links
                    .insert(
                        HANDLE,
                        LinkState::Receiving(ReceivingLink {
                            max_message_size: u64::MAX,
                            deliveries,
                            partial: None,
                            detached,
                            credit: ReceiveCredit::new(0, LINK_CREDIT, consumption),
                            decoders: MessageFormatDecoders::default(),
                            identity: fixture.owner.clone(),
                            sender_settle_mode: SenderSettleMode::Mixed,
                            receiver_settle_mode: ReceiverSettleMode::First,
                        }),
                    );
            }
            let (reply, result) = oneshot::channel();
            fixture
                .process(Command::Send {
                    channel: CHANNEL,
                    handle: HANDLE,
                    identity: stranger.clone(),
                    message: Box::new(Message::data(vec![1])),
                    delivery_tag: vec![1].into(),
                    reply,
                })
                .await;
            assert!(result.await.expect("stale send result").is_err());
            fixture.output.assert_silent();

            let (reply, result) = oneshot::channel();
            fixture
                .process(Command::Detach {
                    channel: CHANNEL,
                    handle: HANDLE,
                    identity: stranger,
                    error: None,
                    reply,
                })
                .await;
            let result = result.await.expect("stale close result");
            if !retired {
                assert!(result.is_err());
            }
            fixture.output.assert_silent();
            let session = &fixture.sessions[&CHANNEL];
            assert!(session.closing_handles.is_empty());
            assert!(!fixture.owner.is_retired());
            match &session.links[&HANDLE] {
                LinkState::Sending(link) => {
                    assert!(link.identity.same_link(&fixture.owner));
                    assert!(link.queued.is_empty());
                    assert!(link.active.is_none());
                }
                LinkState::Receiving(link) => {
                    assert!(link.identity.same_link(&fixture.owner));
                }
            }
        }
    }
}

#[tokio::test]
async fn manual_acknowledgement_commits_and_replies_only_after_flush() {
    let mut fixture = Fixture::new(false);
    let token = fixture.pending(ID);
    fixture.output.blocked_flush.store(true, Ordering::Release);
    let (reply, mut result) = oneshot::channel();
    let command = Command::SettleOutgoing {
        channel: CHANNEL,
        handle: HANDLE,
        owner: fixture.owner.clone(),
        identity: Some(token.clone()),
        state: accepted(),
        reply,
    };
    let mut processing = Box::pin(handle_command(
        command,
        &mut fixture.writer,
        &mut fixture.sessions,
        512,
    ));
    poll_fn(|cx| {
        assert!(processing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(!token.is_settled());
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let frames = fixture.output.frames().await;
    assert_eq!(frames.len(), 1);
    assert_ack(&frames[0], ID, Some(accepted()));
    fixture.output.release_flush();
    let action = timeout(DEADLINE, processing)
        .await
        .expect("released acknowledgement is prompt")
        .expect("successful acknowledgement write");
    assert!(matches!(action, CommandAction::Continue));
    result.await.expect("postwrite reply").expect("settlement");
    assert!(token.is_settled());
    assert!(fixture.sending().pending_acknowledgements.is_empty());
}

#[tokio::test]
async fn failed_acknowledgement_flush_does_not_commit_the_pending_token() {
    let mut fixture = Fixture::new(false);
    let token = fixture.pending(ID);
    fixture.output.failed_flush.store(true, Ordering::Release);
    let (reply, _result) = oneshot::channel();
    let result = timeout(
        DEADLINE,
        handle_command(
            Command::SettleOutgoing {
                channel: CHANNEL,
                handle: HANDLE,
                owner: fixture.owner.clone(),
                identity: Some(token.clone()),
                state: accepted(),
                reply,
            },
            &mut fixture.writer,
            &mut fixture.sessions,
            512,
        ),
    )
    .await
    .expect("flush failure is prompt");
    assert!(matches!(result, Err(EngineError::Io(_))));
    assert!(!token.is_settled());
    assert!(fixture.sending().pending_acknowledgements[&ID].same_ack(&token));
}

#[tokio::test]
async fn automatic_acknowledgement_precedes_send_reply_and_survives_a_dropped_receiver() {
    for dropped in [false, true] {
        let mut fixture = Fixture::new(true);
        let (reply, result) = oneshot::channel();
        let mut result = (!dropped).then_some(result);
        fixture
            .sending_mut()
            .outstanding_tags
            .insert(vec![ID as u8]);
        fixture.sending_mut().unsettled.insert(
            ID,
            OutgoingDelivery {
                reply,
                delivery_tag: vec![ID as u8].into(),
                outcome: Some(Outcome::Accepted(Accepted)),
                receiver_settled: false,
            },
        );
        fixture.output.blocked_flush.store(true, Ordering::Release);
        let link = fixture
            .sessions
            .get_mut(&CHANNEL)
            .expect("session")
            .links
            .get_mut(&HANDLE)
            .expect("sending endpoint");
        let LinkState::Sending(link) = link else {
            panic!("sending endpoint");
        };
        let mut processing = Box::pin(resolve_outgoing(CHANNEL, link, ID, &mut fixture.writer));
        poll_fn(|cx| {
            assert!(processing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        if let Some(result) = &mut result {
            assert!(matches!(
                result.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        let frames = fixture.output.frames().await;
        assert_eq!(frames.len(), 1);
        assert_ack(&frames[0], ID, None);
        fixture.output.release_flush();
        timeout(DEADLINE, processing)
            .await
            .expect("automatic acknowledgement completes")
            .expect("automatic acknowledgement write");
        if let Some(result) = result {
            let sent = result.await.expect("send reply").expect("send outcome");
            assert_eq!(sent.outcome, Outcome::Accepted(Accepted));
            assert!(sent.acknowledgement.is_none());
        }
        assert!(fixture.sending().pending_acknowledgements.is_empty());
        assert!(fixture.sending().unsettled.is_empty());
    }
}

#[tokio::test]
async fn automatic_acknowledgement_flush_failure_never_publishes_a_successful_send() {
    let mut fixture = Fixture::new(true);
    let (reply, mut result) = oneshot::channel();
    fixture
        .sending_mut()
        .outstanding_tags
        .insert(vec![ID as u8]);
    fixture.sending_mut().unsettled.insert(
        ID,
        OutgoingDelivery {
            reply,
            delivery_tag: vec![ID as u8].into(),
            outcome: Some(Outcome::Accepted(Accepted)),
            receiver_settled: false,
        },
    );
    fixture.output.failed_flush.store(true, Ordering::Release);
    let link = fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .get_mut(&HANDLE)
        .expect("sending endpoint");
    let LinkState::Sending(link) = link else {
        panic!("sending endpoint");
    };
    let resolved = timeout(
        DEADLINE,
        resolve_outgoing(CHANNEL, link, ID, &mut fixture.writer),
    )
    .await
    .expect("automatic flush failure is prompt");
    assert!(matches!(resolved, Err(EngineError::Io(_))));
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed)
            | Ok(Err(_))
    ));
    let token = fixture
        .sending()
        .pending_acknowledgements
        .get(&ID)
        .expect("failed acknowledgement remains uncommitted");
    assert!(!token.is_settled());
    assert!(token.belongs_to(&fixture.owner));
    let frames = fixture.output.frames().await;
    assert_eq!(frames.len(), 1);
    assert_ack(&frames[0], ID, None);
}

#[tokio::test]
async fn early_outcome_is_latched_until_final_transfer_then_automatically_acknowledged() {
    let mut fixture = Fixture::new(true);
    let (reply, mut result) = oneshot::channel();
    let link = fixture.sending_mut();
    link.outstanding_tags.insert(vec![ID as u8]);
    link.unsettled.insert(
        ID,
        OutgoingDelivery {
            reply,
            delivery_tag: vec![ID as u8].into(),
            outcome: None,
            receiver_settled: false,
        },
    );
    link.active = Some(ActiveSend {
        payload: vec![1, 2],
        offset: 1,
        first_frame_sent: true,
        delivery_id: ID,
        delivery_tag: vec![ID as u8].into(),
        message_format: 0,
        settled: false,
        settled_reply: None,
    });
    apply_disposition(
        CHANNEL,
        Disposition {
            role: Role::Receiver,
            first: ID,
            last: None,
            settled: false,
            state: Some(accepted()),
            batchable: false,
        },
        &mut fixture.writer,
        &mut fixture.sessions,
    )
    .await
    .expect("early disposition is latched");
    fixture.output.assert_silent();
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(fixture.sending().active.is_some());
    assert!(fixture.sending().unsettled[&ID].outcome.is_some());
    assert!(fixture.sending().pending_acknowledgements.is_empty());

    send_fragment(
        CHANNEL,
        HANDLE,
        fixture.sessions.get_mut(&CHANNEL).expect("session"),
        &mut fixture.writer,
    )
    .await
    .expect("final transfer and Sender acknowledgement");
    let frames = fixture.output.frames().await;
    let [transfer, acknowledgement] = frames.as_slice() else {
        panic!("final Transfer must precede acknowledgement: {frames:?}");
    };
    assert!(matches!(transfer, Frame::Amqp {
        channel: CHANNEL,
        performative: Some(Performative::Transfer(value)),
        payload,
    } if value.handle == HANDLE && !value.more && value.delivery_id.is_none() && payload == &[2]));
    assert_ack(acknowledgement, ID, None);
    let sent = result
        .await
        .expect("completed send reply")
        .expect("outcome");
    assert_eq!(sent.outcome, Outcome::Accepted(Accepted));
    assert!(sent.acknowledgement.is_none());
    assert!(fixture.sending().active.is_none());
    assert!(fixture.sending().unsettled.is_empty());
    assert!(fixture.sending().pending_acknowledgements.is_empty());
}

#[tokio::test]
async fn automatic_policy_never_acknowledges_first_mode_or_already_settled_outcomes() {
    for receiver_settle_mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for peer_settled in [false, true] {
            if receiver_settle_mode == ReceiverSettleMode::Second && !peer_settled {
                continue;
            }
            let mut fixture = Fixture::new(true);
            fixture.sending_mut().receiver_settle_mode = receiver_settle_mode.clone();
            let (reply, result) = oneshot::channel();
            fixture
                .sending_mut()
                .outstanding_tags
                .insert(vec![ID as u8]);
            fixture.sending_mut().unsettled.insert(
                ID,
                OutgoingDelivery {
                    reply,
                    delivery_tag: vec![ID as u8].into(),
                    outcome: None,
                    receiver_settled: false,
                },
            );
            apply_disposition(
                CHANNEL,
                Disposition {
                    role: Role::Receiver,
                    first: ID,
                    last: None,
                    settled: peer_settled,
                    state: Some(accepted()),
                    batchable: false,
                },
                &mut fixture.writer,
                &mut fixture.sessions,
            )
            .await
            .expect("receiver outcome");
            let sent = result.await.expect("send reply").expect("send outcome");
            assert_eq!(sent.outcome, Outcome::Accepted(Accepted));
            assert!(sent.acknowledgement.is_none());
            assert!(fixture.sending().unsettled.is_empty());
            assert!(fixture.sending().pending_acknowledgements.is_empty());
            fixture.output.assert_silent();
        }
    }
}

#[tokio::test]
async fn presettled_active_send_reports_success_only_after_final_transfer_flush() {
    let mut fixture = Fixture::new(true);
    let (reply, mut result) = oneshot::channel();
    fixture.sending_mut().active = Some(ActiveSend {
        payload: vec![1, 2],
        offset: 1,
        first_frame_sent: true,
        delivery_id: ID,
        delivery_tag: vec![ID as u8].into(),
        message_format: 0,
        settled: true,
        settled_reply: Some(reply),
    });
    fixture.output.blocked_flush.store(true, Ordering::Release);
    let mut processing = Box::pin(send_fragment(
        CHANNEL,
        HANDLE,
        fixture.sessions.get_mut(&CHANNEL).expect("session"),
        &mut fixture.writer,
    ));
    poll_fn(|cx| {
        assert!(processing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let frames = fixture.output.frames().await;
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], Frame::Amqp {
        performative: Some(Performative::Transfer(value)),
        payload,
        ..
    } if value.handle == HANDLE && !value.more && payload == &[2]));
    fixture.output.release_flush();
    timeout(DEADLINE, processing)
        .await
        .expect("final presettled write completes")
        .expect("presettled send");
    let sent = result.await.expect("send reply").expect("send outcome");
    assert_eq!(sent.outcome, Outcome::Accepted(Accepted));
    assert!(sent.acknowledgement.is_none());
    assert!(fixture.sending().active.is_none());
    assert!(fixture.sending().pending_acknowledgements.is_empty());
}
