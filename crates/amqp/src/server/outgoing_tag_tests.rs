use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use super::*;
use crate::{Body, Rejected, Value};

const CHANNEL: u16 = 3;
const HANDLE: u32 = 7;

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    failed_flush: AtomicBool,
    blocked_flush: AtomicBool,
}

impl Output {
    fn len(&self) -> usize {
        self.bytes.lock().expect("captured bytes").len()
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = self.bytes.lock().expect("captured bytes").clone();
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
            .expect("captured bytes")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.failed_flush.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected flush failure",
            )));
        }
        if self.0.blocked_flush.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn sending() -> SendingLink {
    let (detached, _) = watch::channel(false);
    let mut credit = LinkCredit::new(0);
    credit
        .update_peer(Some(0), 10_000, false)
        .expect("grant credit");
    SendingLink {
        identity: LinkIdentity::new(),
        auto_acknowledge: false,
        default_outcome: None,
        outstanding_tags: HashSet::new(),
        max_message_size: None,
        receiver_settle_mode: ReceiverSettleMode::Second,
        settle_mode: SenderSettleMode::Unsettled,
        credit,
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
    fn new() -> Self {
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        session
            .links
            .insert(HANDLE, LinkState::Sending(Box::new(sending())));
        let output = Arc::new(Output::default());
        Self {
            sessions: HashMap::from([(CHANNEL, session)]),
            writer: FrameWriter::new(Writer(output.clone()), 512).expect("frame writer"),
            output,
        }
    }

    fn link(&self, handle: u32) -> &SendingLink {
        let LinkState::Sending(link) = &self.sessions[&CHANNEL].links[&handle] else {
            panic!("sending link");
        };
        link
    }

    fn link_mut(&mut self, handle: u32) -> &mut SendingLink {
        let LinkState::Sending(link) = self
            .sessions
            .get_mut(&CHANNEL)
            .expect("session")
            .links
            .get_mut(&handle)
            .expect("link")
        else {
            panic!("sending link");
        };
        link
    }

    async fn queue_as(
        &mut self,
        handle: u32,
        owner: &LinkIdentity,
        message: Message,
        tag: &[u8],
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let (reply, result) = oneshot::channel();
        queue_send(
            CHANNEL,
            handle,
            self.sessions.get_mut(&CHANNEL).expect("session"),
            owner,
            message,
            tag.to_vec().into(),
            0,
            reply,
            &mut self.writer,
            512,
        )
        .await
        .expect("local refusal is not an actor failure");
        result
    }

    async fn queue(
        &mut self,
        handle: u32,
        message: Message,
        tag: &[u8],
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let owner = self.link(handle).identity.clone();
        self.queue_as(handle, &owner, message, tag).await
    }

    async fn fragment(&mut self, handle: u32) -> Result<(), EngineError> {
        send_fragment(
            CHANNEL,
            handle,
            self.sessions.get_mut(&CHANNEL).expect("session"),
            &mut self.writer,
        )
        .await
    }

    async fn finish(&mut self, handle: u32) {
        self.fragment(handle).await.expect("first fragment");
        while self.link(handle).active.is_some() {
            self.fragment(handle).await.expect("continuation fragment");
        }
    }

    async fn update(
        &mut self,
        id: u32,
        settled: bool,
        state: Option<DeliveryState>,
    ) -> Result<(), EngineError> {
        apply_disposition(
            CHANNEL,
            Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled,
                state,
                batchable: false,
            },
            &mut self.writer,
            &mut self.sessions,
        )
        .await
    }

    async fn settle(
        &mut self,
        handle: u32,
        token: Option<&AckIdentity>,
        state: DeliveryState,
    ) -> Result<(), EngineError> {
        let owner = self.link(handle).identity.clone();
        let (reply, result) = oneshot::channel();
        settle_outgoing(
            CHANNEL,
            handle,
            owner,
            token.cloned(),
            state,
            reply,
            &mut self.sessions,
            &mut self.writer,
        )
        .await?;
        result.await.expect("settlement reply")
    }

    async fn pending(&mut self, handle: u32, tag: &[u8]) -> (u32, AckIdentity) {
        let id = self.sessions[&CHANNEL].next_delivery_id;
        let result = self.queue(handle, Message::data(vec![1]), tag).await;
        self.finish(handle).await;
        self.update(id, false, Some(accepted()))
            .await
            .expect("terminal outcome");
        let sent = result.await.expect("send reply").expect("send outcome");
        (id, sent.acknowledgement.expect("manual ACK token"))
    }

    fn seed_ack(&mut self, handle: u32, id: u32) -> AckIdentity {
        let tag = id.to_be_bytes();
        let link = self.link_mut(handle);
        assert!(link.outstanding_tags.insert(tag.to_vec()));
        let token = AckIdentity::new(&link.identity, id, &tag);
        assert!(
            link.pending_acknowledgements
                .insert(id, token.clone())
                .is_none()
        );
        token
    }

    async fn refused(&mut self, handle: u32, tag: &[u8], expected: &str) {
        let session = &self.sessions[&CHANNEL];
        let id = session.next_delivery_id;
        let flow = session.flow.snapshot();
        let link = self.link(handle);
        let credit = link.credit.snapshot();
        let tags = link.outstanding_tags.clone();
        let queued = link.queued.len();
        let unsettled: HashSet<_> = link.unsettled.keys().copied().collect();
        let pending = link.pending_acknowledgements.clone();
        let active = link
            .active
            .as_ref()
            .map(|send| (send.delivery_id, send.offset));
        let bytes = self.output.len();
        let result = self
            .queue(handle, invalid_message(), tag)
            .await
            .await
            .expect("refusal reply");
        assert!(
            matches!(result, Err(EngineError::InvalidState(ref message)) if message.contains(expected))
        );
        assert_eq!(self.sessions[&CHANNEL].next_delivery_id, id);
        assert_eq!(self.sessions[&CHANNEL].flow.snapshot(), flow);
        let link = self.link(handle);
        assert_eq!(link.credit.snapshot(), credit);
        assert_eq!(link.outstanding_tags, tags);
        assert_eq!(link.queued.len(), queued);
        assert_eq!(
            link.unsettled.keys().copied().collect::<HashSet<_>>(),
            unsettled
        );
        assert_eq!(link.pending_acknowledgements, pending);
        assert_eq!(
            link.active
                .as_ref()
                .map(|send| (send.delivery_id, send.offset)),
            active
        );
        assert_eq!(self.output.len(), bytes);
    }

    fn stop(&mut self, handle: u32) {
        stop_link(
            self.sessions
                .get_mut(&CHANNEL)
                .expect("session")
                .links
                .get_mut(&handle)
                .expect("link"),
        );
        assert!(self.link(handle).outstanding_tags.is_empty());
    }
}

fn accepted() -> DeliveryState {
    DeliveryState::Accepted(Accepted)
}

fn invalid_message() -> Message {
    Message {
        body: Body::Value(Value::Symbol(Symbol::from("\u{00e9}"))),
        ..Message::default()
    }
}

fn oversized_rejection() -> DeliveryState {
    DeliveryState::Rejected(Rejected {
        error: Some(Error::new(
            crate::AmqpError::InternalError,
            "x".repeat(1024),
            None,
        )),
    })
}

#[tokio::test]
async fn duplicate_tag_is_reserved_through_queue_fragments_outcome_and_manual_ack() {
    for tag in [Vec::new(), vec![42; MAX_DELIVERY_TAG_BYTES]] {
        let mut fixture = Fixture::new();
        let result = fixture
            .queue(HANDLE, Message::data(vec![1; 2048]), &tag)
            .await;
        fixture.refused(HANDLE, &tag, "already in use").await;
        fixture.fragment(HANDLE).await.expect("first fragment");
        assert!(fixture.link(HANDLE).active.is_some());
        fixture.refused(HANDLE, &tag, "already in use").await;
        while fixture.link(HANDLE).active.is_some() {
            fixture.fragment(HANDLE).await.expect("next fragment");
        }
        fixture.refused(HANDLE, &tag, "already in use").await;
        fixture
            .update(0, false, Some(accepted()))
            .await
            .expect("outcome");
        let token = result
            .await
            .expect("send reply")
            .expect("outcome")
            .acknowledgement
            .expect("ACK");
        fixture.refused(HANDLE, &tag, "already in use").await;
        fixture
            .settle(HANDLE, Some(&token), accepted())
            .await
            .expect("flushed ACK");
        assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
        let retry = fixture.queue(HANDLE, Message::data(vec![2]), &tag).await;
        assert!(
            fixture
                .link(HANDLE)
                .outstanding_tags
                .contains(tag.as_slice())
        );
        fixture.stop(HANDLE);
        assert!(matches!(
            retry.await.expect("stopped send reply"),
            Err(EngineError::RemoteDetached)
        ));
    }
}

#[tokio::test]
async fn early_remote_settlement_releases_tag_only_after_final_transfer_flush() {
    for state in [None, Some(accepted())] {
        let mut fixture = Fixture::new();
        let result = fixture
            .queue(HANDLE, Message::data(vec![1; 2048]), b"early")
            .await;
        fixture.fragment(HANDLE).await.expect("first fragment");
        fixture
            .update(0, true, state.clone())
            .await
            .expect("early settlement");
        fixture.refused(HANDLE, b"early", "already in use").await;
        while fixture.link(HANDLE).active.is_some() {
            fixture.fragment(HANDLE).await.expect("continuation");
        }
        let sent = result.await.expect("send reply");
        if state.is_some() {
            assert!(sent.expect("terminal outcome").acknowledgement.is_none());
        } else {
            assert!(matches!(
                sent,
                Err(EngineError::RemoteSettledWithoutOutcome)
            ));
        }
        assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
        drop(
            fixture
                .queue(HANDLE, Message::data(vec![2]), b"early")
                .await,
        );
        assert_eq!(fixture.link(HANDLE).outstanding_tags.len(), 1);
    }
}

#[tokio::test]
async fn first_mode_remote_settlement_and_automatic_ack_release_completed_tags() {
    for mode in 0..3 {
        let mut fixture = Fixture::new();
        if mode == 0 {
            fixture.link_mut(HANDLE).receiver_settle_mode = ReceiverSettleMode::First;
        }
        fixture.link_mut(HANDLE).auto_acknowledge = mode == 2;
        let result = fixture
            .queue(HANDLE, Message::data(vec![1]), b"reuse")
            .await;
        fixture.finish(HANDLE).await;
        fixture
            .update(0, mode == 1, Some(accepted()))
            .await
            .expect("outcome");
        assert!(
            result
                .await
                .expect("reply")
                .expect("outcome")
                .acknowledgement
                .is_none()
        );
        assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
        let frames = fixture.output.frames().await;
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(
                    frame,
                    Frame::Amqp {
                        performative: Some(Performative::Disposition(_)),
                        ..
                    }
                ))
                .count(),
            usize::from(mode == 2)
        );
        drop(
            fixture
                .queue(HANDLE, Message::data(vec![2]), b"reuse")
                .await,
        );
        assert_eq!(fixture.link(HANDLE).outstanding_tags.len(), 1);
    }
}

#[tokio::test]
async fn successful_presettled_final_flush_releases_tag_without_any_remote_disposition() {
    let mut fixture = Fixture::new();
    fixture.link_mut(HANDLE).settle_mode = SenderSettleMode::Settled;
    for size in [2048, 1] {
        let result = fixture
            .queue(HANDLE, Message::data(vec![1; size]), b"presettled")
            .await;
        fixture
            .refused(HANDLE, b"presettled", "already in use")
            .await;
        fixture.finish(HANDLE).await;
        assert!(
            result
                .await
                .expect("send reply")
                .expect("flushed delivery")
                .acknowledgement
                .is_none()
        );
        assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
    }
    assert!(fixture.output.frames().await.iter().all(|frame| matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Transfer(_)),
            ..
        }
    )));
}

#[tokio::test]
async fn dropped_send_waiter_keeps_tag_through_queue_active_and_unsettled_until_remote_settlement()
{
    let mut fixture = Fixture::new();
    drop(
        fixture
            .queue(HANDLE, Message::data(vec![1; 2048]), b"abandoned")
            .await,
    );
    fixture
        .refused(HANDLE, b"abandoned", "already in use")
        .await;
    fixture.fragment(HANDLE).await.expect("first fragment");
    fixture
        .refused(HANDLE, b"abandoned", "already in use")
        .await;
    while fixture.link(HANDLE).active.is_some() {
        fixture.fragment(HANDLE).await.expect("continuation");
    }
    fixture
        .refused(HANDLE, b"abandoned", "already in use")
        .await;
    fixture
        .update(0, false, Some(accepted()))
        .await
        .expect("outcome after abandoned waiter");
    fixture
        .refused(HANDLE, b"abandoned", "already in use")
        .await;
    fixture
        .update(0, true, None)
        .await
        .expect("peer settlement");
    assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
    drop(
        fixture
            .queue(HANDLE, Message::data(vec![2]), b"abandoned")
            .await,
    );
    fixture.stop(HANDLE);
}

#[tokio::test]
async fn old_terminal_and_absent_ack_cannot_release_reused_tag_or_id() {
    let mut fixture = Fixture::new();
    let (id, old) = fixture.pending(HANDLE, b"same").await;
    fixture
        .settle(HANDLE, Some(&old), accepted())
        .await
        .expect("first ACK");
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .next_delivery_id = id;
    let (new_id, fresh) = fixture.pending(HANDLE, b"same").await;
    assert_eq!(new_id, id);
    assert!(!old.same_ack(&fresh));
    let bytes = fixture.output.len();
    fixture
        .settle(HANDLE, Some(&old), oversized_rejection())
        .await
        .expect("terminal no-op");
    fixture
        .settle(HANDLE, None, oversized_rejection())
        .await
        .expect("absent no-op");
    assert_eq!(fixture.output.len(), bytes);
    assert!(!fresh.is_settled());
    fixture.refused(HANDLE, b"same", "already in use").await;
    fixture
        .settle(HANDLE, Some(&fresh), accepted())
        .await
        .expect("fresh ACK");
    assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
}

#[tokio::test]
async fn remote_settlement_releases_only_the_exact_pending_tag() {
    let mut fixture = Fixture::new();
    let (first_id, first) = fixture.pending(HANDLE, b"first").await;
    let (_, second) = fixture.pending(HANDLE, b"second").await;
    let bytes = fixture.output.len();
    fixture
        .update(first_id, true, None)
        .await
        .expect("remote settlement");
    assert_eq!(fixture.output.len(), bytes);
    assert!(first.is_settled());
    assert!(!second.is_settled());
    assert!(
        !fixture
            .link(HANDLE)
            .outstanding_tags
            .contains(b"first".as_slice())
    );
    fixture.refused(HANDLE, b"second", "already in use").await;
    drop(
        fixture
            .queue(HANDLE, Message::data(vec![2]), b"first")
            .await,
    );
    fixture
        .settle(HANDLE, Some(&first), accepted())
        .await
        .expect("old receipt no-op");
    fixture.refused(HANDLE, b"first", "already in use").await;
}

#[tokio::test]
async fn oversized_ack_keeps_tag_and_exact_token_until_valid_retry() {
    let mut fixture = Fixture::new();
    let (id, token) = fixture.pending(HANDLE, b"retry").await;
    let bytes = fixture.output.len();
    assert!(
        fixture
            .settle(HANDLE, Some(&token), oversized_rejection())
            .await
            .is_err()
    );
    assert_eq!(fixture.output.len(), bytes);
    assert!(!token.is_settled());
    assert!(fixture.link(HANDLE).pending_acknowledgements[&id].same_ack(&token));
    fixture.refused(HANDLE, b"retry", "already in use").await;
    fixture
        .settle(HANDLE, Some(&token), accepted())
        .await
        .expect("smaller retry");
    assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
}

#[tokio::test]
async fn failed_ack_flush_keeps_tag_for_manual_and_automatic_ack_until_teardown() {
    for automatic in [false, true] {
        let mut fixture = Fixture::new();
        if automatic {
            fixture.link_mut(HANDLE).auto_acknowledge = true;
            drop(
                fixture
                    .queue(HANDLE, Message::data(vec![1]), b"failed")
                    .await,
            );
            fixture.finish(HANDLE).await;
            fixture.output.failed_flush.store(true, Ordering::Release);
            assert!(fixture.update(0, false, Some(accepted())).await.is_err());
        } else {
            let (_, token) = fixture.pending(HANDLE, b"failed").await;
            fixture.output.failed_flush.store(true, Ordering::Release);
            assert!(
                fixture
                    .settle(HANDLE, Some(&token), accepted())
                    .await
                    .is_err()
            );
            assert!(!token.is_settled());
        }
        assert!(
            fixture
                .link(HANDLE)
                .outstanding_tags
                .contains(b"failed".as_slice())
        );
        assert_eq!(fixture.link(HANDLE).pending_acknowledgements.len(), 1);
        fixture.stop(HANDLE);
    }
}

#[tokio::test]
async fn cancelled_manual_and_automatic_ack_flush_retain_exact_tag_until_teardown() {
    for automatic in [false, true] {
        let mut fixture = Fixture::new();
        if automatic {
            fixture.link_mut(HANDLE).auto_acknowledge = true;
            drop(
                fixture
                    .queue(HANDLE, Message::data(vec![1]), b"cancelled")
                    .await,
            );
            fixture.finish(HANDLE).await;
            fixture.output.blocked_flush.store(true, Ordering::Release);
            let mut pending = Box::pin(fixture.update(0, false, Some(accepted())));
            poll_fn(|context| {
                assert!(pending.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(pending);
        } else {
            let (_, token) = fixture.pending(HANDLE, b"cancelled").await;
            fixture.output.blocked_flush.store(true, Ordering::Release);
            let mut pending = Box::pin(fixture.settle(HANDLE, Some(&token), accepted()));
            poll_fn(|context| {
                assert!(pending.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(pending);
            assert!(fixture.link(HANDLE).pending_acknowledgements[&token.id()].same_ack(&token));
        }
        assert!(
            fixture
                .link(HANDLE)
                .outstanding_tags
                .contains(b"cancelled".as_slice())
        );
        assert_eq!(fixture.link(HANDLE).pending_acknowledgements.len(), 1);
        assert!(
            !fixture
                .link(HANDLE)
                .pending_acknowledgements
                .values()
                .next()
                .expect("pending token")
                .is_settled()
        );
        fixture.stop(HANDLE);
    }
}

#[tokio::test]
async fn failed_or_cancelled_final_presettled_flush_keeps_tag_until_teardown() {
    for failed in [false, true] {
        let mut fixture = Fixture::new();
        fixture.link_mut(HANDLE).settle_mode = SenderSettleMode::Settled;
        let mut result = fixture
            .queue(HANDLE, Message::data(vec![1]), b"final")
            .await;
        if failed {
            fixture.output.failed_flush.store(true, Ordering::Release);
            assert!(fixture.fragment(HANDLE).await.is_err());
        } else {
            fixture.output.blocked_flush.store(true, Ordering::Release);
            let mut pending = Box::pin(fixture.fragment(HANDLE));
            poll_fn(|context| {
                assert!(pending.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(pending);
        }
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(fixture.link(HANDLE).active.is_some());
        fixture.refused(HANDLE, b"final", "already in use").await;
        fixture.stop(HANDLE);
        assert!(matches!(
            result.await.expect("teardown reply"),
            Err(EngineError::RemoteDetached)
        ));
    }
}

#[tokio::test]
async fn distinct_links_allow_identical_tags_and_wrong_owner_does_not_reserve_one() {
    let mut fixture = Fixture::new();
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .insert(HANDLE + 1, LinkState::Sending(Box::new(sending())));
    let (_, first) = fixture.pending(HANDLE, b"shared").await;
    let (_, second) = fixture.pending(HANDLE + 1, b"shared").await;
    let owner = fixture.link(HANDLE).identity.clone();
    let result = fixture
        .queue_as(HANDLE + 1, &owner, invalid_message(), b"unused")
        .await
        .await
        .expect("refusal");
    assert!(matches!(result, Err(EngineError::InvalidState(_))));
    assert_eq!(fixture.link(HANDLE + 1).outstanding_tags.len(), 1);
    fixture
        .settle(HANDLE, Some(&first), accepted())
        .await
        .expect("first link ACK");
    fixture
        .refused(HANDLE + 1, b"shared", "already in use")
        .await;
    assert!(!second.is_settled());
    drop(
        fixture
            .queue(HANDLE, Message::data(vec![2]), b"shared")
            .await,
    );
    fixture.stop(HANDLE);
    assert_eq!(fixture.link(HANDLE + 1).outstanding_tags.len(), 1);
}

#[tokio::test]
async fn queue_and_tag_length_refusals_do_not_encode_or_reserve_tags() {
    let mut fixture = Fixture::new();
    fixture
        .refused(HANDLE, &[1; MAX_DELIVERY_TAG_BYTES + 1], "exceeds 32")
        .await;
    for id in 0..DELIVERY_QUEUE_CAPACITY {
        drop(
            fixture
                .queue(HANDLE, Message::data(vec![1]), &(id as u32).to_be_bytes())
                .await,
        );
    }
    fixture.refused(HANDLE, b"overflow", "queue is full").await;
    fixture
        .refused(HANDLE, &0u32.to_be_bytes(), "already in use")
        .await;
    fixture.stop(HANDLE);
}

#[tokio::test]
async fn exact_link_limit_refuses_before_encoding_and_recovers_one_released_slot() {
    let mut fixture = Fixture::new();
    let first = fixture.seed_ack(HANDLE, 0);
    for id in 1..MAX_OUTGOING_DELIVERIES_PER_LINK as u32 {
        fixture.seed_ack(HANDLE, id);
    }
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .next_delivery_id = MAX_OUTGOING_DELIVERIES_PER_LINK as u32;
    fixture
        .refused(HANDLE, b"overflow", "limit reached on this link")
        .await;
    fixture
        .settle(HANDLE, Some(&first), accepted())
        .await
        .expect("release slot");
    drop(
        fixture
            .queue(HANDLE, Message::data(vec![1]), b"overflow")
            .await,
    );
    assert_eq!(
        fixture.link(HANDLE).outstanding_tags.len(),
        MAX_OUTGOING_DELIVERIES_PER_LINK
    );
    fixture.refused(HANDLE, b"overflow", "already in use").await;
    fixture.stop(HANDLE);
}

#[tokio::test]
async fn exact_session_limit_is_shared_across_links_but_not_other_sessions() {
    let mut fixture = Fixture::new();
    let mut first = None;
    for index in 0..4u32 {
        let handle = HANDLE + index;
        if index != 0 {
            fixture
                .sessions
                .get_mut(&CHANNEL)
                .expect("session")
                .links
                .insert(handle, LinkState::Sending(Box::new(sending())));
        }
        for offset in 0..MAX_OUTGOING_DELIVERIES_PER_LINK as u32 {
            let token = fixture.seed_ack(
                handle,
                index * MAX_OUTGOING_DELIVERIES_PER_LINK as u32 + offset,
            );
            if index == 0 && offset == 0 {
                first = Some(token);
            }
        }
    }
    let target = HANDLE + 4;
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .insert(target, LinkState::Sending(Box::new(sending())));
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .next_delivery_id = MAX_OUTGOING_DELIVERIES_PER_SESSION as u32;
    fixture
        .refused(target, b"overflow", "limit reached on this session")
        .await;
    let mut other = SessionState::new(&Begin::default());
    other.local_begin_sent = true;
    let sender = sending();
    let owner = sender.identity.clone();
    other
        .links
        .insert(HANDLE, LinkState::Sending(Box::new(sender)));
    let (reply, mut result) = oneshot::channel();
    queue_send(
        CHANNEL + 1,
        HANDLE,
        &mut other,
        &owner,
        Message::data(vec![1]),
        b"other".to_vec().into(),
        0,
        reply,
        &mut fixture.writer,
        512,
    )
    .await
    .expect("other session enqueue");
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    fixture
        .settle(HANDLE, first.as_ref(), accepted())
        .await
        .expect("release exact slot");
    drop(
        fixture
            .queue(target, Message::data(vec![1]), b"overflow")
            .await,
    );
    let total: usize = fixture.sessions[&CHANNEL]
        .links
        .values()
        .map(|link| match link {
            LinkState::Sending(link) => link.outstanding_tags.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(total, MAX_OUTGOING_DELIVERIES_PER_SESSION);
    fixture.stop(HANDLE);
    drop(
        fixture
            .queue(target, Message::data(vec![1]), b"after-teardown")
            .await,
    );
    assert_eq!(fixture.link(target).outstanding_tags.len(), 2);
}
