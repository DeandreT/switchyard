use std::{
    future::poll_fn,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use super::*;

pub(super) const ACTOR_CHANNEL: u16 = 3;
pub(super) const ACTOR_HANDLE: u32 = 7;
pub(super) const REUSED_ID: u32 = 11;

#[derive(Default)]
pub(super) struct Output {
    bytes: Mutex<Vec<u8>>,
    blocked: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Output {
    pub(super) fn block_final_transfer(&self) {
        self.blocked.store(true, Ordering::Release);
    }

    pub(super) fn release(&self) {
        self.blocked.store(false, Ordering::Release);
        if let Some(waker) = self.waker.lock().expect("flush waker").take() {
            waker.wake();
        }
    }

    pub(super) async fn frames(&self) -> Vec<Frame> {
        let bytes = self.bytes.lock().expect("captured frames").clone();
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            frames.push(
                read_frame(&mut input)
                    .await
                    .expect("complete captured frame"),
            );
        }
        frames
    }

    pub(super) fn clear(&self) {
        self.bytes.lock().expect("captured frames").clear();
    }
}

pub(super) struct Writer(Arc<Output>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .bytes
            .lock()
            .expect("captured frame write")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.blocked.load(Ordering::Acquire) {
            let bytes = self.0.bytes.lock().expect("captured final frame");
            assert!(matches!(
                crate::codec::decode_frame_for_test(&bytes).expect("typed final frame"),
                Frame::Amqp {
                    channel: ACTOR_CHANNEL,
                    performative: Some(Performative::Transfer(Transfer { more: false, .. })),
                    ..
                }
            ));
            *self.0.waker.lock().expect("flush waker") = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub(super) struct ActorHarness {
    pub(super) owner: LinkIdentity,
    pub(super) sessions: HashMap<u16, SessionState>,
    pub(super) writer: FrameWriter<Writer>,
    pub(super) output: Arc<Output>,
}

impl ActorHarness {
    // This isolated helper ledger shares only the real socket sender's origin;
    // all original delivery generations are minted by production send_fragment.
    pub(super) fn from_sender(sender: &Sender) -> Self {
        let owner = sender.identity.clone();
        let mut credit = LinkCredit::new(0);
        credit
            .update_peer(Some(0), 100, false)
            .expect("helper peer credit");
        let (detached, _) = watch::channel(false);
        let link = SendingLink {
            identity: owner.clone(),
            auto_acknowledge: false,
            max_message_size: None,
            receiver_settle_mode: ReceiverSettleMode::Second,
            default_outcome: None,
            outstanding_tags: HashSet::new(),
            settle_mode: SenderSettleMode::Unsettled,
            credit,
            reservations: Default::default(),
            queued: VecDeque::new(),
            active: None,
            unsettled: HashMap::new(),
            pending_acknowledgements: HashMap::new(),
            detached,
        };
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        session.next_delivery_id = REUSED_ID;
        session
            .links
            .insert(ACTOR_HANDLE, LinkState::Sending(Box::new(link)));
        let output = Arc::new(Output::default());
        Self {
            owner,
            sessions: HashMap::from([(ACTOR_CHANNEL, session)]),
            writer: FrameWriter::new(Writer(Arc::clone(&output)), 512).expect("small-frame writer"),
            output,
        }
    }

    pub(super) fn link(&self) -> &SendingLink {
        let LinkState::Sending(link) = &self.sessions[&ACTOR_CHANNEL].links[&ACTOR_HANDLE] else {
            panic!("helper sending link");
        };
        link
    }

    pub(super) fn retained_bytes(&self) -> usize {
        self.writer.content_budget().retained_bytes()
    }

    pub(super) async fn enqueue(
        &mut self,
        message: Message,
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let (reply, result) = oneshot::channel();
        let command = Command::Send {
            channel: ACTOR_CHANNEL,
            handle: ACTOR_HANDLE,
            identity: self.owner.clone(),
            message: Box::new(message),
            delivery_tag: TAG.to_vec().into(),
            reply,
        };
        self.process(command).await;
        result
    }

    pub(super) async fn process(&mut self, command: Command) {
        let action = bounded(
            "bounded production helper command",
            handle_command(command, &mut self.writer, &mut self.sessions, 512),
        )
        .await
        .expect("helper command handled");
        assert!(matches!(action, CommandAction::Continue));
    }

    pub(super) async fn fragment(&mut self) {
        bounded(
            "bounded production Transfer fragment",
            send_fragment(
                ACTOR_CHANNEL,
                ACTOR_HANDLE,
                self.sessions
                    .get_mut(&ACTOR_CHANNEL)
                    .expect("helper session"),
                &mut self.writer,
            ),
        )
        .await
        .expect("fragment flushed");
    }

    pub(super) fn next_fragment_is_last(&self) -> bool {
        let active = self
            .link()
            .active
            .as_ref()
            .expect("active fragmented delivery");
        let (_, _, complete) = fragment_frame(
            ACTOR_CHANNEL,
            ACTOR_HANDLE,
            active.delivery_id,
            &active.delivery_tag,
            active.message_format,
            active.settled,
            &active.payload,
            active.offset,
            active.first_frame_sent,
            &self.writer,
        )
        .expect("bounded next fragment");
        complete
    }

    pub(super) async fn outcome(&mut self, settled: bool) {
        bounded(
            "bounded production receiver outcome",
            apply_disposition(
                ACTOR_CHANNEL,
                Disposition {
                    role: Role::Receiver,
                    first: REUSED_ID,
                    last: None,
                    settled,
                    state: Some(DeliveryState::Accepted(Accepted)),
                    batchable: false,
                },
                &mut self.writer,
                &mut self.sessions,
            ),
        )
        .await
        .expect("ordinary helper outcome");
    }

    pub(super) async fn settle(&mut self, acknowledgement: AckIdentity) {
        let (reply, result) = oneshot::channel();
        self.process(Command::SettleOutgoing {
            channel: ACTOR_CHANNEL,
            handle: ACTOR_HANDLE,
            owner: self.owner.clone(),
            identity: Some(acknowledgement),
            state: DeliveryState::Accepted(Accepted),
            reply,
        })
        .await;
        result
            .await
            .expect("helper acknowledgement reply")
            .expect("exact helper acknowledgement");
    }

    pub(super) fn reuse_id(&mut self) {
        assert!(self.link().active.is_none());
        assert!(self.link().unsettled.is_empty());
        assert!(self.link().pending_acknowledgements.is_empty());
        assert!(self.link().outstanding_tags.is_empty());
        self.sessions
            .get_mut(&ACTOR_CHANNEL)
            .expect("helper session")
            .next_delivery_id = REUSED_ID;
    }
}

pub(super) async fn assert_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
