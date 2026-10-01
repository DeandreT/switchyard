use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use super::*;
use crate::{Source, Target};

const CHANNEL: u16 = 3;
const HANDLE: u32 = 7;
const CHUNK: usize = 64 * 1024;

#[derive(Clone, Default)]
struct Writer(Arc<Mutex<Vec<u8>>>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .lock()
            .expect("captured bytes")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    output: Writer,
    writer: FrameWriter<Writer>,
    inboxes: HashMap<u32, mpsc::Receiver<Delivery>>,
}

impl Fixture {
    fn new() -> Self {
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        let output = Writer::default();
        Self {
            sessions: HashMap::from([(CHANNEL, session)]),
            writer: FrameWriter::new(output.clone(), 512).expect("writer"),
            output,
            inboxes: HashMap::new(),
        }
    }

    fn link(&self, handle: u32) -> &ReceivingLink {
        let LinkState::Receiving(link) = &self.sessions[&CHANNEL].links[&handle] else {
            panic!("receiving link")
        };
        link
    }

    async fn receiver(&mut self, handle: u32, maximum: u64) {
        let session = self.sessions.get_mut(&CHANNEL).expect("session");
        let receipt = IncomingAttach::new(
            Attach {
                name: format!("ceiling-{handle}"),
                handle,
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Mixed,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: Some(Source::new("queue")),
                target: Some(Target::new("queue")),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            },
            session.identity.clone(),
            handle,
        );
        session.handle_aliases.insert(
            handle,
            super::link_handles::HandleAlias {
                identity: receipt.approval().link_identity().clone(),
                peer_handle: Some(handle),
                own_attach_sent: false,
                error_detached: false,
            },
        );
        session
            .pending_attaches
            .insert(handle, PendingLinkFlow::incoming(&receipt));
        let (deliveries_tx, inbox) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        self.inboxes.insert(handle, inbox);
        let (detached_tx, _) = watch::channel(false);
        let (reply, result) = oneshot::channel();
        let command = Command::AcceptLink {
            channel: CHANNEL,
            session: session.identity.clone(),
            attach: Box::new(receipt),
            max_message_size: maximum,
            properties: None,
            decoders: MessageFormatDecoders::default(),
            deliveries_tx,
            detached_tx,
            consumption: Arc::new(Consumption::new(Arc::new(Notify::new()))),
            reply,
        };
        assert!(matches!(
            handle_command(command, &mut self.writer, &mut self.sessions, 512)
                .await
                .expect("approval"),
            CommandAction::Continue
        ));
        result
            .await
            .expect("approval reply")
            .expect("receiver approved");
    }

    async fn receive(&mut self, transfer: Transfer, bytes: Vec<u8>) {
        receive_transfer(
            CHANNEL,
            transfer,
            bytes,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("delivery or link-local refusal");
    }

    async fn message(&mut self, handle: u32, id: u32, encoded: &[u8]) {
        if encoded.is_empty() {
            self.receive(first(handle, id, false), Vec::new()).await;
            return;
        }
        let count = encoded.len().div_ceil(CHUNK);
        for (index, bytes) in encoded.chunks(CHUNK).enumerate() {
            let more = index + 1 != count;
            let transfer = if index == 0 {
                first(handle, id, more)
            } else {
                continuation(handle, more)
            };
            self.receive(transfer, bytes.to_vec()).await;
        }
    }

    fn delivered(&mut self, handle: u32) -> Delivery {
        self.inboxes
            .get_mut(&handle)
            .expect("inbox")
            .try_recv()
            .expect("complete delivery")
    }

    fn clear_output(&self) {
        self.output.0.lock().expect("captured bytes").clear();
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = self.output.0.lock().expect("captured bytes").clone();
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            frames.push(read_frame(&mut input).await.expect("complete frame"));
        }
        frames
    }

    async fn assert_refused(&mut self, handle: u32) {
        assert!(!self.sessions[&CHANNEL].links.contains_key(&handle));
        assert!(self.sessions[&CHANNEL].closing_handles.contains(&handle));
        assert!(!self.sessions[&CHANNEL].ending);
        assert!(
            self.inboxes
                .get_mut(&handle)
                .expect("inbox")
                .try_recv()
                .is_err()
        );
        let frames = self.frames().await;
        assert_eq!(frames.len(), 1);
        let Frame::Amqp {
            channel: CHANNEL,
            performative: Some(Performative::Detach(detach)),
            payload,
        } = &frames[0]
        else {
            panic!("only offending link is detached")
        };
        assert_eq!(detach.handle, handle);
        assert!(detach.closed);
        assert!(payload.is_empty());
        assert_eq!(
            detach
                .error
                .as_ref()
                .expect("size error")
                .condition
                .as_symbol(),
            Symbol::from("amqp:link:message-size-exceeded")
        );
    }
}

fn first(handle: u32, id: u32, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
        message_format: Some(0),
        settled: Some(false),
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn continuation(handle: u32, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn boundary_message(extra: usize) -> (Message, Vec<u8>) {
    let message = Message::data(vec![9; MAX_RECEIVED_MESSAGE_BYTES as usize - 8 + extra]);
    let encoded = encode_message(&message).expect("boundary encoding");
    assert_eq!(encoded.len(), MAX_RECEIVED_MESSAGE_BYTES as usize + extra);
    (message, encoded)
}

#[test]
fn local_receive_limit_defaults_and_clamps_without_raising_smaller_positive_requests() {
    for (request, expected) in [
        (None, MAX_RECEIVED_MESSAGE_BYTES),
        (Some(0), MAX_RECEIVED_MESSAGE_BYTES),
        (Some(1), 1),
        (Some(1024), 1024),
        (
            Some(MAX_RECEIVED_MESSAGE_BYTES - 1),
            MAX_RECEIVED_MESSAGE_BYTES - 1,
        ),
        (Some(MAX_RECEIVED_MESSAGE_BYTES), MAX_RECEIVED_MESSAGE_BYTES),
        (
            Some(MAX_RECEIVED_MESSAGE_BYTES + 1),
            MAX_RECEIVED_MESSAGE_BYTES,
        ),
        (Some(u64::MAX), MAX_RECEIVED_MESSAGE_BYTES),
    ] {
        assert_eq!(effective_receive_maximum(request), expected);
    }
    assert_eq!(normalized_message_size(None), None);
    assert_eq!(normalized_message_size(Some(0)), None);
    assert_eq!(normalized_message_size(Some(u64::MAX)), Some(u64::MAX));
}

#[tokio::test]
async fn server_receiver_advertisement_matches_its_actual_effective_limit() {
    for maximum in [
        0,
        1,
        1024,
        MAX_RECEIVED_MESSAGE_BYTES - 1,
        MAX_RECEIVED_MESSAGE_BYTES,
        MAX_RECEIVED_MESSAGE_BYTES + 1,
        u64::MAX,
    ] {
        let mut fixture = Fixture::new();
        fixture.receiver(HANDLE, maximum).await;
        let expected = effective_receive_maximum(Some(maximum));
        assert_eq!(fixture.link(HANDLE).max_message_size, expected);
        let frames = fixture.frames().await;
        let Frame::Amqp {
            performative: Some(Performative::Attach(attach)),
            ..
        } = &frames[0]
        else {
            panic!("receiver Attach")
        };
        assert_eq!(attach.role, Role::Receiver);
        assert_eq!(attach.max_message_size, Some(expected));
        assert!(
            matches!(&frames[1], Frame::Amqp { performative: Some(Performative::Flow(flow)), .. } if flow.handle == Some(HANDLE) && flow.link_credit == Some(LINK_CREDIT))
        );
        assert_eq!(frames.len(), 2);
    }
}

#[tokio::test]
async fn exact_local_ceiling_accepts_a_complete_fragmented_standard_message() {
    let mut fixture = Fixture::new();
    fixture.receiver(HANDLE, 0).await;
    fixture.clear_output();
    let (message, encoded) = boundary_message(0);
    let count = encoded.len().div_ceil(CHUNK);
    for (index, bytes) in encoded.chunks(CHUNK).enumerate() {
        let more = index + 1 != count;
        fixture
            .receive(
                if index == 0 {
                    first(HANDLE, 0, more)
                } else {
                    continuation(HANDLE, more)
                },
                bytes.to_vec(),
            )
            .await;
        if more {
            assert!(matches!(
                fixture.inboxes.get_mut(&HANDLE).expect("inbox").try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert_eq!(
                fixture
                    .link(HANDLE)
                    .partial
                    .as_ref()
                    .expect("partial")
                    .bytes
                    .len(),
                (index + 1) * CHUNK
            );
        }
    }
    assert_eq!(fixture.delivered(HANDLE).message, message);
    assert!(fixture.link(HANDLE).partial.is_none());
    assert_eq!(
        fixture.sessions[&CHANNEL].flow.snapshot().next_incoming_id as usize,
        count
    );
    assert!(fixture.frames().await.is_empty());
}

#[tokio::test]
async fn one_byte_beyond_ceiling_is_refused_before_copying_and_releases_its_alias_for_sibling() {
    let mut fixture = Fixture::new();
    fixture.receiver(HANDLE, u64::MAX).await;
    fixture.receiver(HANDLE + 1, 64).await;
    let owner = fixture.link(HANDLE).identity.clone();
    fixture.clear_output();
    let (_, encoded) = boundary_message(1);
    let maximum = MAX_RECEIVED_MESSAGE_BYTES as usize;
    for (index, bytes) in encoded[..maximum].chunks(CHUNK).enumerate() {
        fixture
            .receive(
                if index == 0 {
                    first(HANDLE, 0, true)
                } else {
                    continuation(HANDLE, true)
                },
                bytes.to_vec(),
            )
            .await;
    }
    assert_eq!(
        fixture
            .link(HANDLE)
            .partial
            .as_ref()
            .expect("exact partial boundary")
            .bytes
            .len(),
        maximum
    );
    fixture
        .receive(continuation(HANDLE, false), encoded[maximum..].to_vec())
        .await;
    fixture.assert_refused(HANDLE).await;
    assert!(owner.is_retired());
    assert!(!fixture.link(HANDLE + 1).identity.is_retired());
    let healthy = Message::data(vec![7; 3]);
    fixture
        .message(
            HANDLE + 1,
            0,
            &encode_message(&healthy).expect("healthy encoding"),
        )
        .await;
    assert_eq!(fixture.delivered(HANDLE + 1).message, healthy);
    assert_eq!(fixture.frames().await.len(), 1);
}

#[tokio::test]
async fn endlessly_incomplete_fragments_hit_the_ceiling_without_decoding_or_delivery() {
    let mut fixture = Fixture::new();
    fixture.receiver(HANDLE, 0).await;
    fixture.clear_output();
    for index in 0..MAX_RECEIVED_MESSAGE_BYTES as usize / CHUNK {
        fixture
            .receive(
                if index == 0 {
                    first(HANDLE, 0, true)
                } else {
                    continuation(HANDLE, true)
                },
                vec![0xff; CHUNK],
            )
            .await;
    }
    assert_eq!(
        fixture
            .link(HANDLE)
            .partial
            .as_ref()
            .expect("bounded incomplete content")
            .bytes
            .len(),
        MAX_RECEIVED_MESSAGE_BYTES as usize
    );
    assert!(fixture.frames().await.is_empty());
    fixture
        .receive(continuation(HANDLE, true), vec![0xff])
        .await;
    fixture.assert_refused(HANDLE).await;
}

#[tokio::test]
async fn abort_at_ceiling_releases_partial_content_and_allows_same_id_and_tag_again() {
    let mut fixture = Fixture::new();
    fixture.receiver(HANDLE, 0).await;
    fixture.clear_output();
    for index in 0..MAX_RECEIVED_MESSAGE_BYTES as usize / CHUNK {
        fixture
            .receive(
                if index == 0 {
                    first(HANDLE, 0, true)
                } else {
                    continuation(HANDLE, true)
                },
                vec![0xff; CHUNK],
            )
            .await;
    }
    let old = fixture
        .link(HANDLE)
        .partial
        .as_ref()
        .expect("partial")
        .identity
        .clone();
    let mut aborted = continuation(HANDLE, false);
    aborted.aborted = true;
    fixture.receive(aborted, Vec::new()).await;
    assert!(fixture.link(HANDLE).partial.is_none());
    assert!(matches!(
        fixture.sessions[&CHANNEL].incoming.sender_is_settled(&old),
        Err(IncomingLedgerError::AbortedDelivery)
    ));
    let message = Message::data(vec![8; 3]);
    fixture
        .message(HANDLE, 0, &encode_message(&message).expect("encoding"))
        .await;
    assert_eq!(fixture.delivered(HANDLE).message, message);
    assert!(!fixture.link(HANDLE).identity.is_retired());
    assert!(fixture.frames().await.iter().all(|frame| matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Flow(_)),
            ..
        }
    )));
}

#[tokio::test]
async fn smaller_positive_limit_is_exact_and_tiny_limit_still_allows_an_empty_message() {
    for maximum in [1, 64] {
        let mut fixture = Fixture::new();
        fixture.receiver(HANDLE, maximum).await;
        fixture.clear_output();
        let message = if maximum == 1 {
            Message::default()
        } else {
            Message::data(vec![1; 59])
        };
        let encoded = encode_message(&message).expect("encoding");
        assert_eq!(
            encoded.len(),
            if maximum == 1 { 0 } else { maximum as usize }
        );
        fixture.message(HANDLE, 0, &encoded).await;
        assert_eq!(fixture.delivered(HANDLE).message, message);
        assert!(fixture.frames().await.is_empty());
        fixture
            .receive(first(HANDLE, 1, false), vec![0xff; maximum as usize + 1])
            .await;
        fixture.assert_refused(HANDLE).await;
    }
}
