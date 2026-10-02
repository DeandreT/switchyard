use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use super::*;
use crate::server::content_budget::ContentBudget;
use crate::{Source, Target};

const CHANNEL: u16 = 3;
const PEER_CHANNEL: u16 = 19;
const CONTROL: u32 = 11;
const POST: u32 = 17;
const CUSTOM_FORMAT: u32 = u32::MAX - 1;

#[derive(Clone, Default)]
struct RecordedWriter(Arc<Mutex<Vec<u8>>>);

impl RecordedWriter {
    fn len(&self) -> usize {
        self.0.lock().expect("recorded output").len()
    }
}

impl AsyncWrite for RecordedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut output = self.0.lock().expect("recorded output");
        assert!(output.len() + bytes.len() <= 8192, "bounded fixture output");
        output.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    book: NativeTransactionBook,
    sessions: HashMap<u16, SessionState>,
    connection: NativeConnectionIdentity,
    writer: FrameWriter<RecordedWriter>,
    output: RecordedWriter,
    budget: ContentBudget,
    commands: Option<mpsc::Receiver<Command>>,
    incoming_sessions: mpsc::Sender<IncomingSession>,
    _exit: connection_identity::ConnectionActorExit,
}

impl Fixture {
    fn new() -> (Self, ServerSession) {
        let connection = NativeConnectionIdentity::new();
        let (terminated, _) = watch::channel(false);
        let exit = connection_identity::ConnectionActorExit::new(connection.clone(), terminated);
        let (commands_tx, commands) = mpsc::channel(4);
        let (attach_tx, incoming_attaches) = mpsc::channel(4);
        let (incoming_sessions, _) = mpsc::channel(1);
        let consumed = Arc::new(Notify::new());
        let mut session = SessionState::for_connection(&Begin::default(), &connection);
        session.local_begin_sent = true;
        session.peer_channel = Some(PEER_CHANNEL);
        session.attach_tx = Some(attach_tx);
        let api = ServerSession {
            channel: CHANNEL,
            identity: session.identity.clone(),
            commands: commands_tx,
            incoming_attaches,
            consumed,
        };
        let budget = ContentBudget::new(4096);
        let output = RecordedWriter::default();
        (
            Self {
                book: NativeTransactionBook::new(&connection, NativeIngressPolicy::Posting),
                sessions: HashMap::from([(CHANNEL, session)]),
                connection,
                writer: FrameWriter::new_with_content_budget(output.clone(), 512, budget.clone())
                    .expect("bounded writer"),
                output,
                budget,
                commands: Some(commands),
                incoming_sessions,
                _exit: exit,
            },
            api,
        )
    }

    async fn frame(&mut self, performative: Performative, payload: Vec<u8>) {
        let result = handle_frame_scoped(
            Frame::Amqp {
                channel: PEER_CHANNEL,
                performative: Some(performative),
                payload,
            },
            &mut self.writer,
            &self.incoming_sessions,
            &mut self.sessions,
            512,
            u16::MAX,
            false,
            ConnectionScope::Native(&self.connection),
            &mut self.book,
        )
        .await
        .expect("actual actor frame handling");
        assert!(matches!(result, FrameAction::Continue));
    }

    async fn attach(&mut self, api: &mut ServerSession, request: Attach) -> IncomingAttach {
        self.frame(Performative::Attach(Box::new(request)), Vec::new())
            .await;
        api.incoming_attaches
            .try_recv()
            .expect("actor-approved attach")
    }

    async fn drive<T>(&mut self, future: impl Future<Output = Result<T, EngineError>>) -> T {
        let mut future = Box::pin(future);
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let Command::NativeTransactions(command) = self
            .commands
            .as_mut()
            .expect("command receiver")
            .try_recv()
            .expect("native request")
        else {
            panic!("native request");
        };
        handle_native_command(
            command,
            &mut self.book,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("native acceptance or provisional flush");
        future.await.expect("actual native reply")
    }

    async fn receiver(
        &mut self,
        api: &mut ServerSession,
        handle: u32,
        decoders: MessageFormatDecoders,
    ) -> TransactionalReceiver {
        let attach = self.attach(api, ordinary_attach(handle)).await;
        self.drive(api.accept_transactional_receiver_with_decoders(attach, 512, decoders))
            .await
    }

    async fn declaration(
        &mut self,
        api: &mut ServerSession,
    ) -> (CoordinatorEndpoint, NativeTransactionIdentity) {
        let attach = self.attach(api, coordinator_attach()).await;
        let mut coordinator = self.drive(api.accept_coordinator(attach, 4096)).await;
        let message = Message {
            body: Body::Value(crate::Value::from(TransactionCommand::Declare(
                crate::Declare { global_id: None },
            ))),
            ..Message::default()
        };
        self.frame(
            Performative::Transfer(transfer(CONTROL, 0, 0, None)),
            encode_message(&message).expect("Declare encoding"),
        )
        .await;
        let CoordinatorRequest::Declare(receipt) =
            coordinator.recv().await.expect("actual Declare receipt")
        else {
            panic!("Declare receipt");
        };
        let identity = self
            .drive(receipt.declared(TransactionId::new([42]).expect("bounded transaction ID")))
            .await;
        (coordinator, identity)
    }

    async fn posting(
        &mut self,
        receiver: &mut TransactionalReceiver,
        transaction: &NativeTransactionIdentity,
        format: u32,
        payload: Vec<u8>,
    ) -> TransactionPostingReceipt {
        self.frame(
            Performative::Transfer(transfer(
                POST,
                1,
                format,
                Some(transaction.transaction_id().clone()),
            )),
            payload,
        )
        .await;
        let TransactionalIngress::Posting(posting) =
            receiver.recv().await.expect("actual posting receipt")
        else {
            panic!("transactional posting");
        };
        posting
    }
}

fn ordinary_attach(handle: u32) -> Attach {
    Attach {
        name: format!("private-receiver-{handle}"),
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("private-queue")),
        target: Some(Target::new("private-queue").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn coordinator_attach() -> Attach {
    Attach {
        name: "private-controller".into(),
        source: None,
        target: Some(crate::Coordinator::default().into()),
        ..ordinary_attach(CONTROL)
    }
}

fn transfer(handle: u32, id: u32, format: u32, transaction: Option<TransactionId>) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(vec![id as u8].into()),
        message_format: Some(format),
        settled: Some(false),
        more: false,
        rcv_settle_mode: None,
        state: transaction.map(|txn_id| {
            DeliveryState::Transactional(TransactionalState {
                txn_id,
                outcome: None,
            })
        }),
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn custom_decode(bytes: &[u8]) -> io::Result<Message> {
    Ok(Message::data(bytes.to_vec()))
}

fn custom_decoders() -> MessageFormatDecoders {
    MessageFormatDecoders::default()
        .with_decoder(CUSTOM_FORMAT, custom_decode)
        .expect("bounded custom registry")
}

#[test]
fn test_only_unbound_identity_fails_closed_without_a_connection_proof() {
    let owner = LinkIdentity::new();
    let identity = receiver_identity::NativeReceiverIdentity::for_accepted_receiver(&owner);
    let mut ledger = IncomingLedger::default();
    let delivery = ledger
        .reserve(&owner, 7, b"same-id")
        .expect("private unbound negative fixture");
    assert!(!owner.is_retired());
    assert!(!identity.is_active());
    assert!(identity.connection_identity().is_none());
    assert!(!identity.owns_delivery(&delivery));
    assert!(identity.same_receiver(&identity.clone()));
    drop(identity.clone());
    assert!(
        !owner.is_retired(),
        "observer destruction does not retire a link"
    );
    assert!(!format!("{identity:?}").contains("same-id"));
}

#[tokio::test]
async fn accepted_origin_is_exact_and_actor_retirement_faults_before_reuse() {
    let (mut fixture, mut api) = Fixture::new();
    let mut receiver = fixture
        .receiver(&mut api, POST, MessageFormatDecoders::default())
        .await;
    let identity = receiver.receiver_identity();
    let copy = identity.clone();
    assert!(identity.is_active());
    assert!(identity.same_receiver(&copy));
    assert!(
        identity
            .connection_identity()
            .expect("bound connection")
            .same_connection(&fixture.connection)
    );
    let sibling = fixture
        .receiver(&mut api, POST + 1, MessageFormatDecoders::default())
        .await;
    assert!(!identity.same_receiver(&sibling.receiver_identity()));
    assert!(
        identity
            .connection_identity()
            .expect("bound connection")
            .same_connection(
                sibling
                    .receiver_identity()
                    .connection_identity()
                    .expect("sibling connection")
            )
    );
    let (mut foreign, mut foreign_api) = Fixture::new();
    let foreign_receiver = foreign
        .receiver(&mut foreign_api, POST, MessageFormatDecoders::default())
        .await;
    assert!(
        !identity.same_receiver(&foreign_receiver.receiver_identity()),
        "equal names and handles do not establish origin"
    );
    assert!(
        !identity
            .connection_identity()
            .expect("bound connection")
            .same_connection(
                foreign_receiver
                    .receiver_identity()
                    .connection_identity()
                    .expect("foreign connection")
            )
    );

    let (_coordinator, transaction) = fixture.declaration(&mut api).await;
    let posting = fixture
        .posting(
            &mut receiver,
            &transaction,
            0,
            encode_message(&Message::data(vec![5; 127])).expect("message encoding"),
        )
        .await;
    assert!(posting.belongs_to_receiver(&identity));
    assert!(!posting.belongs_to_receiver(&sibling.receiver_identity()));
    assert!(!posting.belongs_to_receiver(&foreign_receiver.receiver_identity()));
    fixture
        .frame(
            Performative::Detach(Detach {
                handle: POST,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await;
    assert_eq!(transaction.state(), NativeTransactionState::Faulted);
    assert!(!identity.is_active());
    assert!(
        identity.same_receiver(&copy),
        "retirement does not change exact provenance"
    );
    assert!(!posting.belongs_to_receiver(&identity));
    let replacement = fixture
        .receiver(&mut api, POST, MessageFormatDecoders::default())
        .await;
    assert!(replacement.receiver_identity().is_active());
    assert!(
        !identity.same_receiver(&replacement.receiver_identity()),
        "reused path and handle mint a new generation"
    );
    assert!(!posting.belongs_to_receiver(&replacement.receiver_identity()));
    drop(posting);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn observers_retain_neither_content_nor_command_senders_after_native_cleanup() {
    let (mut fixture, mut api) = Fixture::new();
    let mut receiver = fixture.receiver(&mut api, POST, custom_decoders()).await;
    let identity = receiver.receiver_identity();
    let copy = identity.clone();
    drop(identity.clone());
    assert!(identity.is_active());
    let (coordinator, transaction) = fixture.declaration(&mut api).await;
    let body = b"private-post-body";
    let posting = fixture
        .posting(&mut receiver, &transaction, CUSTOM_FORMAT, body.to_vec())
        .await;
    assert_eq!(posting.message(), &Message::data(body.to_vec()));
    assert_eq!(posting.message_format(), CUSTOM_FORMAT);
    assert!(posting.belongs_to_receiver(&identity));
    let retained = fixture.budget.retained_bytes();
    assert_eq!(retained, body.len());
    let prepared = fixture.drive(posting.provisional_accept()).await;
    assert!(prepared.belongs_to_receiver(&identity));
    fixture
        .frame(
            Performative::Disposition(Disposition {
                role: Role::Sender,
                first: 1,
                last: None,
                settled: true,
                state: None,
                batchable: false,
            }),
            Vec::new(),
        )
        .await;
    assert!(
        prepared.belongs_to_receiver(&identity),
        "transport settlement does not erase receiver origin"
    );
    assert_eq!(fixture.budget.retained_bytes(), retained);
    let debug = format!("{identity:?}");
    for private in [
        "private-post-body",
        "private-receiver",
        "private-queue",
        "private-controller",
    ] {
        assert!(!debug.contains(private));
    }
    let budget = fixture.budget.clone();
    drop(prepared);
    assert_eq!(
        budget.retained_bytes(),
        0,
        "only the receipt retains encoded content"
    );
    assert!(identity.is_active());
    let mut commands = fixture.commands.take().expect("command receiver");
    drop((receiver, coordinator, api, fixture));
    assert!(
        !identity.is_active(),
        "observer does not keep actor scope active"
    );
    assert!(identity.same_receiver(&copy));
    assert!(
        !identity
            .connection_identity()
            .expect("retained connection provenance")
            .is_active()
    );
    assert!(
        matches!(
            commands.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ),
        "observer retains no command sender"
    );
    assert_eq!(budget.retained_bytes(), 0);
}

#[tokio::test]
async fn coordinator_custom_decoder_guard_precedes_any_write_or_install() {
    let (mut fixture, mut api) = Fixture::new();
    let attach = fixture.attach(&mut api, coordinator_attach()).await;
    let owner = attach.approval().link_identity().clone();
    let (requests, _) = mpsc::channel(1);
    let (detached, _) = watch::channel(false);
    let result = accept_native_receiving(
        CHANNEL,
        api.identity.clone(),
        attach,
        4096,
        custom_decoders(),
        ReceivingSink::Coordinator(requests),
        detached,
        Arc::new(Consumption::new(api.consumed.clone())),
        &mut fixture.sessions,
        &mut fixture.writer,
    )
    .await;
    assert!(
        matches!(result, Err(EngineError::InvalidState(message)) if message == "custom message-format decoders require a transactional receiving endpoint")
    );
    assert_eq!(fixture.output.len(), 0);
    let session = fixture.sessions.get(&CHANNEL).expect("actual session");
    assert!(session.links.is_empty());
    assert_eq!(session.pending_attaches.len(), 1);
    assert!(!owner.is_retired());
    assert!(
        !session
            .handle_aliases
            .get(&CONTROL)
            .expect("pending exact alias")
            .own_attach_sent
    );

    let (mut healthy, mut healthy_api) = Fixture::new();
    let attach = healthy.attach(&mut healthy_api, coordinator_attach()).await;
    let _endpoint = healthy
        .drive(healthy_api.accept_coordinator(attach, 4096))
        .await;
    let Some(LinkState::Receiving(link)) = healthy
        .sessions
        .get(&CHANNEL)
        .expect("healthy session")
        .links
        .get(&CONTROL)
    else {
        panic!("installed coordinator");
    };
    assert!(link.decoders.is_default());
    assert!(matches!(&link.deliveries, ReceivingSink::Coordinator(_)));
    assert!(healthy.output.len() > 0);
}

#[tokio::test]
async fn immutable_approval_kind_cannot_be_reclassified_by_registry_or_mutated_target() {
    for original_coordinator in [false, true] {
        let (mut fixture, mut api) = Fixture::new();
        let request = if original_coordinator {
            coordinator_attach()
        } else {
            ordinary_attach(CONTROL)
        };
        let mut attach = fixture.attach(&mut api, request).await;
        let original = attach.approval().kind();
        let sink = if original_coordinator {
            attach.target = Some(Target::new("private-queue").into());
            let (sink, _) = mpsc::channel(1);
            ReceivingSink::Transactional(sink)
        } else {
            attach.target = Some(crate::Coordinator::default().into());
            let (sink, _) = mpsc::channel(1);
            ReceivingSink::Coordinator(sink)
        };
        assert_eq!(attach.approval().kind(), original);
        let (detached, _) = watch::channel(false);
        assert!(
            accept_native_receiving(
                CHANNEL,
                api.identity.clone(),
                attach,
                512,
                MessageFormatDecoders::default(),
                sink,
                detached,
                Arc::new(Consumption::new(api.consumed.clone())),
                &mut fixture.sessions,
                &mut fixture.writer,
            )
            .await
            .is_err()
        );
        assert_eq!(fixture.output.len(), 0);
        let session = fixture.sessions.get(&CHANNEL).expect("actual session");
        assert!(session.links.is_empty());
        assert_eq!(session.pending_attaches.len(), 1);
        assert!(
            !session
                .handle_aliases
                .get(&CONTROL)
                .expect("pending exact alias")
                .own_attach_sent
        );
    }
}
