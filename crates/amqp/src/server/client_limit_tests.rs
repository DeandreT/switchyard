//! Actual Begin/Attach/Transfer wire checks for the feature client's limit.
use super::*;
use crate::test_deadline::WAIT;
use tokio::{
    io::{AsyncWriteExt, DuplexStream},
    task::JoinHandle,
    time::timeout,
};

async fn control(peer: &mut DuplexStream) -> (u16, Performative) {
    loop {
        let Frame::Amqp {
            channel,
            performative: Some(value),
            ..
        } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
        else {
            panic!("control frame")
        };
        // A prior message can legitimately release a batch of session slots.
        if matches!(&value, Performative::Flow(flow) if flow.handle.is_none()) {
            continue;
        }
        return (channel, value);
    }
}

struct Fixture {
    connection: ClientConnection,
    session: ClientSession,
    peer: DuplexStream,
    driver: JoinHandle<Result<(), EngineError>>,
}

impl Fixture {
    async fn new() -> Self {
        let (stream, mut peer) = tokio::io::duplex(64 * 1024);
        let (commands, command_rx) = mpsc::channel(256);
        let (closed_tx, closed) = watch::channel(false);
        let driver = tokio::spawn(run_client(stream, 256 * 1024, command_rx, closed_tx));
        let mut connection = ClientConnection { commands, closed };
        let (session, ()) = timeout(WAIT, async {
            tokio::join!(connection.begin(), async {
                let (channel, begin) = control(&mut peer).await;
                assert!(matches!(begin, Performative::Begin(_)));
                write_amqp(
                    &mut peer,
                    channel,
                    Performative::Begin(Begin {
                        remote_channel: Some(channel),
                        ..Begin::default()
                    }),
                    vec![],
                )
                .await
                .unwrap();
            })
        })
        .await
        .unwrap();
        Self {
            connection,
            session: session.unwrap(),
            peer,
            driver,
        }
    }

    async fn attach(
        &mut self,
        name: &str,
        requested: Option<Option<u64>>,
        expected: u64,
    ) -> ClientReceiver {
        let session = &mut self.session;
        let peer = &mut self.peer;
        let (receiver, ()) = timeout(WAIT, async {
            tokio::join!(
                async {
                    match requested {
                        None => session.attach_receiver(name, "orders").await,
                        Some(limit) => {
                            ClientReceiver::builder()
                                .name(name)
                                .source("orders")
                                .max_message_size(limit)
                                .attach(session)
                                .await
                        }
                    }
                },
                async {
                    let (channel, attach) = control(peer).await;
                    let Performative::Attach(mut attach) = attach else {
                        panic!("client Attach")
                    };
                    assert_eq!(attach.role, Role::Receiver);
                    assert_eq!(attach.max_message_size, Some(expected), "{name}");
                    let handle = attach.handle;
                    attach.role = Role::Sender;
                    attach.initial_delivery_count = Some(0);
                    write_amqp(peer, channel, Performative::Attach(attach), vec![])
                        .await
                        .unwrap();
                    let (_, flow) = control(peer).await;
                    let Performative::Flow(flow) = flow else {
                        panic!("client receive credit")
                    };
                    assert_eq!(flow.handle, Some(handle));
                }
            )
        })
        .await
        .unwrap();
        receiver.unwrap()
    }

    async fn send(&mut self, handle: u32, id: u32, size: usize) {
        let payload = encode_message(&Message::data(vec![0x5a; size - 8])).unwrap();
        assert_eq!(payload.len(), size);
        let chunks = payload.chunks(128 * 1024).collect::<Vec<_>>();
        for (index, payload) in chunks.iter().enumerate() {
            timeout(
                WAIT,
                write_amqp(
                    &mut self.peer,
                    self.session.channel,
                    Performative::Transfer(Transfer {
                        handle,
                        delivery_id: (index == 0).then_some(id),
                        delivery_tag: (index == 0)
                            .then(|| DeliveryTag::from(id.to_be_bytes().to_vec())),
                        message_format: (index == 0).then_some(0),
                        settled: (index == 0).then_some(true),
                        more: index + 1 != chunks.len(),
                        rcv_settle_mode: None,
                        state: None,
                        resume: false,
                        aborted: false,
                        batchable: false,
                    }),
                    payload.to_vec(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        }
    }

    async fn finish(mut self) {
        let connection = &mut self.connection;
        let peer = &mut self.peer;
        let (closed, ()) = timeout(WAIT, async {
            tokio::join!(connection.close(), async {
                assert!(matches!(control(peer).await.1, Performative::Close(_)));
                write_amqp(peer, 0, Performative::Close(Close::default()), vec![])
                    .await
                    .unwrap();
            })
        })
        .await
        .unwrap();
        closed.unwrap();
        timeout(WAIT, self.driver).await.unwrap().unwrap().unwrap();
        // The helper retains no reader join capability. Release the peer's
        // write side and witness IO closure, without claiming a reader join.
        self.peer.shutdown().await.unwrap();
        assert!(
            timeout(WAIT, read_frame(&mut self.peer))
                .await
                .unwrap()
                .is_err()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn advertised_default_omitted_oversized_smaller_and_zero_limits_match_body_enforcement() {
    let maximum = engine::incoming::MAX_MESSAGE;
    let mut fixture = Fixture::new().await;
    for (index, (name, requested, effective)) in [
        ("default", None, maximum),
        ("omitted", Some(None), maximum),
        ("oversized", Some(Some(maximum + 4096)), maximum),
        ("smaller", Some(Some(1024 * 1024)), 1024 * 1024),
        ("zero", Some(Some(0)), maximum),
    ]
    .into_iter()
    .enumerate()
    {
        let mut receiver = fixture.attach(name, requested, effective).await;
        fixture
            .send(receiver.handle, index as u32 * 2, effective as usize)
            .await;
        let delivery = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
        assert_eq!(
            delivery.encoded_message().len(),
            effective as usize,
            "{name}"
        );
        assert_eq!(delivery.encoded_message().last(), Some(&0x5a));
        drop(delivery);
        fixture
            .send(
                receiver.handle,
                index as u32 * 2 + 1,
                effective as usize + 1,
            )
            .await;
        let (channel, detach) = control(&mut fixture.peer).await;
        let Performative::Detach(detach) = detach else {
            panic!("oversize Detach for {name}")
        };
        assert_eq!(channel, fixture.session.channel);
        assert_eq!(detach.handle, receiver.handle);
        assert_eq!(
            detach.error.unwrap().condition.as_symbol(),
            Symbol::from(super::super::MESSAGE_SIZE_EXCEEDED_CONDITION)
        );
        assert!(matches!(
            timeout(WAIT, receiver.recv()).await.unwrap(),
            Err(EngineError::RemoteDetached)
        ));
    }
    fixture.finish().await;
}

fn incoming_transfer(handle: u32, id: u32) -> Performative {
    Performative::Transfer(Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(DeliveryTag::from(id.to_be_bytes().to_vec())),
        message_format: Some(0),
        settled: Some(true),
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    })
}

async fn pending<F: std::future::Future>(mut future: std::pin::Pin<&mut F>) {
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
            .await
            .is_pending()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn window_retirement_completes_pending_close_and_attach_without_retiring_another_channel() {
    let mut f = Fixture::new().await;
    let mut stalled = f.attach("stalled", Some(Some(4096)), 4096).await;
    let channel = f.session.channel;
    let (healthy, ()) = timeout(WAIT, async {
        tokio::join!(f.connection.begin(), async {
            let (channel, begin) = control(&mut f.peer).await;
            assert!(matches!(begin, Performative::Begin(_)));
            write_amqp(
                &mut f.peer,
                channel,
                Performative::Begin(Begin {
                    remote_channel: Some(channel),
                    ..Begin::default()
                }),
                vec![],
            )
            .await
            .unwrap();
        })
    })
    .await
    .unwrap();
    let mut healthy = healthy.unwrap();
    let healthy_channel = healthy.channel;
    for id in 0..SESSION_WINDOW {
        timeout(
            WAIT,
            write_amqp(
                &mut f.peer,
                channel,
                incoming_transfer(stalled.handle, id),
                vec![],
            ),
        )
        .await
        .unwrap()
        .unwrap();
    }
    let mut closing = Box::pin(stalled.close());
    pending(closing.as_mut()).await;
    let (owner, detach) = control(&mut f.peer).await;
    let Performative::Detach(detach) = detach else {
        panic!("original close is awaiting its Detach acknowledgement")
    };
    assert_eq!(owner, channel);
    assert_eq!(detach.handle, stalled.handle);

    let mut retiring_attach = Box::pin(f.session.attach_receiver("same-name", "orders"));
    pending(retiring_attach.as_mut()).await;
    let (owner, retired_offer) = control(&mut f.peer).await;
    let Performative::Attach(mut retired_offer) = retired_offer else {
        panic!("pending retiring Attach")
    };
    assert_eq!(owner, channel);
    let mut healthy_attach = Box::pin(healthy.attach_receiver("same-name", "orders"));
    pending(healthy_attach.as_mut()).await;
    let (owner, healthy_offer) = control(&mut f.peer).await;
    let Performative::Attach(mut healthy_offer) = healthy_offer else {
        panic!("pending independent Attach")
    };
    assert_eq!(owner, healthy_channel);
    pending(retiring_attach.as_mut()).await;
    pending(closing.as_mut()).await;

    write_amqp(
        &mut f.peer,
        channel,
        incoming_transfer(stalled.handle, SESSION_WINDOW),
        vec![],
    )
    .await
    .unwrap();
    let (owner, end) = control(&mut f.peer).await;
    let Performative::End(end) = end else {
        panic!("window violation End")
    };
    assert_eq!(owner, channel);
    assert_eq!(
        end.error.unwrap().condition.as_symbol(),
        Symbol::from("amqp:session:window-violation")
    );
    // Both original replies terminate before either crossed acknowledgement.
    assert!(matches!(
        timeout(WAIT, closing.as_mut()).await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(WAIT, retiring_attach.as_mut()).await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    drop(closing);
    drop(retiring_attach);
    pending(healthy_attach.as_mut()).await;

    write_amqp(&mut f.peer, channel, Performative::Detach(detach), vec![])
        .await
        .unwrap();
    retired_offer.role = Role::Sender;
    retired_offer.initial_delivery_count = Some(0);
    write_amqp(
        &mut f.peer,
        channel,
        Performative::Attach(retired_offer),
        vec![],
    )
    .await
    .unwrap();
    write_amqp(
        &mut f.peer,
        channel,
        Performative::End(End::default()),
        vec![],
    )
    .await
    .unwrap();
    healthy_offer.role = Role::Sender;
    healthy_offer.initial_delivery_count = Some(0);
    write_amqp(
        &mut f.peer,
        healthy_channel,
        Performative::Attach(healthy_offer),
        vec![],
    )
    .await
    .unwrap();
    assert!(
        matches!(control(&mut f.peer).await, (owner, Performative::Flow(_)) if owner == healthy_channel)
    );
    let mut active = timeout(WAIT, healthy_attach.as_mut())
        .await
        .unwrap()
        .unwrap();
    drop(healthy_attach);
    assert!(!*f.connection.closed.borrow());
    // Release one accepted old delivery, not a fake broker completion, to make
    // room in the shared count budget for the independent session's delivery.
    assert!(stalled.recv().await.unwrap().encoded_message().is_empty());
    let encoded = encode_message(&Message::data(vec![9])).unwrap();
    write_amqp(
        &mut f.peer,
        healthy_channel,
        incoming_transfer(active.handle, 0),
        encoded.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        timeout(WAIT, active.recv())
            .await
            .unwrap()
            .unwrap()
            .encoded_message(),
        encoded
    );
    assert!(matches!(
        timeout(WAIT, f.session.attach_receiver("late", "orders"))
            .await
            .unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(WAIT, stalled.close()).await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    f.finish().await;
}

#[tokio::test]
async fn retirement_drains_only_its_pending_begin_attach_and_detach_replies_once() {
    let mut begins = PendingBegins::new();
    let mut attaches = PendingAttaches::new();
    let mut detaches = PendingDetaches::new();
    let mut replies = Vec::new();
    for channel in [1, 2] {
        let (reply, begin) = oneshot::channel();
        begins.insert(channel, reply);
        let (reply, attach) = oneshot::channel();
        attaches.insert(
            (channel, String::from("same-name")),
            PendingAttach {
                handle: 7,
                incarnation: 42,
                reply,
            },
        );
        let (reply, detach) = oneshot::channel();
        detaches.insert((channel, 7), reply);
        replies.push((begin, attach, detach));
    }
    retire_pending_operations(1, &mut begins, &mut attaches, &mut detaches);
    retire_pending_operations(1, &mut begins, &mut attaches, &mut detaches);
    let (begin, attach, detach) = replies.remove(0);
    assert!(matches!(
        begin.await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        attach.await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        detach.await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    let (mut begin, mut attach, mut detach) = replies.remove(0);
    assert!(matches!(
        begin.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        attach.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        detach.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    begins.remove(&2).unwrap().send(Ok(2)).unwrap();
    assert_eq!(begin.await.unwrap().unwrap(), 2);
    assert!(attaches.contains_key(&(2, String::from("same-name"))));
    assert!(detaches.contains_key(&(2, 7)));
}
