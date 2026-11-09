//! Actual Begin/Attach/Transfer wire checks for the feature client's limit.
use super::*;
use crate::test_deadline::WAIT;
use tokio::{
    io::{AsyncWriteExt, DuplexStream},
    task::JoinHandle,
    time::timeout,
};

async fn control(peer: &mut (impl AsyncRead + Unpin)) -> (u16, Performative) {
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
    async fn seed_handle(&self, handle: u32) {
        timeout(
            WAIT,
            client_request(&self.connection.commands, |reply| {
                ClientCommand::SeedNextHandle {
                    channel: self.session.channel,
                    handle,
                    reply,
                }
            }),
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn deliver(&mut self, receiver: &mut ClientReceiver, id: u32) {
        let message = Message::data(id.to_be_bytes().to_vec());
        write_amqp(
            &mut self.peer,
            self.session.channel,
            incoming_transfer(receiver.handle, id),
            encode_message(&message).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            timeout(WAIT, receiver.recv())
                .await
                .unwrap()
                .unwrap()
                .message(),
            &message
        );
    }

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
        // The original driver now retains and joins its reader. Leave the peer
        // transport open: its EOF must come from native ownership release.
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

#[tokio::test(flavor = "current_thread")]
async fn automatic_refusal_answers_are_consumed_once_and_fence_crossed_traffic_and_reuse() {
    for cause in ["echo", "drain", "message", "resource"] {
        let mut f = Fixture::new().await;
        let mut healthy = f.attach("healthy", Some(Some(4096)), 4096).await;
        let maximum = if cause == "message" {
            8
        } else {
            engine::incoming::MAX_MESSAGE
        };
        let mut old = f.attach("retired", Some(Some(maximum)), maximum).await;
        let handle = old.handle;
        let channel = f.session.channel;
        let mut partials = Vec::new();
        if cause == "resource" {
            for index in 0..4 {
                let receiver = f.attach(&format!("partial-{index}"), None, maximum).await;
                let Performative::Transfer(mut transfer) =
                    incoming_transfer(receiver.handle, index)
                else {
                    unreachable!()
                };
                transfer.more = true;
                write_amqp(
                    &mut f.peer,
                    channel,
                    Performative::Transfer(transfer),
                    vec![0],
                )
                .await
                .unwrap();
                partials.push(receiver);
            }
        }
        if cause == "echo" || cause == "drain" {
            write_amqp(
                &mut f.peer,
                channel,
                Performative::Flow(Flow {
                    handle: Some(handle),
                    echo: cause == "echo",
                    drain: cause == "drain",
                    incoming_window: SESSION_WINDOW,
                    ..Flow::default()
                }),
                vec![],
            )
            .await
            .unwrap();
        } else {
            let encoded = encode_message(&Message::data(vec![1; 4])).unwrap();
            assert_eq!(encoded.len(), 9);
            write_amqp(&mut f.peer, channel, incoming_transfer(handle, 0), encoded)
                .await
                .unwrap();
        }
        let (_, refusal) = control(&mut f.peer).await;
        let Performative::Detach(refusal) = refusal else {
            panic!("automatic {cause} refusal")
        };
        assert_eq!(refusal.handle, handle);
        assert_eq!(
            refusal.error.unwrap().condition.as_symbol().to_string(),
            match cause {
                "message" => "amqp:link:message-size-exceeded",
                "resource" => "amqp:resource-limit-exceeded",
                _ => "amqp:not-implemented",
            }
        );
        assert!(matches!(old.recv().await, Err(EngineError::RemoteDetached)));
        old.close().await.unwrap(); // must not send a second local Detach
        f.seed_handle(handle).await;
        assert!(matches!(
            timeout(WAIT, f.session.attach_receiver("too-early", "orders"))
                .await
                .unwrap(),
            Err(EngineError::InvalidState(_))
        ));
        // These already-crossed bytes must neither be decoded nor restart the
        // retired receiver. Repeated refusal requests must also stay silent.
        write_amqp(
            &mut f.peer,
            channel,
            incoming_transfer(handle, 99),
            vec![0xff],
        )
        .await
        .unwrap();
        write_amqp(
            &mut f.peer,
            channel,
            Performative::Flow(Flow {
                handle: Some(handle),
                echo: true,
                incoming_window: SESSION_WINDOW,
                ..Flow::default()
            }),
            vec![],
        )
        .await
        .unwrap();
        for (index, receiver) in partials.iter().enumerate() {
            let Performative::Transfer(mut abort) =
                incoming_transfer(receiver.handle, index as u32)
            else {
                unreachable!()
            };
            abort.aborted = true;
            write_amqp(&mut f.peer, channel, Performative::Transfer(abort), vec![])
                .await
                .unwrap();
        }
        write_amqp(
            &mut f.peer,
            channel,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: None,
            }),
            vec![],
        )
        .await
        .unwrap();
        // An actual delivery behind the answer proves the actor consumed it.
        // The next Attach is a positive wire barrier: an extra Detach fails it.
        f.deliver(&mut healthy, 100).await;
        f.seed_handle(handle).await;
        let mut replacement = f.attach("retired", Some(Some(4096)), 4096).await;
        assert_eq!(replacement.handle, handle);
        old.close().await.unwrap();
        f.deliver(&mut replacement, 101).await;
        f.deliver(&mut healthy, 102).await;
        f.seed_handle(64).await;
        let _barrier = f
            .attach("positive-wire-barrier", Some(Some(4096)), 4096)
            .await;
        f.finish().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_original_application_close_survives_crossed_refusal_and_duplicate_close_until_answer() {
    let mut f = Fixture::new().await;
    let old = f.attach("closing", Some(Some(4096)), 4096).await;
    let mut healthy = f.attach("healthy", Some(Some(4096)), 4096).await;
    let channel = f.session.channel;
    let handle = old.handle;
    let mut closing = Box::pin(old.close());
    pending(closing.as_mut()).await;
    let (_, detach) = control(&mut f.peer).await;
    let Performative::Detach(detach) = detach else {
        panic!("original application Detach")
    };
    assert_eq!(detach.handle, handle);
    assert!(detach.error.is_none());
    write_amqp(
        &mut f.peer,
        channel,
        Performative::Flow(Flow {
            handle: Some(handle),
            echo: true,
            incoming_window: SESSION_WINDOW,
            ..Flow::default()
        }),
        vec![],
    )
    .await
    .unwrap();
    old.close().await.unwrap();
    f.deliver(&mut healthy, 1).await;
    pending(closing.as_mut()).await; // duplicate close did not replace the reply
    write_amqp(
        &mut f.peer,
        channel,
        Performative::Detach(Detach {
            handle,
            closed: true,
            error: None,
        }),
        vec![],
    )
    .await
    .unwrap();
    timeout(WAIT, closing.as_mut()).await.unwrap().unwrap();
    drop(closing);
    let _barrier = f
        .attach("positive-wire-barrier", Some(Some(4096)), 4096)
        .await;
    f.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn automatic_refusal_completes_the_original_pending_attach_before_its_late_response() {
    let mut f = Fixture::new().await;
    let mut healthy = f.attach("healthy", Some(Some(4096)), 4096).await;
    let mut opening = Box::pin(f.session.attach_receiver("pending-refusal", "orders"));
    pending(opening.as_mut()).await;
    let (channel, offer) = control(&mut f.peer).await;
    let Performative::Attach(mut offer) = offer else {
        panic!("original pending Attach")
    };
    let handle = offer.handle;
    write_amqp(
        &mut f.peer,
        channel,
        Performative::Flow(Flow {
            handle: Some(handle),
            echo: true,
            incoming_window: SESSION_WINDOW,
            ..Flow::default()
        }),
        vec![],
    )
    .await
    .unwrap();
    assert!(matches!(
        control(&mut f.peer).await.1,
        Performative::Detach(_)
    ));
    assert!(matches!(
        timeout(WAIT, opening.as_mut()).await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    drop(opening);
    offer.role = Role::Sender;
    offer.initial_delivery_count = Some(0);
    write_amqp(&mut f.peer, channel, Performative::Attach(offer), vec![])
        .await
        .unwrap();
    write_amqp(
        &mut f.peer,
        channel,
        Performative::Detach(Detach {
            handle,
            closed: true,
            error: None,
        }),
        vec![],
    )
    .await
    .unwrap();
    f.deliver(&mut healthy, 1).await;
    f.seed_handle(handle).await;
    let mut replacement = f.attach("pending-refusal", Some(Some(4096)), 4096).await;
    f.deliver(&mut replacement, 2).await;
    f.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn end_and_eof_finish_original_close_replies_without_retaining_detach_records() {
    for eof in [false, true] {
        let mut f = Fixture::new().await;
        let old = f.attach("closing", Some(Some(4096)), 4096).await;
        let mut closing = Box::pin(old.close());
        pending(closing.as_mut()).await;
        assert!(matches!(
            control(&mut f.peer).await.1,
            Performative::Detach(_)
        ));
        if eof {
            f.peer.shutdown().await.unwrap();
            assert!(matches!(
                timeout(WAIT, closing.as_mut()).await.unwrap(),
                Err(EngineError::Stopped)
            ));
            drop(closing);
            timeout(WAIT, f.driver).await.unwrap().unwrap().unwrap();
            assert!(*f.connection.closed.borrow());
        } else {
            timeout(WAIT, f.session.end()).await.unwrap().unwrap();
            assert!(matches!(control(&mut f.peer).await.1, Performative::End(_)));
            assert!(matches!(
                timeout(WAIT, closing.as_mut()).await.unwrap(),
                Err(EngineError::RemoteDetached)
            ));
            drop(closing);
            write_amqp(
                &mut f.peer,
                f.session.channel,
                Performative::Detach(Detach {
                    handle: old.handle,
                    closed: true,
                    error: None,
                }),
                vec![],
            )
            .await
            .unwrap();
            write_amqp(
                &mut f.peer,
                f.session.channel,
                Performative::End(End::default()),
                vec![],
            )
            .await
            .unwrap();
            let (session, ()) = timeout(WAIT, async {
                tokio::join!(f.connection.begin(), async {
                    let (channel, begin) = control(&mut f.peer).await;
                    assert!(
                        matches!(begin, Performative::Begin(_)),
                        "no late Detach answer precedes new Begin"
                    );
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
            f.session = session.unwrap();
            let mut active = f.attach("closing", Some(Some(4096)), 4096).await;
            assert_eq!(active.handle, old.handle);
            f.deliver(&mut active, 1).await;
            f.finish().await;
        }
    }
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
    // The incoming Attach answer is behind every Transfer on the same peer
    // stream. Observe its original reply before closing the receiver: a local
    // command alone is not a barrier for the separate frame queue. The full
    // handoff must still be held, not consumed or discarded by local Detach.
    let (_barrier, ()) = timeout(WAIT, async {
        tokio::join!(f.session.attach_sender("window-barrier", "orders"), async {
            let (owner, offer) = control(&mut f.peer).await;
            let Performative::Attach(mut offer) = offer else {
                panic!("window barrier Attach")
            };
            assert_eq!(owner, channel);
            offer.role = Role::Receiver;
            offer.initial_delivery_count = None;
            write_amqp(&mut f.peer, channel, Performative::Attach(offer), vec![])
                .await
                .unwrap();
        })
    })
    .await
    .unwrap();
    let _barrier = _barrier.unwrap();
    assert_eq!(stalled.deliveries.len(), SESSION_WINDOW as usize);
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

#[tokio::test(flavor = "current_thread")]
async fn idle_tcp_peer_cannot_keep_original_reader_alive_after_the_detach_ledger_limit() {
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
    };

    async fn attach(
        session: &mut ClientSession,
        peer: &mut TcpStream,
        name: &str,
    ) -> ClientReceiver {
        let (receiver, ()) = timeout(WAIT, async {
            tokio::join!(session.attach_receiver(name, "orders"), async {
                let (channel, offer) = control(peer).await;
                let Performative::Attach(mut offer) = offer else {
                    panic!("TCP receiver Attach, not an extra Detach")
                };
                let handle = offer.handle;
                offer.role = Role::Sender;
                offer.initial_delivery_count = Some(0);
                write_amqp(peer, channel, Performative::Attach(offer), vec![])
                    .await
                    .unwrap();
                let (_, flow) = control(peer).await;
                let Performative::Flow(flow) = flow else {
                    panic!("TCP initial credit")
                };
                assert_eq!(flow.handle, Some(handle));
            })
        })
        .await
        .unwrap();
        receiver.unwrap()
    }

    // Both receive-flow refusal and an application close can reach the shared
    // limit through an early actor error return. Neither may strand its reader.
    for application_close in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (stream, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let stream = stream.unwrap();
        let (mut peer, _) = accepted.unwrap();
        drop(listener);
        stream.set_nodelay(true).unwrap();
        peer.set_nodelay(true).unwrap();
        let (commands, command_rx) = mpsc::channel(256);
        let (closed_tx, closed) = watch::channel(false);
        let (observations, observed) = watch::channel(None);
        let driver = tokio::spawn(CLIENT_READER_CUSTODY.scope(
            observations,
            run_client(stream, 256 * 1024, command_rx, closed_tx),
        ));
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
        let mut session = session.unwrap();
        let channel = session.channel;
        let Some(ClientReaderObservation::Spawned(original_reader)) = *observed.borrow() else {
            panic!("original reader ID was retained at spawn")
        };
        let mut healthy = attach(&mut session, &mut peer, "healthy").await;
        let mut retired = Vec::new();
        for index in 0..64 {
            let mut receiver = attach(&mut session, &mut peer, &format!("retiring-{index}")).await;
            write_amqp(
                &mut peer,
                channel,
                Performative::Flow(Flow {
                    handle: Some(receiver.handle),
                    echo: true,
                    incoming_window: SESSION_WINDOW,
                    ..Flow::default()
                }),
                vec![],
            )
            .await
            .unwrap();
            let (_, refusal) = control(&mut peer).await;
            let Performative::Detach(refusal) = refusal else {
                panic!("one refusal per retiring link")
            };
            assert_eq!(refusal.handle, receiver.handle);
            assert_eq!(
                refusal.error.unwrap().condition.as_symbol().to_string(),
                "amqp:not-implemented"
            );
            assert!(matches!(
                receiver.recv().await,
                Err(EngineError::RemoteDetached)
            ));
            retired.push(receiver); // withhold every Detach answer
            if index >= 62 {
                let message = Message::data(vec![index]);
                write_amqp(
                    &mut peer,
                    channel,
                    incoming_transfer(healthy.handle, u32::from(index)),
                    encode_message(&message).unwrap(),
                )
                .await
                .unwrap();
                assert_eq!(
                    timeout(WAIT, healthy.recv())
                        .await
                        .unwrap()
                        .unwrap()
                        .message(),
                    &message
                );
                assert_eq!(
                    *observed.borrow(),
                    Some(ClientReaderObservation::Spawned(original_reader)),
                    "the same original reader is still serving healthy traffic at 63/64"
                );
                assert!(!*connection.closed.borrow());
            }
        }
        let mut last = attach(&mut session, &mut peer, "over-limit").await;
        if application_close {
            assert!(matches!(
                timeout(WAIT, last.close()).await.unwrap(),
                Err(EngineError::Stopped)
            ));
        } else {
            write_amqp(
                &mut peer,
                channel,
                Performative::Flow(Flow {
                    handle: Some(last.handle),
                    echo: true,
                    incoming_window: SESSION_WINDOW,
                    ..Flow::default()
                }),
                vec![],
            )
            .await
            .unwrap();
        }
        // From here the peer is entirely idle: no EOF, End, Close or Detach
        // answers can rescue native cleanup. The original driver must join the
        // same reader and release the socket by itself.
        let result = timeout(WAIT, driver).await.unwrap().unwrap();
        assert!(
            matches!(result, Err(EngineError::InvalidState(ref error)) if error.contains("Detach limit"))
        );
        assert_eq!(
            *observed.borrow(),
            Some(ClientReaderObservation::Joined {
                id: original_reader,
                success: true
            })
        );
        assert!(*connection.closed.borrow());
        assert!(matches!(last.recv().await, Err(EngineError::Stopped)));
        let mut byte = [0];
        assert_eq!(
            timeout(WAIT, peer.read(&mut byte)).await.unwrap().unwrap(),
            0,
            "actual TCP EOF, not a second Detach, Close or leaked reader"
        );
    }
}
