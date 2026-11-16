//! Test-only access to real actor-minted drains; no sender count is seeded.
use super::*;
use crate::test_deadline::WAIT;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::timeout,
};

async fn control(peer: &mut TcpStream) -> Performative {
    let Frame::Amqp {
        performative: Some(p),
        ..
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("TCP frame")
    };
    p
}

async fn flow(
    peer: &mut TcpStream,
    channel: u16,
    handle: u32,
    count: u32,
    credit: u32,
    drain: bool,
    received: u32,
) {
    write_amqp(
        peer,
        channel,
        Performative::Flow(Flow {
            next_incoming_id: Some(received),
            incoming_window: 4096,
            handle: Some(handle),
            delivery_count: Some(count),
            link_credit: Some(credit),
            drain,
            ..Flow::default()
        }),
        vec![],
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn tcp_client_wraps_after_legal_drain_pauses_original_send_and_joins_original_idle_reader() {
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
        run_client(stream, 1024, command_rx, closed_tx),
    ));
    let mut connection = ClientConnection { commands, closed };
    let (session, ()) = timeout(WAIT, async {
        tokio::join!(connection.begin(), async {
            assert!(matches!(control(&mut peer).await, Performative::Begin(_)));
            write_amqp(
                &mut peer,
                0,
                Performative::Begin(Begin {
                    remote_channel: Some(0),
                    incoming_window: 4096,
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
    let session = session.unwrap();
    let channel = session.channel;
    let Some(ClientReaderObservation::Spawned(reader)) = *observed.borrow() else {
        panic!("original reader")
    };
    let (deliveries_tx, _) = engine::incoming::delivery_channel();
    let (detached_tx, _detached) = watch::channel(false);
    let (drain_tx, mut drains) = watch::channel(None);
    let (credit_tx, _credits) = watch::channel(false);
    let (attached, ()) = timeout(WAIT, async {
        tokio::join!(
            client_request(&session.commands, |reply| ClientCommand::Attach {
                channel,
                request: Box::new(AttachRequest {
                    name: "tcp-wrap".into(),
                    role: Role::Sender,
                    sender_settle_mode: SenderSettleMode::Unsettled,
                    receiver_settle_mode: ReceiverSettleMode::First,
                    source: None,
                    target: Some(Target::new("orders")),
                    max_message_size: None,
                    detach_error: None
                }),
                deliveries_tx,
                detached_tx,
                drain_tx,
                credit_tx,
                reply,
            }),
            async {
                let Performative::Attach(mut a) = control(&mut peer).await else {
                    panic!("Attach")
                };
                assert_eq!(a.initial_delivery_count, Some(0));
                a.role = Role::Receiver;
                a.initial_delivery_count = None;
                write_amqp(&mut peer, channel, Performative::Attach(a), vec![])
                    .await
                    .unwrap();
            }
        )
    })
    .await
    .unwrap();
    let (handle, incarnation, _) = attached.unwrap();
    let mut count = 0_u32;
    for n in [2147483647, 2147483647, 1] {
        flow(&mut peer, channel, handle, count, n, true, 0).await;
        timeout(WAIT, drains.changed()).await.unwrap().unwrap();
        let request = drains
            .borrow_and_update()
            .expect("genuine actor-minted drain");
        assert_eq!(request.incarnation, incarnation);
        timeout(
            WAIT,
            client_request(&session.commands, |reply| ClientCommand::AcknowledgeDrain {
                request,
                reply,
            }),
        )
        .await
        .unwrap()
        .unwrap();
        count = count.wrapping_add(n);
        let Performative::Flow(f) = control(&mut peer).await else {
            panic!("control-only drain")
        };
        assert_eq!(
            (f.delivery_count, f.link_credit, f.next_outgoing_id),
            (Some(count), Some(0), 0)
        );
        assert!(f.drain && !f.echo);
        // Consume completion before waiting for the next generation.
        assert!(drains.borrow_and_update().is_none());
    }
    assert_eq!(count, u32::MAX);
    flow(&mut peer, channel, handle, count, 1, false, 0).await;
    let capacity = Arc::new(Semaphore::new(2));
    let mut starts = Vec::new();
    let mut outcomes = Vec::new();
    for marker in [11, 12] {
        let (started, start) = oneshot::channel();
        let (reply, outcome) = oneshot::channel();
        session
            .commands
            .send(ClientCommand::Send {
                channel,
                handle,
                incarnation,
                message: Box::new(Message::data(vec![marker])),
                message_format: 0,
                delivery_tag: vec![marker].into(),
                permit: Arc::clone(&capacity).acquire_owned().await.unwrap(),
                started,
                reply,
            })
            .await
            .unwrap();
        starts.push(start);
        outcomes.push(outcome);
    }
    let Performative::Transfer(t) = control(&mut peer).await else {
        panic!("one Transfer")
    };
    assert_eq!(
        (t.delivery_id, t.delivery_tag),
        (Some(0), Some(vec![11].into()))
    );
    assert_eq!(
        timeout(WAIT, &mut starts[0])
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .delivery_id,
        0
    );
    // Positive command FIFO barrier: both original Send commands precede Begin.
    let (barrier, ()) = timeout(WAIT, async {
        tokio::join!(connection.begin(), async {
            assert!(
                matches!(control(&mut peer).await, Performative::Begin(_)),
                "no second Transfer before a new grant"
            );
            write_amqp(
                &mut peer,
                1,
                Performative::Begin(Begin {
                    remote_channel: Some(1),
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
    let _barrier = barrier.unwrap();
    assert!(matches!(
        starts[1].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    flow(&mut peer, channel, handle, 0, 1, false, 1).await;
    let Performative::Transfer(t) = control(&mut peer).await else {
        panic!("original second Transfer")
    };
    assert_eq!(
        (t.delivery_id, t.delivery_tag),
        (Some(1), Some(vec![12].into()))
    );
    assert_eq!(
        timeout(WAIT, &mut starts[1])
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .delivery_id,
        1
    );
    assert_eq!(
        capacity.available_permits(),
        0,
        "unsettled retention is not link credit"
    );
    for id in [1, 0] {
        write_amqp(
            &mut peer,
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled: true,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
            vec![],
        )
        .await
        .unwrap();
        assert!(matches!(
            timeout(WAIT, &mut outcomes[id as usize])
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .outcome,
            Outcome::Accepted(_)
        ));
    }
    assert_eq!(capacity.available_permits(), 2);
    // A real invalid session observation takes the actor's error path. From
    // this write onward the TCP peer stays idle; no EOF or Close answer rescues
    // the exact original reader, which must be canceled/joined by its owner.
    flow(&mut peer, channel, handle, 1, 0, false, 3).await;
    assert!(matches!(
        timeout(WAIT, driver).await.unwrap().unwrap(),
        Err(EngineError::InvalidState(_))
    ));
    assert_eq!(
        *observed.borrow(),
        Some(ClientReaderObservation::Joined {
            id: reader,
            success: true
        })
    );
    assert!(*connection.closed.borrow());
    assert_eq!(
        timeout(WAIT, peer.read(&mut [0])).await.unwrap().unwrap(),
        0
    );
}
