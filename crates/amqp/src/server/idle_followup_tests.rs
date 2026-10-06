#[tokio::test(start_paused = true)]
async fn transmitting_heartbeats_does_not_extend_receive_silence_and_close_stops_them() {
    for &client in sides() {
        let (connection, mut peer) = pair(
            client,
            ConnectionOptions::default().idle_timeout_millis(1000),
            Some(1000),
        )
        .await;
        for _ in 0..3 {
            advance(Duration::from_millis(500)).await;
            assert_heartbeat(next_frame(&mut peer).await);
        }
        advance(Duration::from_millis(500)).await;
        assert_idle_close(next_frame(&mut peer).await);
        write_amqp(
            &mut peer,
            0,
            Performative::Begin(Begin::default()),
            Vec::new(),
        )
        .await
        .expect("crossing Begin after Close");
        advance(Duration::from_millis(1000)).await;
        ready().await;
        assert_no_bytes(&mut peer).await;
        write_amqp(
            &mut peer,
            0,
            Performative::Close(Close::default()),
            Vec::new(),
        )
        .await
        .expect("ack Close");
        connection.terminated().await;
        let mut rest = Vec::new();
        peer.read_to_end(&mut rest).await.expect("EOF");
        assert!(
            rest.is_empty(),
            "no Begin reply, heartbeat, or second Close"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn ordinary_outbound_frame_delays_heartbeat_but_not_receive_deadline() {
    let (connection, mut peer) = server_pair(
        ConnectionOptions::default().idle_timeout_millis(1000),
        Some(1000),
        4096,
    )
    .await;
    #[cfg(feature = "test-client")]
    let TestConnection::Server(mut connection) = connection else {
        unreachable!()
    };
    #[cfg(not(feature = "test-client"))]
    let TestConnection::Server(mut connection) = connection;
    advance(Duration::from_millis(400)).await;
    write_amqp(
        &mut peer,
        0,
        Performative::Begin(Begin::default()),
        Vec::new(),
    )
    .await
    .expect("Begin");
    let incoming = connection
        .next_incoming_session()
        .await
        .expect("incoming Begin");
    let (session, frame) = tokio::join!(connection.accept_session(incoming), next_frame(&mut peer));
    session.expect("accepted session");
    assert!(matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    advance(Duration::from_millis(499)).await;
    ready().await;
    assert_no_bytes(&mut peer).await;
    advance(Duration::from_millis(1)).await;
    assert_heartbeat(next_frame(&mut peer).await);
    connection.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unrelated_activity_updates_cannot_starve_an_absolute_watchdog_deadline() {
    let activity = Activity::new();
    let options = ConnectionOptions::default().idle_timeout_millis(1000);
    for _ in 0..20 {
        advance(Duration::from_millis(100)).await;
        activity.completed_write();
    }
    assert_eq!(
        activity.timeout(options, 1000).await,
        ActivityTimeout::Receive
    );

    let activity = Activity::new();
    activity.begin_write(true);
    activity.completed_write();
    for _ in 0..20 {
        advance(Duration::from_millis(100)).await;
        activity.received_frame();
    }
    assert_eq!(
        activity.timeout(options, 1000).await,
        ActivityTimeout::Close
    );

    let activity = Activity::new();
    for _ in 0..10 {
        advance(Duration::from_millis(100)).await;
        activity.received_frame();
    }
    assert_eq!(activity.timeout(options, 1000).await, ActivityTimeout::Peer);
}

#[tokio::test(start_paused = true)]
async fn default_receive_silence_is_twice_the_advertised_sixty_seconds() {
    let (connection, mut peer) = server_pair(ConnectionOptions::default(), None, 4096).await;
    advance(Duration::from_secs(119)).await;
    ready().await;
    assert_no_bytes(&mut peer).await;
    advance(Duration::from_secs(1)).await;
    assert_idle_close(next_frame(&mut peer).await);
    connection.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn zero_receive_idle_still_interrupts_a_blocked_peer_heartbeat_write() {
    for block_flush in [false, true] {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let gate = Arc::new(WriteGate::default());
        let stream = GatedIo {
            inner: wire,
            gate: gate.clone(),
            block_flush,
        };
        let opening = async {
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("server header");
            peer_open(&mut peer, Some(1000)).await;
            next_frame(&mut peer).await;
        };
        let (connection, ()) = tokio::join!(
            ServerConnection::accept_with_options(
                stream,
                "gated-server",
                None,
                ConnectionOptions::default().idle_timeout_millis(0)
            ),
            opening
        );
        let connection = connection.expect("server opens");
        ready().await;
        gate.blocked.store(true, Ordering::Release);
        let start = Instant::now();
        advance(Duration::from_millis(500)).await;
        ready().await;
        advance(Duration::from_millis(500)).await;
        connection.lifecycle.wait_terminated().await;
        assert_eq!(Instant::now() - start, Duration::from_secs(1));
        let mut rest = Vec::new();
        peer.read_to_end(&mut rest)
            .await
            .expect("terminated transport");
        if block_flush {
            assert_heartbeat(
                read_frame(&mut rest.as_slice())
                    .await
                    .expect("one unflushed heartbeat"),
            );
            assert_eq!(rest.len(), 8);
        } else {
            assert_eq!(rest.len(), 1, "no Close appended to truncated heartbeat");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_late_complete_frame_cannot_revive_an_expired_receive_deadline() {
    let options = ConnectionOptions::default().idle_timeout_millis(1000);
    let activity = Activity::configured(options);
    advance(Duration::from_millis(1999)).await;
    assert!(
        activity.received_frame(),
        "complete frame before the deadline resets it"
    );
    advance(Duration::from_millis(2000)).await;
    assert!(
        !activity.received_frame(),
        "equal deadline is already expired"
    );
    assert_eq!(activity.timeout(options, 0).await, ActivityTimeout::Receive);
    advance(Duration::from_secs(1)).await;
    assert!(
        !activity.received_frame(),
        "expiry is sticky rather than renewed by late input"
    );
    activity.begin_write(true);
    activity.completed_write();
    assert!(
        activity.received_frame(),
        "Close acknowledgment remains legal after receive expiry"
    );
}

struct LateFlushWriter {
    state: Arc<BlockedWriteState>,
    flush: Pin<Box<tokio::time::Sleep>>,
}

impl AsyncWrite for LateFlushWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.state
            .bytes
            .lock()
            .expect("bytes")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.flush.as_mut().poll(cx).map(|_| Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn a_ready_flush_at_or_after_the_deadline_cannot_be_recorded_as_activity() {
    for elapsed in [Duration::from_millis(25), Duration::from_millis(30)] {
        let state = Arc::new(BlockedWriteState::default());
        let activity = Activity::new();
        let io = LateFlushWriter {
            state: state.clone(),
            flush: Box::pin(tokio::time::sleep(elapsed)),
        };
        let mut writer = FrameWriter::new(io, 512).expect("writer");
        writer.configure_activity(
            ConnectionOptions::default().write_timeout(Duration::from_millis(25)),
            0,
            activity.clone(),
        );
        let frame = Frame::Amqp {
            channel: 0,
            performative: None,
            payload: Vec::new(),
        };
        {
            let writing = writer.write_frame(&frame);
            tokio::pin!(writing);
            std::future::poll_fn(|cx| {
                assert!(writing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            // Both the flush and timeout are ready when next polled. Tokio polls
            // the inner I/O first, so the post-completion deadline check matters.
            advance(elapsed).await;
            assert_eq!(
                writing.await.expect_err("late flush rejected").kind(),
                io::ErrorKind::TimedOut
            );
        }
        assert!(activity.is_tainted());
        assert_eq!(
            activity.timeout(ConnectionOptions::default(), 0).await,
            ActivityTimeout::WriteFailed
        );
        let before = state.bytes.lock().expect("bytes").clone();
        assert!(
            writer
                .write_amqp(0, Performative::Close(Close::default()), Vec::new())
                .await
                .is_err()
        );
        assert_eq!(*state.bytes.lock().expect("bytes"), before);
    }
}

#[tokio::test(start_paused = true)]
async fn negotiation_write_and_flush_deadlines_are_also_strict() {
    for frame in [false, true] {
        let state = Arc::new(BlockedWriteState::default());
        let mut io = LateFlushWriter {
            state,
            flush: Box::pin(tokio::time::sleep(Duration::from_millis(25))),
        };
        let options = ConnectionOptions::default().write_timeout(Duration::from_millis(25));
        let writing = async {
            if frame {
                negotiation_frame(
                    &mut io,
                    &Frame::Amqp {
                        channel: 0,
                        performative: None,
                        payload: Vec::new(),
                    },
                    options,
                )
                .await
            } else {
                negotiation_header(&mut io, ProtocolHeader::AMQP, options).await
            }
        };
        tokio::pin!(writing);
        std::future::poll_fn(|cx| {
            assert!(writing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        advance(Duration::from_millis(25)).await;
        assert!(matches!(writing.await, Err(EngineError::Timeout("write"))));
    }
}
