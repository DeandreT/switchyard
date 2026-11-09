use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use cluster::{
    consensus::{
        ConsensusIdentity, MAX_CONSENSUS_CONTEXT_BYTES, MAX_CONSENSUS_PROPOSAL_BYTES, Voter,
    },
    transport::{
        FRAME_HEADER_BYTES, FrameCodec, PeerHandler, PeerSender, PeerTransport, TransportError,
        TransportLimits, TransportStatus,
    },
};
use protobuf::Message as ProtoMessage;
use raft::eraftpb::{Entry, Message, MessageType};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};

fn identity() -> ConsensusIdentity {
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let voters = std::array::from_fn(|i| Voter {
        id: i as u64 + 1,
        endpoint: sockets[i].local_addr().unwrap(),
    });
    ConsensusIdentity::new(
        "local-test".into(),
        "transport".into(),
        domain::NamespaceName::new("tenant").unwrap(),
        voters,
    )
    .unwrap()
}
fn address(identity: &ConsensusIdentity, id: u64) -> std::net::SocketAddr {
    identity
        .voters()
        .iter()
        .find(|v| v.id == id)
        .unwrap()
        .endpoint
}
fn message(from: u64, to: u64) -> Message {
    Message {
        from,
        to,
        msg_type: MessageType::MsgAppend,
        term: 1,
        ..Default::default()
    }
}
fn body(frame: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut output = frame[..FRAME_HEADER_BYTES].to_vec();
    output[56..60].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
    output.extend_from_slice(bytes);
    output
}
async fn status(
    sender: &PeerSender,
    predicate: impl Fn(&TransportStatus) -> bool,
) -> TransportStatus {
    let mut updates = sender.subscribe();
    timeout(Duration::from_secs(30), async {
        loop {
            let value = updates.borrow_and_update().clone();
            if predicate(&value) {
                return value;
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}
async fn drive<H: PeerHandler, T>(
    owner: &mut PeerTransport<H>,
    work: impl Future<Output = T>,
) -> T {
    tokio::select! {value=work=>value,_=owner.serve()=>panic!("transport retired before test barrier")}
}
async fn write_frame(
    identity: &ConsensusIdentity,
    from: u64,
    to: u64,
    message: Message,
) -> TcpStream {
    let codec = FrameCodec::new(identity.clone(), from, TransportLimits::default()).unwrap();
    let mut stream = TcpStream::connect(address(identity, to)).await.unwrap();
    stream
        .write_all(&codec.encode(&message).unwrap())
        .await
        .unwrap();
    stream
}

#[test]
fn frames_bound_identity_version_schema_and_decoded_work() {
    let identity = identity();
    let limits = TransportLimits::default();
    let source = FrameCodec::new(identity.clone(), 1, limits.clone()).unwrap();
    let target = FrameCodec::new(identity.clone(), 2, limits.clone()).unwrap();
    let original = message(1, 2);
    let good = source.encode(&original).unwrap();
    assert_eq!(target.decode(&good).unwrap(), original);
    for field in [4usize, 6, 8, 40, 48] {
        let mut bad = good.clone();
        bad[field] ^= 1;
        assert!(target.decode(&bad).is_err());
    }
    let mut bad = good.clone();
    bad[56..60].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(target.decode(&bad).is_err());
    let mut mapped = original.clone();
    mapped.from = 3;
    assert!(matches!(
        target.decode(&body(&good, &mapped.write_to_bytes().unwrap())),
        Err(TransportError::Identity)
    ));
    let mut duplicate = original.write_to_bytes().unwrap();
    duplicate.extend_from_slice(&[0x18, 1]);
    assert!(target.decode(&body(&good, &duplicate)).is_err());
    let mut excessive = original.write_to_bytes().unwrap();
    for _ in 0..65 {
        excessive.extend_from_slice(&[0x3a, 0]);
    }
    assert!(matches!(
        target.decode(&body(&good, &excessive)),
        Err(TransportError::Limit("decoded entry count"))
    ));
    let mut unknown = original.write_to_bytes().unwrap();
    unknown.extend_from_slice(&[0xa3, 1]);
    assert!(target.decode(&body(&good, &unknown)).is_err());
    let mut huge = original.write_to_bytes().unwrap();
    huge.push(0x3a);
    huge.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1]);
    assert!(target.decode(&body(&good, &huge)).is_err());
    let mut snapshot = original.clone();
    snapshot.set_snapshot(Default::default());
    assert!(source.encode(&snapshot).is_err());
    let mut config = original.clone();
    config.entries.push(Entry {
        entry_type: raft::eraftpb::EntryType::EntryConfChangeV2,
        ..Default::default()
    });
    assert!(source.encode(&config).is_err());
    let mut local = original;
    local.msg_type = MessageType::MsgHup;
    assert!(source.encode(&local).is_err());
    let mut noncanonical = vec![0x08, 0x83, 0x00];
    noncanonical.extend_from_slice(&[0x10, 2, 0x18, 1]);
    assert!(target.decode(&body(&good, &noncanonical)).is_err());
}

#[test]
fn within_cap_truncated_context_is_rejected_by_borrowed_preflight() {
    let identity = identity();
    let source = FrameCodec::new(identity.clone(), 1, TransportLimits::default()).unwrap();
    let receiver = FrameCodec::new(identity, 2, TransportLimits::default()).unwrap();
    let header = source.encode(&message(1, 2)).unwrap();
    // Exact review input: a ten-byte body declares a within-cap 1 MiB context,
    // but supplies none. Oversize-only checks cannot prevent its allocation.
    let truncated = [0x08, 0x03, 0x10, 0x02, 0x18, 0x01, 0x62, 0x80, 0x80, 0x40];
    assert!(matches!(
        receiver.decode(&body(&header, &truncated)),
        Err(TransportError::Malformed("truncated bytes field"))
    ));
}

#[test]
fn canonical_ten_byte_scalars_still_use_the_maintained_generated_codec() {
    let identity = identity();
    let source = FrameCodec::new(identity.clone(), 1, TransportLimits::default()).unwrap();
    let receiver = FrameCodec::new(identity, 2, TransportLimits::default()).unwrap();
    let mut original = message(1, 2);
    original.term = u64::MAX;
    original.log_term = u64::MAX;
    original.index = u64::MAX;
    original.commit = u64::MAX;
    original.commit_term = u64::MAX;
    original.deprecated_priority = u64::MAX;
    original.priority = -1;
    original.reject = true;
    original.reject_hint = u64::MAX;
    assert_eq!(
        receiver.decode(&source.encode(&original).unwrap()).unwrap(),
        original
    );
}

#[test]
fn invalid_profiles_and_oversized_lengths_refuse_before_decode_allocation() {
    let identity = identity();
    for limits in [
        TransportLimits {
            max_frame_bytes: cluster::transport::MIN_FRAME_BYTES - 1,
            ..Default::default()
        },
        TransportLimits {
            max_frame_bytes: usize::MAX,
            ..Default::default()
        },
        TransportLimits {
            max_entries: usize::MAX,
            ..Default::default()
        },
        TransportLimits {
            per_peer_bytes: usize::MAX,
            ..Default::default()
        },
        TransportLimits {
            max_bytes: 32 * 1024 * 1024,
            ..Default::default()
        },
    ] {
        assert!(FrameCodec::new(identity.clone(), 1, limits).is_err());
    }
    let codec = FrameCodec::new(identity.clone(), 1, TransportLimits::default()).unwrap();
    let receiver = FrameCodec::new(identity, 2, TransportLimits::default()).unwrap();
    let good = codec.encode(&message(1, 2)).unwrap();
    // Canonical outer length but a nested data length outside the 358 ceiling.
    // No generated protobuf decoder is reached for this hostile length.
    let mut entry = vec![0x22];
    let mut size = (MAX_CONSENSUS_PROPOSAL_BYTES + 1) as u64;
    while size >= 128 {
        entry.push((size as u8 & 127) | 128);
        size >>= 7;
    }
    entry.push(size as u8);
    entry.resize(entry.len() + MAX_CONSENSUS_PROPOSAL_BYTES + 1, 0);
    let mut bytes = message(1, 2).write_to_bytes().unwrap();
    bytes.push(0x3a);
    let mut size = entry.len() as u64;
    while size >= 128 {
        bytes.push((size as u8 & 127) | 128);
        size >>= 7;
    }
    bytes.push(size as u8);
    bytes.extend_from_slice(&entry);
    assert!(matches!(
        receiver.decode(&body(&good, &bytes)),
        Err(TransportError::Limit("decoded bytes field"))
    ));
}

#[test]
fn full_persistence_data_and_context_ceilings_fit_without_reencoding_payloads() {
    let identity = identity();
    let source = FrameCodec::new(identity.clone(), 1, TransportLimits::default()).unwrap();
    let target = FrameCodec::new(identity, 2, TransportLimits::default()).unwrap();
    let make = |size| {
        domain::DurableProposal::unbound(domain::Command::new(
            domain::NamespaceName::new("tenant").unwrap(),
            domain::EntityPath::new("orders").unwrap(),
            domain::Timestamp::from_millis(37),
            domain::CommandKind::Send {
                message_id: "historical".into(),
                body: vec![7; size],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        ))
        .encode()
    };
    let (mut low, mut high) = (0, MAX_CONSENSUS_PROPOSAL_BYTES);
    while low < high {
        let mid = (low + high).div_ceil(2);
        if make(mid).is_ok() {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    let data = make(low).unwrap();
    assert_eq!(data.len(), MAX_CONSENSUS_PROPOSAL_BYTES);
    let mut msg = message(1, 2);
    msg.context = vec![9; MAX_CONSENSUS_CONTEXT_BYTES].into();
    msg.entries.push(Entry {
        index: 1,
        term: 1,
        data: data.clone().into(),
        context: vec![8; MAX_CONSENSUS_CONTEXT_BYTES].into(),
        ..Default::default()
    });
    let encoded = source.encode(&msg).unwrap();
    let decoded = target.decode(&encoded).unwrap();
    assert_eq!(decoded, msg);
    assert_eq!(decoded.entries[0].data.as_ref(), data);
    msg.entries[0].data = vec![0; MAX_CONSENSUS_PROPOSAL_BYTES + 1].into();
    assert!(source.encode(&msg).is_err());
}

#[tokio::test]
async fn partial_header_and_body_deadlines_close_whole_idle_socket_and_keep_unjoined_credit() {
    for partial_body in [false, true] {
        let identity = identity();
        let limits = TransportLimits {
            io_timeout: Duration::from_millis(200),
            ..Default::default()
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (mut owner, sender) = PeerTransport::bind(identity.clone(), 2, limits, move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .await
        .unwrap();
        let mut stream = drive(&mut owner, async {
            let mut stream = TcpStream::connect(address(&identity, 2)).await.unwrap();
            let frame = FrameCodec::new(identity.clone(), 1, TransportLimits::default())
                .unwrap()
                .encode(&message(1, 2))
                .unwrap();
            let bytes = if partial_body { FRAME_HEADER_BYTES } else { 3 };
            stream.write_all(&frame[..bytes]).await.unwrap();
            status(&sender, |s| {
                s.global.connections == 1 && (!partial_body || s.peers[0].connections == 1)
            })
            .await;
            stream
        })
        .await;
        // The borrowed serving future is gone; original IO still owns its one
        // absolute deadline. Actual EOF proves both directions closed.
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(10), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(
            sender.status().global.connections,
            1,
            "finished IO keeps credit until join"
        );
        let report = owner.finish().await;
        assert_eq!(report.joined, 1);
        assert_eq!(report.deadlines, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(sender.status().global.connections, 0);
    }
}

#[tokio::test]
async fn begun_handler_survives_disconnect_retirement_and_canceled_finish() {
    let identity = identity();
    let gate = Arc::new(Semaphore::new(0));
    let release = gate.clone();
    let (count_tx, mut starts) = mpsc::channel(1);
    let done = Arc::new(AtomicUsize::new(0));
    let completed = done.clone();
    let (mut owner, sender) = PeerTransport::bind(
        identity.clone(),
        2,
        TransportLimits {
            io_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        move |_| {
            let gate = gate.clone();
            let count = count_tx.clone();
            let completed = completed.clone();
            async move {
                count.send(()).await.unwrap();
                gate.acquire().await.unwrap().forget();
                completed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    )
    .await
    .unwrap();
    drive(&mut owner, async {
        let mut socket = write_frame(&identity, 1, 2, message(1, 2)).await;
        starts.recv().await.unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        drop(socket);
        // A later accepted partial frame reaches its real IO deadline first:
        // the older admitted handler's original deadline has therefore elapsed.
        let mut idle = TcpStream::connect(address(&identity, 2)).await.unwrap();
        idle.write_all(b"SW").await.unwrap();
        status(&sender, |s| s.global.connections == 2).await;
        assert_eq!(idle.read(&mut byte).await.unwrap(), 0);
        status(&sender, |s| s.report.deadlines == 1).await;
    })
    .await;
    let mut finishing = Box::pin(owner.finish());
    std::future::poll_fn(|cx| {
        assert!(finishing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(finishing);
    assert!(sender.status().retired);
    assert_eq!(sender.status().dispatching, 1);
    assert_eq!(sender.status().global.connections, 1);
    assert_eq!(done.load(Ordering::SeqCst), 0);
    release.add_permits(1);
    let report = owner.finish().await;
    assert_eq!(report.joined, 2);
    assert_eq!(report.dispatched, 1);
    assert_eq!(done.load(Ordering::SeqCst), 1);
    assert_eq!(sender.status().global.connections, 0);
}

#[tokio::test]
async fn per_peer_backpressure_isolates_a_held_handler_from_healthy_peer() {
    let identity = identity();
    let gate = Arc::new(Semaphore::new(0));
    let release = gate.clone();
    let (tx, mut events) = mpsc::channel(4);
    let limits = TransportLimits {
        per_peer_operations: 1,
        per_peer_connections: 1,
        max_operations: 4,
        max_connections: 4,
        ..Default::default()
    };
    let (mut owner, sender) =
        PeerTransport::bind(identity.clone(), 2, limits, move |msg: Message| {
            let gate = gate.clone();
            let events = tx.clone();
            async move {
                events.send(msg.from).await.unwrap();
                if msg.from == 1 {
                    gate.acquire().await.unwrap().forget();
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    drive(&mut owner, async {
        let _first = write_frame(&identity, 1, 2, message(1, 2)).await;
        assert_eq!(events.recv().await, Some(1));
        let mut denied = write_frame(&identity, 1, 2, message(1, 2)).await;
        let mut byte = [0];
        match denied.read(&mut byte).await {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            result => panic!("refused peer remained open: {result:?}"),
        }
        let _healthy = write_frame(&identity, 3, 2, message(3, 2)).await;
        assert_eq!(events.recv().await, Some(3));
        let live = status(&sender, |s| {
            s.report.refused == 1 && s.report.dispatched == 1
        })
        .await;
        assert_eq!(live.peers[0].operations, 1);
        assert_eq!(live.peers[2].operations, 0);
    })
    .await;
    release.add_permits(1);
    let report = owner.finish().await;
    assert_eq!(report.dispatched, 2);
    assert_eq!(report.refused, 1);
    assert_eq!(sender.status().global.operations, 0);
}

#[tokio::test]
async fn outbound_quotas_cancelled_receipts_and_queue_retirement_are_bounded() {
    let identity = identity();
    let limits = TransportLimits {
        per_peer_operations: 1,
        per_peer_connections: 2,
        max_operations: 3,
        max_connections: 4,
        ..Default::default()
    };
    let (mut owner, sender) = PeerTransport::bind(identity, 1, limits, |_| async { Ok(()) })
        .await
        .unwrap();
    let first = sender.try_send(message(1, 2)).unwrap();
    assert!(matches!(
        sender.try_send(message(1, 2)),
        Err(TransportError::Busy(_))
    ));
    let mut second = sender.try_send(message(1, 3)).unwrap();
    drop(first);
    assert_eq!(sender.status().global.operations, 2);
    sender.retire();
    assert!(matches!(
        sender.try_send(message(1, 3)),
        Err(TransportError::Retired)
    ));
    let report = owner.finish().await;
    assert_eq!(report.retired_queued, 2);
    assert!(matches!(second.wait().await, Err(TransportError::Retired)));
    assert_eq!(sender.status().global.operations, 0);
}

#[tokio::test]
async fn panicked_dispatch_is_joined_before_releasing_credit() {
    let identity = identity();
    let (mut owner, sender) =
        PeerTransport::bind(identity.clone(), 2, TransportLimits::default(), |_| async {
            panic!("original dispatch panic");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .unwrap();
    drive(&mut owner, async {
        let _socket = write_frame(&identity, 1, 2, message(1, 2)).await;
        status(&sender, |s| s.report.panics == 1).await;
    })
    .await;
    let report = owner.finish().await;
    assert_eq!(report.panics, 1);
    assert_eq!(report.joined, 1);
    assert_eq!(sender.status().global.operations, 0);
}

#[tokio::test]
async fn blocked_write_stop_and_deadline_close_socket_without_detached_reader() {
    for expire in [false, true] {
        let identity = identity();
        let remote = TcpListener::bind(address(&identity, 2)).await.unwrap();
        let limits = TransportLimits {
            max_frame_bytes: 16 * 1024 * 1024,
            per_peer_bytes: 80 * 1024 * 1024,
            max_bytes: 160 * 1024 * 1024,
            io_timeout: Duration::from_secs(if expire { 3 } else { 30 }),
            ..Default::default()
        };
        let (mut owner, sender) = PeerTransport::bind(identity, 1, limits, |_| async { Ok(()) })
            .await
            .unwrap();
        let mut msg = message(1, 2);
        for i in 1..=7 {
            msg.entries.push(Entry {
                index: i,
                term: 1,
                data: vec![0; MAX_CONSENSUS_PROPOSAL_BYTES].into(),
                context: vec![0; MAX_CONSENSUS_CONTEXT_BYTES].into(),
                ..Default::default()
            });
        }
        let mut receipt = sender.try_send(msg).unwrap();
        let mut peer = drive(&mut owner, async {
            let (socket, _) = remote.accept().await.unwrap();
            status(&sender, |s| s.blocked_writes > 0).await;
            socket
        })
        .await;
        let mut waiting = Box::pin(receipt.wait());
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(waiting);
        if expire {
            assert!(matches!(
                drive(&mut owner, receipt.wait()).await,
                Err(TransportError::Deadline)
            ));
        }
        let report = owner.finish().await;
        assert_eq!(report.written, 0);
        assert_eq!(report.joined, 1);
        if expire {
            assert_eq!(report.deadlines, 1);
        } else {
            assert!(matches!(receipt.wait().await, Err(TransportError::Retired)));
        }
        let mut data = Vec::new();
        timeout(Duration::from_secs(10), peer.read_to_end(&mut data))
            .await
            .unwrap()
            .unwrap();
        assert!(data.len() < 14 * 1024 * 1024);
        assert_eq!(sender.status().global.connections, 0);
    }
}

#[tokio::test]
async fn actual_tcp_refuses_foreign_unsupported_mapped_and_malformed_before_dispatch() {
    let identity = identity();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (mut owner, sender) =
        PeerTransport::bind(identity.clone(), 2, TransportLimits::default(), move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .await
        .unwrap();
    let source = FrameCodec::new(identity.clone(), 1, TransportLimits::default()).unwrap();
    let good = source.encode(&message(1, 2)).unwrap();
    let mut bad = Vec::new();
    for field in [4usize, 8, 40, 48] {
        let mut frame = good.clone();
        frame[field] ^= 1;
        bad.push(frame);
    }
    let mut mapped = message(3, 2);
    mapped.term = 99;
    bad.push(body(&good, &mapped.write_to_bytes().unwrap()));
    let mut malformed = message(1, 2).write_to_bytes().unwrap();
    malformed.extend_from_slice(&[0x18, 1]);
    bad.push(body(&good, &malformed));
    let mut repeated = message(1, 2).write_to_bytes().unwrap();
    for _ in 0..65 {
        repeated.extend_from_slice(&[0x3a, 0]);
    }
    bad.push(body(&good, &repeated));
    bad.push(body(
        &good,
        &[0x08, 0x03, 0x10, 0x02, 0x18, 0x01, 0x62, 0x80, 0x80, 0x40],
    ));
    drive(&mut owner, async {
        for (index, frame) in bad.iter().enumerate() {
            let mut peer = TcpStream::connect(address(&identity, 2)).await.unwrap();
            peer.write_all(frame).await.unwrap();
            let mut byte = [0];
            match peer.read(&mut byte).await {
                Ok(0) => {}
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
                other => panic!("invalid frame retained a socket: {other:?}"),
            }
            status(&sender, |s| s.report.joined == index as u64 + 1).await;
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    })
    .await;
    let report = owner.finish().await;
    assert_eq!(report.refused, bad.len() as u64);
    assert_eq!(report.dispatched, 0);
    assert_eq!(sender.status().global.bytes, 0);
}

#[tokio::test]
async fn byte_and_global_operation_backpressure_have_no_waiter_queue() {
    let first_identity = identity();
    let limits = TransportLimits {
        max_connections: 8,
        per_peer_connections: 4,
        max_operations: 3,
        per_peer_operations: 2,
        ..Default::default()
    };
    let (mut owner, sender) = PeerTransport::bind(first_identity, 1, limits, |_| async { Ok(()) })
        .await
        .unwrap();
    let first = sender.try_send(message(1, 2)).unwrap();
    let second = sender.try_send(message(1, 2)).unwrap();
    let third = sender.try_send(message(1, 3)).unwrap();
    assert!(matches!(
        sender.try_send(message(1, 3)),
        Err(TransportError::Busy("global quota"))
    ));
    assert_eq!(sender.status().global.operations, 3);
    drop((first, second, third));
    owner.finish().await;
    assert_eq!(sender.status().global.operations, 0);

    let mut large = message(1, 2);
    large.context = vec![0; MAX_CONSENSUS_CONTEXT_BYTES].into();
    large.entries.push(Entry {
        term: 1,
        index: 1,
        data: vec![0; MAX_CONSENSUS_PROPOSAL_BYTES].into(),
        context: vec![0; MAX_CONSENSUS_CONTEXT_BYTES].into(),
        ..Default::default()
    });
    let charge = 4 * (FRAME_HEADER_BYTES + large.compute_size() as usize)
        + 2 * 64 * std::mem::size_of::<Entry>()
        + std::mem::size_of::<Message>();
    let limits = TransportLimits {
        max_bytes: 4 * charge - 1,
        ..Default::default()
    };
    let (mut owner, sender) = PeerTransport::bind(identity(), 1, limits, |_| async { Ok(()) })
        .await
        .unwrap();
    let first = sender.try_send(large.clone()).unwrap();
    let second = sender.try_send(large.clone()).unwrap();
    assert!(matches!(
        sender.try_send(large.clone()),
        Err(TransportError::Busy("peer quota"))
    ));
    large.to = 3;
    let third = sender.try_send(large.clone()).unwrap();
    assert!(matches!(
        sender.try_send(large),
        Err(TransportError::Busy("global quota"))
    ));
    drop((first, second, third));
    let report = owner.finish().await;
    assert_eq!(report.retired_queued, 3);
    assert_eq!(sender.status().global.bytes, 0);
}

#[tokio::test]
async fn truncated_disconnect_and_failed_destination_do_not_block_reconnect_or_healthy_peer() {
    let identity = identity();
    let (events, mut delivered) = mpsc::channel(1);
    let (mut owner, sender) =
        PeerTransport::bind(identity.clone(), 2, TransportLimits::default(), move |_| {
            let events = events.clone();
            async move {
                events.send(()).await.unwrap();
                Ok(())
            }
        })
        .await
        .unwrap();
    drive(&mut owner, async {
        let frame = FrameCodec::new(identity.clone(), 1, TransportLimits::default())
            .unwrap()
            .encode(&message(1, 2))
            .unwrap();
        let mut broken = TcpStream::connect(address(&identity, 2)).await.unwrap();
        broken
            .write_all(&frame[..FRAME_HEADER_BYTES + 1])
            .await
            .unwrap();
        drop(broken);
        status(&sender, |s| s.report.io_errors == 1).await;
        let mut retry = write_frame(&identity, 1, 2, message(1, 2)).await;
        delivered.recv().await.unwrap();
        let mut byte = [0];
        assert_eq!(retry.read(&mut byte).await.unwrap(), 0);
        status(&sender, |s| s.report.joined == 2).await;
    })
    .await;
    let report = owner.finish().await;
    assert_eq!(report.dispatched, 1);
    assert_eq!(report.io_errors, 1);
    drop(owner);

    let remote = TcpListener::bind(address(&identity, 3)).await.unwrap();
    let (mut owner, sender) =
        PeerTransport::bind(identity, 1, TransportLimits::default(), |_| async {
            Ok(())
        })
        .await
        .unwrap();
    let mut failed = sender.try_send(message(1, 2)).unwrap();
    let mut healthy = sender.try_send(message(1, 3)).unwrap();
    drive(&mut owner, async {
        let (mut peer, _) = remote.accept().await.unwrap();
        let mut frame = Vec::new();
        peer.read_to_end(&mut frame).await.unwrap();
        assert!(frame.len() > FRAME_HEADER_BYTES);
        healthy.wait().await.unwrap();
        assert!(matches!(failed.wait().await, Err(TransportError::Io(_))));
    })
    .await;
    let report = owner.finish().await;
    assert_eq!(report.written, 1);
    assert_eq!(report.io_errors, 1);
    assert_eq!(sender.status().global.connections, 0);
}
