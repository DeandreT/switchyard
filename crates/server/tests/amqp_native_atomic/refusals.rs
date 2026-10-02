use super::*;

pub(super) async fn unsupported_targets_and_bad_posts_are_link_scoped<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true).await?;
    for target in [
        "topic",
        "topic/subscriptions/Alpha",
        "orders/$deadletterqueue",
        "session",
    ] {
        let mut peer = Peer::connect(node.address).await?;
        peer.begin(POST).await?;
        let before = node.snapshot()?;
        node.controls.reset();
        peer.attach(POST, POST_HANDLE, target, false, ReceiverSettleMode::First)
            .await?;
        peer.detached(POST, POST_HANDLE, "amqp:not-allowed").await?;
        node.unchanged(&before)?;
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        peer.healthy().await?;
        peer.close().await?;
    }
    let mut session_message = message("session", b"not supported");
    session_message.properties.as_mut().unwrap().group_id = Some("cart".into());
    let mut scheduled = message("scheduled", b"past due still unsupported");
    scheduled
        .message_annotations
        .get_or_insert_with(Default::default)
        .insert(
            Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(1_i64.into()),
        );
    let malformed = Message {
        body: Body::Data(vec![
            encode_message(&message("valid-first", b"one"))?.into(),
            vec![0xff].into(),
        ]),
        ..Message::default()
    };
    let excessive = batch(
        &(0..101)
            .map(|i| message(&format!("item-{i}"), b"x"))
            .collect::<Vec<_>>(),
    )?;
    for (input, format, condition) in [
        (session_message, 0, "amqp:not-allowed"),
        (scheduled, 0, "amqp:not-allowed"),
        (
            malformed,
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            "amqp:invalid-field",
        ),
        (
            excessive,
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            "amqp:resource-limit-exceeded",
        ),
    ] {
        let mut peer = Peer::connect(node.address).await?;
        peer.setup(ReceiverSettleMode::First).await?;
        let transaction = peer.declare(ReceiverSettleMode::First).await?;
        let before = node.snapshot()?;
        node.controls.reset();
        peer.transfer(POST, POST_HANDLE, Some(&transaction), format, &input)
            .await?;
        peer.detached(POST, POST_HANDLE, condition).await?;
        peer.barrier(CONTROL).await?;
        node.unchanged(&before)?;
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        assert!(node.messages("orders")?.is_empty());
        peer.healthy().await?;
        peer.close().await?;
    }
    node.stop().await;
    Ok(())
}

pub(super) async fn second_queue_cannot_join_a_bound_transaction<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.setup(ReceiverSettleMode::First).await?;
    peer.producer(SECOND, "other", ReceiverSettleMode::First)
        .await?;
    let transaction = peer.declare(ReceiverSettleMode::First).await?;
    let first = peer
        .transfer(
            POST,
            POST_HANDLE,
            Some(&transaction),
            0,
            &message("first", b"must roll back with later refusal"),
        )
        .await?;
    peer.provisional(POST, first, &transaction).await?;
    let before = node.snapshot()?;
    node.controls.reset();
    peer.transfer(
        SECOND,
        POST_HANDLE,
        Some(&transaction),
        0,
        &message("foreign", b"other queue"),
    )
    .await?;
    peer.detached(SECOND, POST_HANDLE, "amqp:not-allowed")
        .await?;
    peer.barrier(CONTROL).await?;
    node.unchanged(&before)?;
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
    assert!(node.messages("orders")?.is_empty());
    assert!(node.messages("other")?.is_empty());
    peer.healthy().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "native-orders-send";
const KEY: &str = "native-orders-secret";

async fn authenticated(
    address: std::net::SocketAddr,
    certificate: rustls::pki_types::CertificateDer<'static>,
) -> TestResult<Peer> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate)?;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = timeout(DEADLINE, TcpStream::connect(address)).await??;
    let mut tls = timeout(
        DEADLINE,
        tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(rustls::pki_types::ServerName::try_from("localhost")?, tcp),
    )
    .await??;
    timeout(
        DEADLINE,
        write_protocol_header(&mut tls, ProtocolHeader::SASL),
    )
    .await??;
    assert_eq!(
        timeout(DEADLINE, read_protocol_header(&mut tls)).await??,
        ProtocolHeader::SASL
    );
    assert!(matches!(
        timeout(DEADLINE, read_frame(&mut tls)).await??,
        Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
    ));
    let init = amqp::SaslInit {
        mechanism: Symbol::from("PLAIN"),
        initial_response: Some(
            [
                b"\0".as_slice(),
                RULE.as_bytes(),
                b"\0".as_slice(),
                KEY.as_bytes(),
            ]
            .concat()
            .into(),
        ),
        hostname: Some(HOST.into()),
    };
    timeout(
        DEADLINE,
        write_frame(&mut tls, &Frame::Sasl(amqp::SaslPerformative::Init(init))),
    )
    .await??;
    assert!(matches!(timeout(DEADLINE, read_frame(&mut tls)).await??,
        Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) if outcome.code == amqp::SaslCode::Ok));
    Peer::open(Box::new(tls)).await
}

pub(super) async fn scoped_authorization_precedes_missing_or_corrupt_target_admission<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let rule = auth::SharedAccessRule::new(
        RULE,
        auth::ResourceScope::entity(HOST, "orders")?,
        auth::SharedAccessKey::new(KEY)?,
        None,
        auth::PermissionSet::SEND,
    )?;
    let authentication = protocol_amqp::SharedAccessAuthentication::new(
        auth::SharedAccessPolicy::new([rule])?,
        HOST,
    )?;
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let tls = protocol_amqp::tls_server_config(
        cert.pem().as_bytes(),
        key_pair.serialize_pem().as_bytes(),
    )?;
    let certificate = cert.der().clone();
    let node = Node::start_with_security(provider, true, Some((tls, authentication))).await?;
    node.store.apply(WriteBatch::default().put(
        domain::keys::queue_config(&node.namespace, &EntityPath::new("other")?),
        vec![0xff],
    ))?;
    for target in ["Orders", "missing", "other"] {
        let mut peer = authenticated(node.address, certificate.clone()).await?;
        peer.begin(POST).await?;
        let before = node.snapshot()?;
        node.controls.reset();
        peer.attach(POST, POST_HANDLE, target, false, ReceiverSettleMode::First)
            .await?;
        peer.detached(POST, POST_HANDLE, "amqp:unauthorized-access")
            .await?;
        node.unchanged(&before)?;
        assert_eq!(
            node.controls.binds.load(Ordering::SeqCst),
            0,
            "denied scopes cannot probe target existence or corruption"
        );
        assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        peer.close().await?;
    }
    let mut peer = authenticated(node.address, certificate).await?;
    peer.setup(ReceiverSettleMode::First).await?;
    let transaction = peer.declare(ReceiverSettleMode::First).await?;
    node.controls.reset();
    let posting = peer
        .transfer(
            POST,
            POST_HANDLE,
            Some(&transaction),
            0,
            &message("authorized", b"send grant"),
        )
        .await?;
    peer.provisional(POST, posting, &transaction).await?;
    let control = peer.discharge(&transaction, false).await?;
    peer.final_outcome(POST, posting, ReceiverSettleMode::First, true)
        .await?;
    peer.final_outcome(CONTROL, control, ReceiverSettleMode::First, true)
        .await?;
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    assert_eq!(node.messages("orders")?.len(), 1);
    peer.close().await?;
    node.stop().await;
    Ok(())
}
