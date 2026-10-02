use super::*;

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "sdk-defaults-private-key";
const SEND: &str = "sdk-defaults-send-only";

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
    tcp.set_nodelay(true)?;
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
                SEND.as_bytes(),
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
    assert!(matches!(
        timeout(DEADLINE, read_frame(&mut tls)).await??,
        Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) if outcome.code == amqp::SaslCode::Ok
    ));
    Peer::open(Box::new(tls)).await
}

pub(in super::super) async fn mixed_receiver_still_requires_listen_before_bind<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let rule = auth::SharedAccessRule::new(
        SEND,
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
    let node = Node::start_with_security(
        provider,
        ListenerMode::Messaging,
        Some((tls, authentication)),
    )
    .await?;
    node.store.apply(WriteBatch::default().put(
        domain::keys::queue_config(&node.namespace, &EntityPath::new("other")?),
        vec![0xff],
    ))?;
    for target in ["orders", "Orders", "missing", "other"] {
        let mut peer = authenticated(node.address, certificate.clone()).await?;
        peer.coordinator_with_sdk_defaults().await?;
        peer.begin(RECEIVE).await?;
        let mut request = Peer::attach_request(RECEIVE, RECEIVE_HANDLE, target, Role::Receiver);
        request.snd_settle_mode = SenderSettleMode::Mixed;
        let before = node.snapshot()?;
        node.controls.reset_io();
        peer.attach(RECEIVE, request).await?;
        peer.detached(
            RECEIVE,
            RECEIVE_HANDLE,
            Some("amqp:unauthorized-access"),
            true,
        )
        .await?;
        node.unchanged(&before)?;
        assert_eq!(node.controls.binds.load(Ordering::SeqCst), 0, "{target}");
        assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        peer.producer(POST, "orders").await?;
        peer.barrier(POST).await?;
        peer.close().await?;
    }
    node.stop().await;
    Ok(())
}
