use super::*;

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "mixed-native-private-key";
const SEND: &str = "send-only";
const LISTEN: &str = "listen-only";
const BOTH: &str = "send-and-listen";

async fn authenticated(
    address: std::net::SocketAddr,
    certificate: rustls::pki_types::CertificateDer<'static>,
    rule: &str,
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
                rule.as_bytes(),
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
    assert!(
        matches!(timeout(DEADLINE, read_frame(&mut tls)).await??, Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) if outcome.code == amqp::SaslCode::Ok)
    );
    Peer::open(Box::new(tls)).await
}

pub(in super::super) async fn listen_and_send_permissions_precede_target_binding<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let scope = auth::ResourceScope::entity(HOST, "orders")?;
    let key = auth::SharedAccessKey::new(KEY)?;
    let rules = [
        (SEND, auth::PermissionSet::SEND),
        (LISTEN, auth::PermissionSet::LISTEN),
        (
            BOTH,
            auth::PermissionSet::SEND | auth::PermissionSet::LISTEN,
        ),
    ]
    .into_iter()
    .map(|(name, permissions)| {
        auth::SharedAccessRule::new(name, scope.clone(), key.clone(), None, permissions)
    })
    .collect::<Result<Vec<_>, auth::PolicyError>>()?;
    let authentication = protocol_amqp::SharedAccessAuthentication::new(
        auth::SharedAccessPolicy::new(rules)?,
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
    for (rule, role, channel, handle) in [
        (SEND, Role::Receiver, RECEIVE, RECEIVE_HANDLE),
        (LISTEN, Role::Sender, POST, POST_HANDLE),
    ] {
        for target in ["orders", "Orders", "missing", "other"] {
            let mut peer = authenticated(node.address, certificate.clone(), rule).await?;
            peer.begin(channel).await?;
            let before = node.snapshot()?;
            node.controls.reset_io();
            peer.attach(
                channel,
                Peer::attach_request(channel, handle, target, role.clone()),
            )
            .await?;
            peer.detached(channel, handle, Some("amqp:unauthorized-access"), true)
                .await?;
            node.unchanged(&before)?;
            assert_eq!(
                node.controls.binds.load(Ordering::SeqCst),
                0,
                "{rule}/{target} must be refused before lookup"
            );
            assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
            assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 0);
            assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
            peer.close().await?;
        }
    }
    let mut peer = authenticated(node.address, certificate, BOTH).await?;
    let original = peer.setup().await?;
    lifecycle::assert_canonical_hold(&node, &original)?;
    let (transaction, post) = lifecycle::stage_mixed(&mut peer, &original).await?;
    node.controls.reset_io();
    let control = peer.discharge(&transaction, false).await?;
    peer.committed(&original, post, control).await?;
    node.controls.wait_receive_starts(2).await?;
    assert!(node.record()?.is_none());
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.states(), [AtomicCommitState::Committed]);
    assert_eq!(node.messages()?.len(), 2);
    peer.close().await?;
    node.stop().await;
    Ok(())
}
