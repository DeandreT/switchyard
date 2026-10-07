use super::*;

fn config(ttl: Option<u64>, deliveries: u32) -> QueueConfig {
    QueueConfig {
        default_time_to_live_millis: ttl,
        max_delivery_count: deliveries,
        duplicate_detection_history_time_window_millis: 60_000,
        ..QueueConfig::default()
    }
}

pub(super) async fn crud<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        let token = management_token();
        let (status, created) = exchange(&node, Method::PUT, "/orders?api-version=2024-05", Some(token.clone()), CREATE, false).await?;
        assert_eq!(status, StatusCode::CREATED);
        let (status, read) = exchange(&node, Method::GET, "/orders?api-version=2024-05&enrich=False", Some(token.clone()), b"", false).await?;
        assert_eq!(status, StatusCode::OK); assert_eq!(read, created);
        let view = node.handle().get_atom_finite_queue(NamespaceName::new("tenant")?, EntityPath::new("orders")?).await?.unwrap();
        assert_eq!(view.config, config(Some(60_000), 10));
        let (status, listed) = exchange(&node, Method::GET, "/$Resources/queues?api-version=2024-05&enrich=False&$skip=0&$top=100", Some(token.clone()), b"", false).await?;
        assert_eq!(status, StatusCode::OK);
        let listed = String::from_utf8(listed)?;
        assert_eq!(listed.matches("<entry").count(), 1); assert!(listed.contains("orders")); assert!(!listed.contains("$deadletterqueue"));
        let (status, replaced) = exchange(&node, Method::PUT, "/orders?api-version=2021-05", Some(token.clone()), REPLACE, true).await?;
        assert_eq!(status, StatusCode::OK); assert!(!replaced.is_empty());
        let (status, read) = exchange(&node, Method::GET, "/orders?api-version=2021-05", Some(token.clone()), b"", false).await?;
        assert_eq!(status, StatusCode::OK); assert_eq!(read, replaced);
        let actual = node.handle().get_atom_finite_queue(NamespaceName::new("tenant")?, EntityPath::new("orders")?).await?.unwrap();
        assert_eq!(actual.config, config(None, 4), "full PUT resets omitted TTL and inactive duplicate history");
        assert!(matches!(actual.capacity, domain::QueueCapacityStatus::FiniteV1 { limit, .. } if limit.bytes() == 2 * MIB));

        let oracle_store = MemoryStore::default();
        let oracle = Broker::spawn(LocalProposer::new(StateMachine::new(oracle_store.clone()), server::ManualClock::at(1_000)));
        let namespace = NamespaceName::new("tenant")?; let entity = EntityPath::new("orders")?;
        oracle.handle().create_finite_queue_blocking(namespace.clone(), entity.clone(), config(Some(60_000), 10), FiniteQueueCapacity::new(MIB)?)?;
        oracle.handle().update_atom_finite_queue_blocking(namespace.clone(), entity.clone(), config(None, 4), FiniteQueueCapacity::new(2 * MIB)?)?;
        assert_eq!(node.snapshot()?, oracle_store.snapshot()?, "the entire business image matches the canonical Memory owner path");
        let (status, deleted) = exchange(&node, Method::DELETE, "/orders?api-version=2024-05", Some(token.clone()), b"", false).await?;
        assert_eq!(status, StatusCode::OK); assert!(deleted.is_empty());
        oracle.handle().delete_atom_finite_queue_blocking(namespace, entity)?;
        let (status, missing) = exchange(&node, Method::GET, "/orders?api-version=2024-05", Some(token), b"", false).await?;
        assert_eq!(status, StatusCode::NOT_FOUND); assert!(!missing.is_empty());
        let image = node.snapshot()?; assert_eq!(image, oracle_store.snapshot()?);
        expected = Some(image);
        Ok(())
    }).catch_unwind().await;
    let provider = node.finish(outcome).await?;
    let reopened = provider.open()?;
    assert_eq!(
        reopened.snapshot()?,
        expected.unwrap(),
        "all listener/broker/store handles were dropped before reopen"
    );
    Ok(())
}

pub(super) async fn refusals<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        let before = node.snapshot()?;
        let effects = node.effects();
        for authorization in [
            None,
            Some(token(&format!("https://{HOST}"), "send", epoch() + 300)),
            Some(token(
                &format!("https://{HOST}/other"),
                "manage",
                epoch() + 300,
            )),
            Some(token(
                &format!("https://{HOST}:444"),
                "manage",
                epoch() + 300,
            )),
            Some(token(&format!("amqps://{HOST}"), "manage", epoch() + 300)),
            Some(token(&format!("https://{HOST}"), "manage", epoch())),
        ] {
            let (status, body) = exchange(
                &node,
                Method::PUT,
                "/orders?api-version=2024-05",
                authorization,
                b"<invalid secret='producer payload'",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            let public = String::from_utf8(body)?;
            assert!(!public.contains("secret"));
            assert!(!public.contains(KEY));
            assert!(!public.contains("InvalidXml"));
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/orders?api-version=2024-05",
            Some(management_token()),
            b"<invalid",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(node.effects(), effects);
        assert_eq!(node.snapshot()?, before);
        let oversized = vec![b'x'; 65_537];
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/orders?api-version=2024-05",
            Some(management_token()),
            &oversized,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(node.effects(), effects);
        assert_eq!(node.snapshot()?, before);
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/healthy?api-version=2024-05",
            Some(management_token()),
            CREATE,
            false,
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "a fresh verified connection remains usable after refusals"
        );
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

pub(super) async fn literal_and_quota<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        let literal = "Orders/$Management/%23literal/%252F";
        let audience = format!("https://{HOST}/{literal}");
        let token = token(&audience, "manage", epoch() + 300);
        let (status, created) = exchange(
            &node,
            Method::PUT,
            &format!("/{literal}?api-version=2024-05"),
            Some(token.clone()),
            CREATE,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert!(String::from_utf8(created)?.contains("Orders/$Management/#literal/%2F"));
        let before = node.snapshot()?;
        let effects = node.effects();
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/Orders/$management/%23literal/%252F?api-version=2024-05",
            Some(token),
            CREATE,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(node.effects(), effects);
        assert_eq!(node.snapshot()?, before);
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/$resources/queues?api-version=2024-05",
            Some(management_token()),
            CREATE,
            false,
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "only the exact collection spelling is reserved"
        );
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/capacity?api-version=2024-05",
            Some(management_token()),
            REPLACE,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let handle = node.handle();
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("capacity")?;
        for index in 0..5 {
            handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::Send {
                    message_id: format!("quota-{index}"),
                    body: vec![1; 256 * 1_024],
                    time_to_live_millis: None,
                    session_id: None,
                },
            )?;
        }
        let before = node.snapshot()?;
        let (status, error) = exchange(
            &node,
            Method::PUT,
            "/capacity?api-version=2024-05",
            Some(management_token()),
            CREATE,
            true,
        )
        .await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            node.snapshot()?,
            before,
            "combined configuration and limit were refused atomically"
        );
        assert!(String::from_utf8(error)?.contains("QuotaExceeded"));
        let (status, _) = exchange(
            &node,
            Method::GET,
            "/capacity?api-version=2024-05",
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

pub(super) async fn tls_refusals<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        let before = node.snapshot()?;
        let effects = node.effects();
        for (trust, name) in [(false, "localhost"), (true, "wrong.example")] {
            let connector = connector(if trust { Some(&node.certificate) } else { None })?;
            let stream = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
            let error = match timeout(
                DEADLINE,
                connector.connect(ServerName::try_from(name.to_owned())?, stream),
            )
            .await?
            {
                Err(error) => error,
                Ok(_) => return Err("invalid certificate trust or name was accepted".into()),
            };
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            let cause = error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<rustls::Error>());
            if trust {
                assert!(
                    matches!(
                        cause,
                        Some(rustls::Error::InvalidCertificate(
                            rustls::CertificateError::NotValidForNameContext { .. }
                        ))
                    ),
                    "the refusal is specifically a certificate-name mismatch"
                );
            } else {
                assert!(
                    matches!(
                        cause,
                        Some(rustls::Error::InvalidCertificate(
                            rustls::CertificateError::UnknownIssuer
                        ))
                    ),
                    "the refusal is specifically an untrusted certificate issuer"
                );
            }
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        let mut plaintext = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
        timeout(
            DEADLINE,
            plaintext
                .write_all(b"GET /orders?api-version=2024-05 HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        )
        .await??;
        let mut bytes = [0; 64];
        let read = timeout(DEADLINE, plaintext.read(&mut bytes)).await?;
        assert!(
            matches!(read, Ok(0) | Err(_)) || bytes[0] == 21,
            "plaintext does not receive an HTTP response"
        );
        assert_eq!(node.effects(), effects);
        assert_eq!(node.snapshot()?, before);
        drop(plaintext);
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/healthy-after-tls-refusals?api-version=2024-05",
            Some(management_token()),
            CREATE,
            false,
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "the same fixture accepts a verified authorized request after the TLS negatives"
        );
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

pub(super) async fn shutdown<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        let before = node.snapshot()?;
        let connector = connector(Some(&node.certificate))?;
        let stream = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
        let mut accepted = timeout(
            DEADLINE,
            connector.connect(ServerName::try_from("localhost".to_owned())?, stream),
        )
        .await??;
        timeout(
            DEADLINE,
            accepted.write_all(b"GET /orders?api-version=2024-05 HTTP/1.1\r\nHost: localhost\r\n"),
        )
        .await??;
        let mut streams = Vec::new();
        for _ in 0..4 {
            let mut stream = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
            timeout(DEADLINE, stream.write_all(&[22, 3, 3, 0, 1])).await??;
            streams.push(stream);
        }
        // The verified TLS stream proves acceptance by an original connection
        // future; additional raw streams may still be queued for accept.
        node.stop().await?;
        let mut byte = [0];
        assert!(
            matches!(
                timeout(DEADLINE, accepted.read(&mut byte)).await?,
                Ok(0) | Err(_)
            ),
            "shutdown closes the original accepted TLS connection"
        );
        for mut stream in streams {
            let mut byte = [0];
            assert!(matches!(
                timeout(DEADLINE, stream.read(&mut byte)).await?,
                Ok(0) | Err(_)
            ));
        }
        assert_eq!(node.effects(), (0, 0));
        assert_eq!(node.snapshot()?, before);
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

async fn bounded_eof(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> TestResult<Duration> {
    let began = tokio::time::Instant::now();
    let mut total = 0usize;
    let mut bytes = [0; 1_024];
    loop {
        match stream.read(&mut bytes).await {
            Ok(0) | Err(_) => return Ok(began.elapsed()),
            Ok(count) => {
                total += count;
                if total > 16_384 {
                    return Err("idle connection returned excessive data".into());
                }
            }
        }
    }
}

pub(super) async fn deadlines<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        let mut handshake = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
        timeout(DEADLINE, handshake.write_all(&[22, 3, 3, 0, 1])).await??;
        let connector = connector(Some(&node.certificate))?;
        let stream = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
        let mut headers = timeout(
            DEADLINE,
            connector.connect(ServerName::try_from("localhost".to_owned())?, stream),
        )
        .await??;
        timeout(
            DEADLINE,
            headers.write_all(b"GET /orders?api-version=2024-05 HTTP/1.1\r\nHost: localhost\r\n"),
        )
        .await??;
        let (handshake, headers) = timeout(Duration::from_secs(15), async {
            tokio::join!(bounded_eof(&mut handshake), bounded_eof(&mut headers))
        })
        .await?;
        assert!(
            handshake? >= Duration::from_secs(8),
            "the partial TLS handshake is retained until its own timer"
        );
        assert!(
            headers? >= Duration::from_secs(8),
            "the partial HTTP header is retained until its own timer"
        );
        assert_eq!(node.effects(), (0, 0));
        let (status, _) = exchange(
            &node,
            Method::PUT,
            "/healthy-after-timeouts?api-version=2024-05",
            Some(management_token()),
            CREATE,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

pub(super) async fn admission_limit<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        let connector = connector(Some(&node.certificate))?;
        // A completed verified client handshake proves TCP acceptance; finish
        // filling well before the first ten-second HTTP header deadline.
        let accepted = timeout(Duration::from_secs(4), async {
            let mut accepted = Vec::new();
            for _ in 0..128 {
                let stream = TcpStream::connect(node.address).await?;
                accepted.push(
                    connector
                        .connect(ServerName::try_from("localhost".to_owned())?, stream)
                        .await?,
                );
            }
            Ok::<_, Box<dyn Error>>(accepted)
        })
        .await??;
        let mut excess = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
        let mut byte = [0];
        assert!(
            matches!(
                timeout(Duration::from_secs(2), excess.read(&mut byte)).await?,
                Ok(0) | Err(_)
            ),
            "the 129th connection is refused before HTTP or a handshake"
        );
        assert_eq!(node.effects(), (0, 0));
        drop(accepted);
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}
