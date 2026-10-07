use super::*;

#[derive(Clone)]
pub(super) struct ProbeStore<S> {
    pub(super) inner: S,
    pub(super) calls: Arc<AtomicUsize>,
}
impl<S: StateStore> StateStore for ProbeStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
}

#[derive(Clone)]
pub(super) struct ProbeClock(pub(super) Arc<AtomicUsize>);
impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(1_000)
    }
}

pub(super) fn signed_certificate() -> TestResult<(ServerConfig, CertificateDer<'static>)> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate()?;
    let ca = ca_params.self_signed(&ca_key)?;
    let mut leaf_params = CertificateParams::new(vec!["localhost".into()])?;
    leaf_params.use_authority_key_identifier_extension = true;
    leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate()?;
    let leaf = leaf_params.signed_by(&key, &ca, &ca_key)?;
    let tls =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone(), ca.der().clone()],
                PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )?;
    Ok((tls, ca.der().clone()))
}

pub(super) fn connector(certificate: Option<&CertificateDer<'static>>) -> TestResult<TlsConnector> {
    let mut roots = RootCertStore::empty();
    if let Some(certificate) = certificate {
        roots.add(certificate.clone())?;
    }
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsConnector::from(Arc::new(config)))
}

pub(super) fn token(audience: &str, rule: &str, expiry: u64) -> String {
    let resource = url::form_urlencoded::byte_serialize(audience.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = url::form_urlencoded::byte_serialize(signature.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

pub(super) fn management_token() -> String {
    token(&format!("https://{HOST}"), "manage", epoch() + 300)
}
pub(super) fn epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) broker: Option<Broker>,
    pub(super) store: Option<ProbeStore<P::Store>>,
    pub(super) clock: ProbeClock,
    pub(super) address: SocketAddr,
    pub(super) certificate: CertificateDer<'static>,
    shutdown: Option<oneshot::Sender<()>>,
    listener: Option<JoinHandle<Result<(), server::AtomAdminError>>>,
    provider: Option<P>,
}

impl<P: StoreProvider> Node<P> {
    pub(super) async fn start(provider: P) -> TestResult<Self> {
        let store = ProbeStore {
            inner: provider.open()?,
            calls: Arc::default(),
        };
        let clock = ProbeClock(Arc::default());
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let scope = ResourceScope::namespace(HOST)?;
        let policy = SharedAccessPolicy::new([
            SharedAccessRule::new(
                "manage",
                scope.clone(),
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?,
            SharedAccessRule::new(
                "send",
                scope.clone(),
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::SEND,
            )?,
        ])?;
        let (tls, certificate) = signed_certificate()?;
        let admin = AtomAdminListener::new(
            broker.handle(),
            NamespaceName::new("tenant")?,
            policy,
            scope,
            tls,
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, stopped) = oneshot::channel();
        let listener = tokio::spawn(admin.serve_until(listener, async move {
            let _ = stopped.await;
        }));
        Ok(Self {
            broker: Some(broker),
            store: Some(store),
            clock,
            address,
            certificate,
            shutdown: Some(shutdown),
            listener: Some(listener),
            provider: Some(provider),
        })
    }

    pub(super) fn handle(&self) -> server::BrokerHandle {
        self.broker.as_ref().unwrap().handle()
    }
    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.as_ref().unwrap().inner.snapshot()?)
    }
    pub(super) fn effects(&self) -> (usize, usize) {
        (
            self.store.as_ref().unwrap().calls.load(Ordering::SeqCst),
            self.clock.0.load(Ordering::SeqCst),
        )
    }

    pub(super) async fn stop(&mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let mut failure: Option<Box<dyn Error>> = None;
        if let Some(mut listener) = self.listener.take() {
            match timeout(DEADLINE, &mut listener).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => failure = Some(Box::new(error)),
                Ok(Err(error)) => failure = Some(Box::new(error)),
                Err(_) => {
                    listener.abort();
                    let joined = timeout(DEADLINE, &mut listener).await;
                    failure = Some(if joined.is_err() {
                        "original listener failed to join after abort".into()
                    } else {
                        "listener shutdown deadline expired".into()
                    });
                }
            }
        }
        // The original broker stays owned until all listener futures are dropped.
        drop(self.broker.take());
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) async fn finish(
        mut self,
        outcome: Result<TestResult, Box<dyn std::any::Any + Send>>,
    ) -> TestResult<P> {
        let cleanup = self.stop().await;
        match outcome {
            Err(panic) => resume_unwind(panic),
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => {
                cleanup?;
                drop(self.store.take());
                Ok(self.provider.take().unwrap())
            }
        }
    }
}

impl<P: StoreProvider> Drop for Node<P> {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        drop(self.broker.take());
    }
}

pub(super) async fn exchange<P: StoreProvider>(
    node: &Node<P>,
    method: Method,
    path: &str,
    authorization: Option<String>,
    body: &[u8],
    update: bool,
) -> TestResult<(StatusCode, Vec<u8>)> {
    let tls = connector(Some(&node.certificate))?;
    let stream = timeout(DEADLINE, async {
        let stream = TcpStream::connect(node.address).await?;
        Ok::<_, Box<dyn Error>>(
            tls.connect(ServerName::try_from("localhost".to_owned())?, stream)
                .await?,
        )
    })
    .await??;
    let (mut sender, connection) = timeout(
        DEADLINE,
        hyper::client::conn::http1::handshake::<_, Full<Bytes>>(TokioIo::new(stream)),
    )
    .await??;
    let mut driver = tokio::spawn(connection);
    let outcome = AssertUnwindSafe(async {
        let mut input = Request::builder()
            .method(method)
            .uri(path)
            .header(
                "host",
                format!("untrusted-host.example:{}", node.address.port()),
            )
            .header("content-type", "application/atom+xml");
        if let Some(token) = authorization {
            input = input.header("authorization", token);
        }
        if update {
            input = input.header("if-match", "*");
        }
        let response = timeout(
            DEADLINE,
            sender.send_request(input.body(Full::new(Bytes::copy_from_slice(body)))?),
        )
        .await??;
        assert_eq!(response.headers().get("connection").unwrap(), "close");
        let status = response.status();
        let reply = timeout(
            DEADLINE,
            Limited::new(response.into_body(), 1_048_576).collect(),
        )
        .await?
        .map_err(|error| -> Box<dyn Error> { error })?
        .to_bytes()
        .to_vec();
        Ok::<_, Box<dyn Error>>((status, reply))
    })
    .catch_unwind()
    .await;
    drop(sender);
    driver.abort();
    let cleanup: TestResult = match timeout(DEADLINE, &mut driver).await {
        Ok(Err(error)) if error.is_cancelled() => Ok(()),
        Ok(Ok(Ok(()))) => Ok(()),
        // A complete close response can precede a broken pipe sending TLS close_notify.
        Ok(Ok(Err(error)))
            if matches!(&outcome, Ok(Ok(_)))
                && error.is_shutdown()
                && error
                    .source()
                    .and_then(|source| source.downcast_ref::<std::io::Error>())
                    .is_some_and(|source| source.kind() == std::io::ErrorKind::BrokenPipe) =>
        {
            Ok(())
        }
        Ok(Ok(Err(error))) => Err(Box::new(error)),
        Ok(Err(error)) => Err(Box::new(error)),
        Err(_) => Err("original HTTP client driver did not join before its deadline".into()),
    };
    match outcome {
        Err(panic) => resume_unwind(panic),
        Ok(Err(error)) => Err(error),
        Ok(Ok(reply)) => {
            cleanup?;
            Ok(reply)
        }
    }
}
