use super::*;

const CBS_CHANNEL: u16 = 41;
const REQUEST_HANDLE: u32 = 59;
const RESPONSE_HANDLE: u32 = 61;
const REPLY_TO: &str = "cold-first-cbs-replies";

impl Peer {
    pub(in super::super) async fn connect_cbs(
        address: std::net::SocketAddr,
        certificate: rustls::pki_types::CertificateDer<'static>,
        mechanism: &str,
    ) -> TestResult<Self> {
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
        let Frame::Sasl(amqp::SaslPerformative::Mechanisms(offered)) =
            timeout(DEADLINE, read_frame(&mut tls)).await??
        else {
            return Err("actual SASL mechanisms missing".into());
        };
        assert!(
            offered
                .mechanisms
                .iter()
                .any(|offered| { offered.as_str() == mechanism })
        );
        timeout(
            DEADLINE,
            write_frame(
                &mut tls,
                &Frame::Sasl(amqp::SaslPerformative::Init(amqp::SaslInit {
                    mechanism: Symbol::from(mechanism),
                    initial_response: None,
                    hostname: Some("tenant.servicebus.windows.net".into()),
                })),
            ),
        )
        .await??;
        assert!(matches!(
            timeout(DEADLINE, read_frame(&mut tls)).await??,
            Frame::Sasl(amqp::SaslPerformative::Outcome(outcome))
                if outcome.code == amqp::SaslCode::Ok
        ));
        Self::open(Box::new(tls)).await
    }

    pub(in super::super) async fn open_cbs(&mut self) -> TestResult {
        self.begin_with_handle_max(CBS_CHANNEL, 1).await?;
        self.attach(
            CBS_CHANNEL,
            Self::attach_request(
                CBS_CHANNEL,
                REQUEST_HANDLE,
                protocol_amqp::CBS_NODE,
                Role::Sender,
            ),
        )
        .await?;
        self.admitted(CBS_CHANNEL, Role::Sender).await?;
        let mut response = Self::attach_request(
            CBS_CHANNEL,
            RESPONSE_HANDLE,
            protocol_amqp::CBS_NODE,
            Role::Receiver,
        );
        response.name = "cold-first-cbs-response".into();
        response.target = Some(Target::new(REPLY_TO).into());
        response.rcv_settle_mode = ReceiverSettleMode::First;
        self.attach(CBS_CHANNEL, response).await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(attach)),
                payload,
            } = &frame
            {
                assert_eq!(*channel, self.local(CBS_CHANNEL));
                assert_eq!(attach.handle, 1);
                assert_eq!(attach.role, Role::Sender);
                assert_eq!(attach.rcv_settle_mode, ReceiverSettleMode::First);
                assert!(payload.is_empty());
                self.handles.entry(*channel).or_default().insert(1);
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected CBS admission: {frame:?}"
            );
        }
        Err("bounded CBS response admission missing".into())
    }

    pub(in super::super) async fn put_token(
        &mut self,
        audience: &str,
        token: String,
    ) -> TestResult<i32> {
        let replies = self
            .received
            .get(&self.local(CBS_CHANNEL))
            .copied()
            .unwrap_or(0);
        self.send(
            CBS_CHANNEL,
            Performative::Flow(Flow {
                next_incoming_id: Some(replies),
                incoming_window: 2_048,
                next_outgoing_id: self.sent[&CBS_CHANNEL],
                outgoing_window: 2_048,
                handle: Some(RESPONSE_HANDLE),
                delivery_count: Some(replies),
                link_credit: Some(1),
                ..Flow::default()
            }),
            vec![],
        )
        .await?;
        let message_id = format!("cold-first-token-{}", self.deliveries[&CBS_CHANNEL]);
        let message = Message::builder()
            .properties(Properties {
                message_id: Some(message_id.clone().into()),
                reply_to: Some(REPLY_TO.into()),
                ..Properties::default()
            })
            .application_properties(
                amqp::ApplicationProperties::builder()
                    .insert("operation", "put-token")
                    .insert("type", "servicebus.windows.net:sastoken")
                    .insert("name", audience.to_owned())
                    .build(),
            )
            .body(Body::Value(Value::String(token)))
            .build();
        let request = self
            .transfer(CBS_CHANNEL, REQUEST_HANDLE, None, 0, &message)
            .await?;
        let mut accepted = false;
        let mut status = None;
        for _ in 0..32 {
            let frame = self.read().await?;
            match frame {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Disposition(disposition)),
                    payload,
                } => {
                    assert_eq!(channel, self.local(CBS_CHANNEL));
                    assert!(payload.is_empty());
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, request);
                    assert!(disposition.last.is_none() && disposition.settled && !accepted);
                    assert!(matches!(
                        disposition.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                    accepted = true;
                }
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                } => {
                    assert_eq!(channel, self.local(CBS_CHANNEL));
                    assert_eq!(transfer.handle, 1);
                    assert_eq!(transfer.message_format, Some(0));
                    assert_eq!(transfer.settled, Some(false));
                    assert!(!transfer.more && transfer.state.is_none() && status.is_none());
                    let response = decode_message(&payload)?;
                    assert_eq!(
                        response
                            .properties
                            .as_ref()
                            .and_then(|properties| properties.correlation_id.as_ref()),
                        Some(&amqp::MessageId::from(message_id.clone())),
                    );
                    let Some(Value::Int(code)) = response
                        .application_properties
                        .as_ref()
                        .and_then(|properties| properties.get("status-code"))
                    else {
                        return Err("typed CBS status missing".into());
                    };
                    status = Some(*code);
                    self.send(
                        CBS_CHANNEL,
                        Performative::Disposition(Disposition {
                            role: Role::Receiver,
                            first: transfer.delivery_id.ok_or("CBS reply identity missing")?,
                            last: None,
                            settled: true,
                            state: Some(DeliveryState::Accepted(amqp::Accepted)),
                            batchable: false,
                        }),
                        vec![],
                    )
                    .await?;
                }
                frame => assert!(self.valid_flow(&frame), "unexpected CBS reply: {frame:?}"),
            }
            if accepted && let Some(status) = status {
                return Ok(status);
            }
        }
        Err("bounded CBS token response missing".into())
    }

    pub(in super::super) async fn unauthorized_controller(&mut self) -> TestResult {
        let (channel, performative) = self.control().await?;
        assert_eq!(channel, self.local(CONTROL));
        let Performative::Detach(detach) = performative else {
            return Err("unauthorized controller must detach without a successful outcome".into());
        };
        assert_eq!(detach.handle, 0);
        assert!(detach.closed);
        let error = detach.error.ok_or("authorization Detach error missing")?;
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:unauthorized-access"
        );
        assert_eq!(
            error.description.as_deref(),
            Some("the coordinator's authorization has expired")
        );
        self.request_detach(CONTROL, CONTROL_HANDLE).await?;
        self.barrier(CONTROL).await
    }

    pub(in super::super) async fn initial_authorization_close(&mut self) -> TestResult {
        let (channel, performative) = self.control().await?;
        assert_eq!(channel, 0);
        let Performative::Close(close) = performative else {
            return Err("initial authorization timeout must close the connection".into());
        };
        let error = close.error.ok_or("authorization Close error missing")?;
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:unauthorized-access"
        );
        assert_eq!(
            error.description.as_deref(),
            Some("no CBS token was supplied before the authorization deadline")
        );
        self.send(0, Performative::Close(Close::default()), vec![])
            .await
    }
}
