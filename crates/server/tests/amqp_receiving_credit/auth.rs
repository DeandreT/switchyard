use super::*;

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use url::form_urlencoded::byte_serialize;

#[path = "auth/guarded.rs"]
pub(super) mod guarded;
#[path = "auth/pipeline.rs"]
pub(super) mod pipeline;

const HOST: &str = "tenant.servicebus.windows.net";
const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";
const RULE: &str = "receive-credit-listen";
const KEY: &str = "receive-credit-private-key";
const CBS: u16 = 41;
const REQUEST: u32 = 59;
const RESPONSE: u32 = 61;
const REPLY: &str = "receive-credit-cbs-response";

struct Security {
    tls: rustls::ServerConfig,
    authentication: protocol_amqp::SharedAccessAuthentication,
    certificate: rustls::pki_types::CertificateDer<'static>,
}

impl Security {
    fn new() -> TestResult<Self> {
        let rule = ::auth::SharedAccessRule::new(
            RULE,
            ::auth::ResourceScope::entity(HOST, "orders")?,
            ::auth::SharedAccessKey::new(KEY)?,
            None,
            ::auth::PermissionSet::LISTEN,
        )?;
        let authentication = protocol_amqp::SharedAccessAuthentication::new(
            ::auth::SharedAccessPolicy::new([rule])?,
            HOST,
        )?;
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let tls = protocol_amqp::tls_server_config(
            cert.pem().as_bytes(),
            key_pair.serialize_pem().as_bytes(),
        )?;
        Ok(Self {
            tls,
            authentication,
            certificate: cert.der().clone(),
        })
    }

    async fn connect(&self, address: std::net::SocketAddr) -> TestResult<Peer> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.certificate.clone())?;
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
        assert!(matches!(timeout(DEADLINE, read_frame(&mut tls)).await??,
            Frame::Sasl(amqp::SaslPerformative::Mechanisms(mechanisms))
            if mechanisms.mechanisms.iter().any(|mechanism| mechanism.as_str() == "MSSBCBS")));
        timeout(
            DEADLINE,
            write_frame(
                &mut tls,
                &Frame::Sasl(amqp::SaslPerformative::Init(amqp::SaslInit {
                    mechanism: Symbol::from("MSSBCBS"),
                    initial_response: None,
                    hostname: Some(HOST.into()),
                })),
            ),
        )
        .await??;
        assert!(matches!(timeout(DEADLINE, read_frame(&mut tls)).await??,
            Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) if outcome.code == amqp::SaslCode::Ok));
        Peer::open(Box::new(tls)).await
    }
}

fn token(expiry: u64) -> TestResult<String> {
    let resource: String = byte_serialize(AUDIENCE.as_bytes()).collect();
    let mut hmac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes())?;
    hmac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(hmac.finalize().into_bytes());
    let signature: String = byte_serialize(signature.as_bytes()).collect();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={RULE}"
    ))
}

async fn open_cbs(peer: &mut Peer) -> TestResult {
    peer.begin(CBS, 0, 1).await?;
    for (name, handle, role, local_handle) in [
        ("credit-cbs-request", REQUEST, Role::Sender, 0),
        ("credit-cbs-response", RESPONSE, Role::Receiver, 1),
    ] {
        peer.attach(
            CBS,
            Attach {
                name: name.into(),
                handle,
                role: role.clone(),
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: Some(Source::new(protocol_amqp::CBS_NODE)),
                target: Some(
                    Target::new(if role == Role::Sender {
                        protocol_amqp::CBS_NODE
                    } else {
                        REPLY
                    })
                    .into(),
                ),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: (role == Role::Sender).then_some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            },
            local_handle,
        )
        .await?;
    }
    Ok(())
}

async fn put_token(peer: &mut Peer, token: String) -> TestResult {
    let mut flow = peer.flow(CBS);
    flow.handle = Some(RESPONSE);
    flow.delivery_count = Some(0);
    flow.link_credit = Some(1);
    peer.send(CBS, Performative::Flow(flow), vec![]).await?;
    let message = Message::builder()
        .properties(Properties {
            message_id: Some("credit-token".into()),
            reply_to: Some(REPLY.into()),
            ..Properties::default()
        })
        .application_properties(
            amqp::ApplicationProperties::builder()
                .insert("operation", "put-token")
                .insert("type", "servicebus.windows.net:sastoken")
                .insert("name", AUDIENCE)
                .build(),
        )
        .body(Body::Value(Value::String(token)))
        .build();
    peer.send(
        CBS,
        Performative::Transfer(amqp::Transfer {
            handle: REQUEST,
            delivery_id: Some(0),
            delivery_tag: Some(vec![0].into()),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }),
        encode_message(&message)?,
    )
    .await?;
    let mut accepted = false;
    let mut responded = false;
    for _ in 0..32 {
        let frame = peer.read().await?;
        match &frame {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Disposition(disposition)),
                payload,
            } => {
                assert_eq!(*channel, peer.local(CBS));
                assert_eq!(disposition.role, Role::Receiver);
                assert_eq!(disposition.first, 0);
                assert!(
                    disposition.last.is_none()
                        && disposition.settled
                        && payload.is_empty()
                        && !accepted
                );
                assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                accepted = true;
            }
            Frame::Amqp {
                channel,
                performative: Some(Performative::Transfer(transfer)),
                payload,
            } => {
                assert_eq!(*channel, peer.local(CBS));
                assert_eq!(transfer.handle, 1);
                assert_eq!(transfer.message_format, Some(0));
                assert!(!transfer.more && transfer.settled == Some(false) && !responded);
                let reply = decode_message(payload)?;
                assert_eq!(
                    reply
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.correlation_id.as_ref()),
                    Some(&amqp::MessageId::from("credit-token"))
                );
                assert_eq!(
                    reply
                        .application_properties
                        .as_ref()
                        .and_then(|properties| properties.get("status-code")),
                    Some(&Value::Int(202))
                );
                peer.send(
                    CBS,
                    Performative::Disposition(Disposition {
                        role: Role::Receiver,
                        first: transfer.delivery_id.ok_or("CBS reply id missing")?,
                        last: None,
                        settled: true,
                        state: Some(DeliveryState::Accepted(Accepted)),
                        batchable: false,
                    }),
                    vec![],
                )
                .await?;
                responded = true;
            }
            frame => assert!(peer.valid_flow(frame), "unexpected CBS response: {frame:?}"),
        }
        if accepted && responded {
            return Ok(());
        }
    }
    Err("bounded accepted CBS response missing".into())
}

pub(super) async fn listen_expiry_before_credit_does_not_claim_a_message<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let security = Security::new()?;
    let node = Node::start_secure(
        provider,
        Some((security.tls.clone(), security.authentication.clone())),
    )
    .await?;
    node.seed("orders", "authorized-but-unclaimed").await?;
    let mut peer = security.connect(node.address).await?;
    open_cbs(&mut peer).await?;
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(4)
        .ok_or("SAS expiry overflow")?;
    put_token(&mut peer, token(expiry)?).await?;
    peer.begin(CHANNEL, 1, 0).await?;
    peer.attach_receiver("expires-without-credit", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    let before = node.snapshot()?;
    peer.grant(0, 0, false).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    node.inert(&before)?;
    // An actual wire refusal, not elapsed time or absence of Transfer, proves grant loss.
    peer.detached(Some("amqp:unauthorized-access")).await?;
    peer.barrier(CHANNEL).await?;
    peer.barrier(CBS).await?;
    node.fence().await?;
    node.inert(&before)?;
    node.ready("orders", 1, "authorized-but-unclaimed")?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}
