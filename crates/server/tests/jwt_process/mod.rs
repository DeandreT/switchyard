//! Server activation, not a manually constructed listener or an injected grant.
use super::*;
use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, OrderedMap, Outcome, Properties,
    SaslInit, Symbol, Value,
};
use base64::Engine as _;
use protocol_amqp::WebSocketIo;
use serde_json::json;
use std::{ffi::OsString, sync::Arc, time::SystemTime};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

mod startup;

#[allow(dead_code)]
#[path = "../sdk_child/mod.rs"]
mod sdk_child;

const HOST: &str = "tenant.servicebus.windows.net";
const ROOT: &str = admin_tls::ROOT;
const ISSUER: &str = "https://amqp-issuer.example/";
const AUDIENCE: &str = "urn:switchyard:offline-amqp";

fn now() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn amqp_claims(subject: &str) -> serde_json::Value {
    let mut claims = claims(subject);
    claims["iss"] = json!(ISSUER);
    claims["aud"] = json!(AUDIENCE);
    claims
}

fn token(subject: &str) -> String {
    sign(amqp_claims(subject))
}

fn amqp_policy() -> serde_json::Value {
    let mut policy = policy();
    policy["issuer"] = json!(ISSUER);
    policy["audience"] = json!(AUDIENCE);
    policy["bindings"] = json!([
        {"subject":"ns-send","scope":ROOT,"permissions":["send"]},
        {"subject":"ns-listen","scope":ROOT,"permissions":["listen"]},
        {"subject":"ns-manage","scope":ROOT,"permissions":["manage"]},
        {"subject":"entity-send","scope":format!("{ROOT}/orders"),"permissions":["send"]},
        {"subject":"entity-listen","scope":format!("{ROOT}/orders"),"permissions":["listen"]},
        {"subject":"entity-manage","scope":format!("{ROOT}/events"),"permissions":["manage"]}
    ]);
    policy
}

fn arguments(root: &Path, websocket: bool, sas: bool) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "--tls-certificate".into(),
        root.join("cert.pem").into(),
        "--tls-private-key".into(),
        root.join("key.pem").into(),
        "--jwt-policy-file".into(),
        root.join("amqp-policy.json").into(),
    ];
    if websocket {
        args.extend(["--transport".into(), "amqp-websockets".into()]);
    }
    if sas {
        args.extend([
            "--shared-access-key-name".into(),
            "sas".into(),
            "--shared-access-key-file".into(),
            root.join("sas-key").into(),
        ]);
    }
    args
}

async fn start(root: &Path, cert: &str, websocket: bool, sas: bool) -> Result<Process> {
    fs::write(root.join("amqp-policy.json"), amqp_policy().to_string())?;
    fs::write(root.join("sas-key"), "test-secret")?;
    let args = arguments(root, websocket, sas);
    let args: Vec<_> = args.iter().map(|s| s.to_str().unwrap()).collect();
    let (process, mut rpc) = Process::start(root, cert, &args, &[]).await?;
    // Admin authentication is independent and is used only for entity seeding.
    let admin = sign(claims("operator"));
    for (path, kind) in [
        ("orders", EntityKind::Queue),
        ("other", EntityKind::Queue),
        ("events", EntityKind::Topic),
    ] {
        rpc.create_entity(request(create(path, kind), &admin))
            .await?;
    }
    let mut sub = create("", EntityKind::Subscription);
    sub.subscription_topic = Some("events".into());
    sub.subscription_name = Some("consumer".into());
    rpc.create_entity(request(sub, &admin)).await?;
    Ok(process)
}

async fn connect(address: SocketAddr, cert: &str, websocket: bool) -> Result<Connection> {
    connect_named(address, cert, websocket, "localhost").await
}

async fn connect_named(
    address: SocketAddr,
    cert: &str,
    websocket: bool,
    hostname: &str,
) -> Result<Connection> {
    let der = base64::engine::general_purpose::STANDARD.decode(
        cert.lines()
            .filter(|line| !line.starts_with("---"))
            .collect::<String>(),
    )?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(der))?;
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let stream = tokio_rustls::TlsConnector::from(Arc::new(tls))
        .connect(
            rustls::pki_types::ServerName::try_from(hostname.to_owned())?,
            TcpStream::connect(address).await?,
        )
        .await?;
    let builder = Connection::builder()
        .container_id("configured-jwt-client")
        .sasl(SaslInit {
            mechanism: Symbol::from("ANONYMOUS"),
            initial_response: None,
            hostname: Some(HOST.into()),
        });
    if websocket {
        let mut request = format!("ws://{address}/$servicebus/websocket").into_client_request()?;
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "AMQPWSB10".parse()?);
        let (socket, _) = tokio_tungstenite::client_async(request, stream).await?;
        Ok(builder.open_with_stream(WebSocketIo::new(socket)).await?)
    } else {
        Ok(builder.open_with_stream(stream).await?)
    }
}

async fn put(session: &mut Session, kind: &str, resource: &str, token: String) -> Result<i32> {
    let mut requests = Sender::attach(session, "cbs-request", protocol_amqp::CBS_NODE).await?;
    let mut replies = Receiver::builder()
        .name("cbs-response")
        .source(protocol_amqp::CBS_NODE)
        .target("jwt-reply")
        .attach(session)
        .await?;
    requests
        .send(
            Message::builder()
                .properties(Properties {
                    message_id: Some("token-1".into()),
                    reply_to: Some("jwt-reply".into()),
                    ..Default::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("operation", "put-token")
                        .insert("type", kind)
                        .insert("name", resource)
                        .build(),
                )
                .body(Body::Value(Value::String(token)))
                .build(),
        )
        .await?;
    let reply = replies.recv().await?;
    assert_eq!(
        reply.message().properties.as_ref().unwrap().correlation_id,
        Some("token-1".into())
    );
    let Some(Value::Int(status)) = reply
        .message()
        .application_properties
        .as_ref()
        .unwrap()
        .get("status-code")
    else {
        panic!("missing CBS status")
    };
    let status = *status;
    assert_eq!(
        reply
            .message()
            .application_properties
            .as_ref()
            .unwrap()
            .get("status-description"),
        Some(&Value::String(
            match status {
                202 => "Accepted",
                401 => "Unauthorized",
                400 => "unsupported CBS operation or token type",
                _ => panic!("unexpected CBS status"),
            }
            .into()
        ))
    );
    replies.accept(&reply).await?;
    requests.close().await?;
    replies.close().await?;
    Ok(status)
}

fn message(text: &str) -> Message {
    Message::builder()
        .body(Body::Data(vec![text.as_bytes().to_vec().into()]))
        .build()
}

async fn send(session: &mut Session, entity: &str, allowed: bool) -> Result {
    let mut sender = Sender::attach(session, "data-sender", entity).await?;
    let sent = sender.send(message("configured-jwt")).await;
    if allowed {
        assert!(matches!(sent?, Outcome::Accepted(_)));
        sender.close().await?;
    } else {
        assert!(sent.is_err(), "unauthorized transfer was accepted");
    }
    Ok(())
}

async fn management_links(session: &mut Session, entity: &str) -> Result<(Sender, Receiver)> {
    let address = format!("{entity}/$management");
    let responses = Receiver::builder()
        .name("management-response")
        .source(address.clone())
        .target("management-reply")
        .attach(session)
        .await?;
    let requests = Sender::attach(session, "management-request", address).await?;
    Ok((requests, responses))
}

async fn management(
    requests: &mut Sender,
    replies: &mut Receiver,
    operation: &str,
    body: OrderedMap<Value, Value>,
) -> Result<i32> {
    assert!(matches!(
        requests
            .send(
                Message::builder()
                    .properties(Properties {
                        message_id: Some("management-1".into()),
                        reply_to: Some("management-reply".into()),
                        ..Default::default()
                    })
                    .application_properties(
                        ApplicationProperties::builder()
                            .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                            .build()
                    )
                    .body(Body::Value(Value::Map(body)))
                    .build()
            )
            .await?,
        Outcome::Accepted(_)
    ));
    let reply = replies.recv().await?;
    let Some(Value::Int(status)) = reply
        .message()
        .application_properties
        .as_ref()
        .unwrap()
        .get(protocol_amqp::STATUS_CODE_PROPERTY)
    else {
        panic!("missing management status")
    };
    let status = *status;
    replies.accept(&reply).await?;
    Ok(status)
}

fn peek_body() -> OrderedMap<Value, Value> {
    [
        (Value::String("from-sequence-number".into()), Value::Long(0)),
        (Value::String("message-count".into()), Value::Int(1)),
    ]
    .into_iter()
    .collect()
}

fn rules_body() -> OrderedMap<Value, Value> {
    [
        (Value::String("skip".into()), Value::Int(0)),
        (Value::String("top".into()), Value::Int(10)),
    ]
    .into_iter()
    .collect()
}

async fn finish(mut process: Process) -> Result {
    process.signal(Signal::TERM)?;
    assert_eq!(
        process.wait().await?,
        0,
        "original server must exit successfully"
    );
    assert!(TcpStream::connect(process.amqp).await.is_err());
    assert!(TcpStream::connect(process.address).await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_server_trusted_tcp_and_wss_enforce_namespace_and_entity_rights() -> Result {
    for websocket in [false, true] {
        let (root, cert) = fixture()?;
        let process = start(root.path(), &cert, websocket, false).await?;
        for scope in ["ns", "entity"] {
            println!(
                "JWT_PROCESS transport={} scope={scope}",
                if websocket { "wss" } else { "amqp" }
            );
            let mut connection = connect(process.amqp, &cert, websocket).await?;
            let mut session = Session::begin(&mut connection).await?;
            let orders = format!("{ROOT}/orders");
            assert_eq!(
                put(
                    &mut session,
                    "jwt",
                    &orders,
                    token(&format!("{scope}-send"))
                )
                .await?,
                202
            );
            // Send does not imply Listen or Manage, even on management links.
            let (mut requests, mut replies) = management_links(&mut session, "orders").await?;
            assert_eq!(
                management(
                    &mut requests,
                    &mut replies,
                    protocol_amqp::PEEK_MESSAGE_OPERATION,
                    peek_body()
                )
                .await?,
                401
            );
            assert_eq!(
                management(
                    &mut requests,
                    &mut replies,
                    protocol_amqp::ENUMERATE_RULES_OPERATION,
                    rules_body()
                )
                .await?,
                401
            );
            requests.close().await?;
            replies.close().await?;
            let mut denied = Receiver::attach(&mut session, "denied-listen", "orders").await?;
            assert!(denied.recv().await.is_err());
            send(&mut session, "orders", true).await?;
            if scope == "ns" {
                send(&mut session, "other", true).await?;
            } else {
                send(&mut session, "other", false).await?;
            }
            connection.close().await?;

            let mut connection = connect(process.amqp, &cert, websocket).await?;
            let mut session = Session::begin(&mut connection).await?;
            assert_eq!(
                put(
                    &mut session,
                    "jwt",
                    &orders,
                    token(&format!("{scope}-listen"))
                )
                .await?,
                202
            );
            send(&mut session, "orders", false).await?;
            let (mut requests, mut replies) = management_links(&mut session, "orders").await?;
            assert_eq!(
                management(
                    &mut requests,
                    &mut replies,
                    protocol_amqp::PEEK_MESSAGE_OPERATION,
                    peek_body()
                )
                .await?,
                200
            );
            let mut receiver =
                Receiver::attach(&mut session, "authorized-listen", "orders").await?;
            let delivery = receiver.recv().await?;
            assert_eq!(delivery.message().body, message("configured-jwt").body);
            receiver.accept(&delivery).await?;
            let mut sibling = Receiver::attach(&mut session, "sibling-listen", "other").await?;
            if scope == "ns" {
                let delivery = sibling.recv().await?;
                sibling.accept(&delivery).await?;
            } else {
                assert!(sibling.recv().await.is_err());
            }
            connection.close().await?;

            let mut connection = connect(process.amqp, &cert, websocket).await?;
            let mut session = Session::begin(&mut connection).await?;
            let events = format!("{ROOT}/events");
            assert_eq!(
                put(
                    &mut session,
                    "jwt",
                    &events,
                    token(&format!("{scope}-manage"))
                )
                .await?,
                202
            );
            let (mut requests, mut replies) =
                management_links(&mut session, "events/Subscriptions/consumer").await?;
            assert_eq!(
                management(
                    &mut requests,
                    &mut replies,
                    protocol_amqp::ENUMERATE_RULES_OPERATION,
                    rules_body()
                )
                .await?,
                200
            );
            // Manage implies Send and Listen, but an entity binding never covers a sibling.
            send(&mut session, "events", true).await?;
            let mut receiver = Receiver::attach(
                &mut session,
                "managed-listen",
                "events/Subscriptions/consumer",
            )
            .await?;
            let delivery = receiver.recv().await?;
            receiver.accept(&delivery).await?;
            if scope == "entity" {
                send(&mut session, "other", false).await?;
            }
            connection.close().await?;
        }
        finish(process).await?;
        // Only a joined successful original process permits opening its durable store.
        drop(storage::FjallStore::open(root.path().join("state"))?);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_server_rejects_invalid_tokens_scope_and_rights_without_dispatch() -> Result {
    for websocket in [false, true] {
        let (root, cert) = fixture()?;
        let process = start(root.path(), &cert, websocket, false).await?;
        let mut connection = connect(process.amqp, &cert, websocket).await?;
        let mut session = Session::begin(&mut connection).await?;
        let resource = format!("{ROOT}/orders");
        for (field, value) in [
            ("iss", json!("https://wrong.example/")),
            ("aud", json!(resource)),
            ("sub", json!("unknown")),
            ("exp", json!(now() - 1)),
            ("nbf", json!(now() + 300)),
            ("iat", json!(now() + 300)),
        ] {
            let mut invalid = amqp_claims("entity-send");
            invalid[field] = value;
            if field == "exp" {
                invalid["iat"] = json!(now() - 60);
            }
            assert_eq!(
                put(&mut session, "jwt", &resource, sign(invalid)).await?,
                401,
                "rejected {field}"
            );
        }
        for header in [
            json!({"alg":"HS256","kid":"key-1","typ":"switchyard+jwt"}),
            json!({"alg":"RS256","kid":"key-1","typ":"JWT"}),
        ] {
            assert_eq!(
                put(
                    &mut session,
                    "jwt",
                    &resource,
                    admin_tls::sign_with_header(header, amqp_claims("entity-send"))
                )
                .await?,
                401
            );
        }
        assert_eq!(
            put(&mut session, "JWT", &resource, token("entity-send")).await?,
            400
        );
        for resource in ["amqps://other.example/orders", &format!("{ROOT}/other")] {
            assert_eq!(
                put(&mut session, "jwt", resource, token("entity-send")).await?,
                401
            );
        }
        // An admin credential cannot activate the AMQP path.
        assert_eq!(
            put(&mut session, "jwt", &resource, sign(claims("operator"))).await?,
            401
        );
        send(&mut session, "orders", false).await?;
        connection.close().await?;
        // A client claim cannot widen the locally configured entity Send binding.
        let mut connection = connect(process.amqp, &cert, websocket).await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut inflated = amqp_claims("entity-send");
        inflated["roles"] = json!(["Manage"]);
        inflated["scope"] = json!(ROOT);
        assert_eq!(
            put(&mut session, "jwt", &resource, sign(inflated)).await?,
            202
        );
        send(&mut session, "other", false).await?;
        let (mut requests, mut replies) = management_links(&mut session, "orders").await?;
        assert_eq!(
            management(
                &mut requests,
                &mut replies,
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                peek_body()
            )
            .await?,
            401
        );
        requests.close().await?;
        replies.close().await?;
        send(
            &mut session,
            "events/Subscriptions/consumer/$management",
            false,
        )
        .await?;
        connection.close().await?;
        // Observe the real broker, not just a CBS status: rejected sends left no messages.
        let mut connection = connect(process.amqp, &cert, websocket).await?;
        let mut session = Session::begin(&mut connection).await?;
        assert_eq!(
            put(&mut session, "jwt", ROOT, token("ns-listen")).await?,
            202
        );
        for entity in ["orders", "other"] {
            let (mut requests, mut replies) = management_links(&mut session, entity).await?;
            assert_eq!(
                management(
                    &mut requests,
                    &mut replies,
                    protocol_amqp::PEEK_MESSAGE_OPERATION,
                    peek_body()
                )
                .await?,
                204
            );
            requests.close().await?;
            replies.close().await?;
        }
        connection.close().await?;
        finish(process).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_server_refresh_rechecks_retained_data_and_management_and_preserves_sas()
-> Result {
    for websocket in [false, true] {
        let (root, cert) = fixture()?;
        let process = start(root.path(), &cert, websocket, true).await?;
        let mut connection = connect(process.amqp, &cert, websocket).await?;
        let mut session = Session::begin(&mut connection).await?;
        assert_eq!(
            put(&mut session, "jwt", ROOT, token("ns-manage")).await?,
            202
        );
        let mut sender = Sender::attach(&mut session, "retained-sender", "orders").await?;
        let mut receiver = Receiver::attach(&mut session, "retained-receiver", "other").await?;
        let (mut requests, mut replies) = management_links(&mut session, "orders").await?;
        assert!(matches!(
            sender.send(message("before-refresh")).await?,
            Outcome::Accepted(_)
        ));
        assert_eq!(
            management(
                &mut requests,
                &mut replies,
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                peek_body()
            )
            .await?,
            200
        );
        assert_eq!(
            put(&mut session, "jwt", ROOT, "invalid-refresh".into()).await?,
            401
        );
        assert!(matches!(
            sender.send(message("after-invalid-refresh")).await?,
            Outcome::Accepted(_)
        ));
        let mut short = amqp_claims("ns-manage");
        short["exp"] = json!(now() + 5);
        assert_eq!(put(&mut session, "jwt", ROOT, sign(short)).await?, 202);
        tokio::time::timeout(WAIT, sender.on_detach()).await?;
        assert!(sender.send(message("after-expiry")).await.is_err());
        assert!(tokio::time::timeout(WAIT, receiver.recv()).await?.is_err());
        // Management was attached while authorized; it must recheck on each request.
        let request = Message::builder()
            .properties(Properties {
                message_id: Some("expired".into()),
                reply_to: Some("management-reply".into()),
                ..Default::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert(
                        protocol_amqp::OPERATION_PROPERTY,
                        protocol_amqp::PEEK_MESSAGE_OPERATION,
                    )
                    .build(),
            )
            .body(Body::Value(Value::Map(peek_body())))
            .build();
        let sent = requests.send(request).await;
        if sent.is_ok() {
            let reply = replies.recv().await?;
            assert_eq!(
                reply
                    .message()
                    .application_properties
                    .as_ref()
                    .unwrap()
                    .get(protocol_amqp::STATUS_CODE_PROPERTY),
                Some(&Value::Int(401))
            );
            replies.accept(&reply).await?;
        }
        connection.close().await?;
        // The configured SAS path is still independently usable on the same listener.
        let mut connection = connect(process.amqp, &cert, websocket).await?;
        let mut session = Session::begin(&mut connection).await?;
        assert_eq!(
            put(&mut session, "servicebus.windows.net:sastoken", ROOT, sas()).await?,
            202
        );
        send(&mut session, "other", true).await?;
        connection.close().await?;
        finish(process).await?;
    }
    Ok(())
}

fn sas() -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let resource: String = url::form_urlencoded::byte_serialize(ROOT.as_bytes()).collect();
    let expiry = now() + 600;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"test-secret").unwrap();
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let signature: String = url::form_urlencoded::byte_serialize(signature.as_bytes()).collect();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=sas")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux, dotnet 10 and a locked NuGet restore; selected prerequisites must succeed"]
async fn declared_current_offline_jwt_credential_wss_on_spawned_server() -> Result {
    let (root, cert) = fixture()?;
    let process = start(root.path(), &cert, true, false).await?;
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/dotnet-websockets");
    let mut sdk = sdk_child::SdkRun::prepare(
        &source,
        "Switchyard.Conformance.DotNetWebSockets.csproj",
        sdk_child::pins::SdkPin::DeclaredCurrent,
    )?;
    sdk.build().await?;
    let token_path = root.path().join("credential.jwt");
    fs::write(&token_path, token("ns-manage"))?;
    let cert_path = root.path().join("cert.pem");
    let result = sdk
        .run(
            &[
                "offline-jwt".into(),
                HOST.into(),
                format!("sb://localhost:{}", process.amqp.port()).into(),
                "orders".into(),
                token_path.into(),
            ],
            &[("SSL_CERT_FILE", cert_path.as_os_str())],
        )
        .await?;
    assert_eq!(result.records.len(), 2);
    assert_eq!(result.packages[0].package_version, "7.21.0");
    println!(
        "SDK_PIN_VERIFIED declared-current {}",
        serde_json::to_string(&result.packages)?
    );
    println!(
        "SDK_CUSTODY_VERIFIED {}",
        serde_json::to_string(&result.records)?
    );
    println!(
        "SDK_CHILD_STDOUT\n{}",
        String::from_utf8_lossy(&result.output.stdout)
    );
    println!(
        "SDK_CHILD_STDERR\n{}",
        String::from_utf8_lossy(&result.output.stderr)
    );
    finish(process).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_server_requires_real_tls_and_normal_certificate_validation() -> Result {
    for websocket in [false, true] {
        let (root, cert) = fixture()?;
        let process = start(root.path(), &cert, websocket, false).await?;
        let untrusted = generate_simple_self_signed(vec!["localhost".into()])?
            .cert
            .pem();
        assert!(connect(process.amqp, &untrusted, websocket).await.is_err());
        assert!(
            connect_named(process.amqp, &cert, websocket, "wrong.example")
                .await
                .is_err()
        );
        let plaintext = Connection::builder()
            .container_id("plaintext-jwt")
            .open_with_stream(TcpStream::connect(process.amqp).await?)
            .await;
        assert!(plaintext.is_err());
        let mut connection = connect(process.amqp, &cert, websocket).await?;
        let mut session = Session::begin(&mut connection).await?;
        assert_eq!(put(&mut session, "jwt", ROOT, token("ns-send")).await?, 202);
        send(&mut session, "orders", true).await?;
        connection.close().await?;
        finish(process).await?;
    }
    Ok(())
}
