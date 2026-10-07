use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use admin_api::v1::{
    entity_service_client::EntityServiceClient, entity_service_server::EntityServiceServer,
    finite_queue_service_client::FiniteQueueServiceClient, rule_service_server::RuleServiceServer,
};
use auth::{
    JwtPolicy, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use hmac::{Hmac, Mac};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::{
    SignatureScheme,
    crypto::ring,
    pki_types::{PrivateKeyDer, PrivatePkcs1KeyDer},
};
use server::{NativeAdminError, NativeAdminListener};
use sha2::Sha256;
use tokio::{
    net::TcpListener,
    task::{JoinError, JoinHandle},
    time::timeout,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};
use url::form_urlencoded::byte_serialize;

use super::{fixture::*, *};

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-finite-transport-public-fixture";
const ISSUER: &str = "https://issuer.example/";
const AUDIENCE: &str = "urn:switchyard:native-admin";

const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
// Public RSA PKCS1 fixture shared with the pure auth tests, never live credentials.
const PRIVATE_DER: &str = concat!(
    "MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL",
    "UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V",
    "rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H",
    "oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI",
    "Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/",
    "by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd",
    "WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ",
    "Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j",
    "E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn",
    "LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS",
    "bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE",
    "8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl",
    "xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY",
    "2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1",
    "9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0",
    "7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ",
    "mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7",
    "MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4",
    "t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam",
    "QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA",
    "2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ",
    "4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn",
    "PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH",
    "UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8",
    "oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==",
);

fn sas_policy() -> TestResult<SharedAccessPolicy> {
    Ok(SharedAccessPolicy::new([
        SharedAccessRule::new(
            "manage",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?,
        SharedAccessRule::new(
            "send",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::SEND,
        )?,
        SharedAccessRule::new(
            "listen",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::LISTEN,
        )?,
    ])?)
}

fn jwt_policy() -> TestResult<JwtPolicy> {
    Ok(JwtPolicy::from_json(&format!(
        r#"{{"version":1,"issuer":"{ISSUER}","audience":"{AUDIENCE}","keys":[{{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"{MODULUS}","e":"AQAB"}}],"bindings":[{{"subject":"administrator","scope":"amqps://{HOST}","permissions":["manage"]}},{{"subject":"orders-manager","scope":"amqps://{HOST}/orders","permissions":["manage"]}},{{"subject":"sender","scope":"amqps://{HOST}","permissions":["send"]}}]}}"#
    ))?)
}

fn epoch() -> TestResult<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn sas_token(rule: &str, resource: &str, expiry: u64) -> TestResult<String> {
    let resource = byte_serialize(resource.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes())?;
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect::<String>();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}"
    ))
}

fn credential<T>(body: T, token: &str) -> TestResult<Request<T>> {
    let mut request = Request::new(body);
    let mut metadata = token.parse::<tonic::metadata::MetadataValue<_>>()?;
    metadata.set_sensitive(true);
    request.metadata_mut().insert("authorization", metadata);
    Ok(request)
}

fn manage<T>(body: T) -> TestResult<Request<T>> {
    credential(
        body,
        &sas_token("manage", &format!("amqps://{HOST}"), epoch()? + 120)?,
    )
}

fn jwt(subject: &str, audience: &str, expiry: u64) -> TestResult<String> {
    let issued = epoch()?;
    let input = format!("{}.{}",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","kid":"key-1","typ":"at+jwt"}"#),
        URL_SAFE_NO_PAD.encode(format!(r#"{{"iss":"{ISSUER}","sub":"{subject}","aud":"{audience}","iat":{issued},"exp":{expiry}}}"#)));
    let key = ring::default_provider()
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(
            STANDARD.decode(PRIVATE_DER)?,
        )))?;
    let signer = key
        .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
        .ok_or("public RSA fixture signing unavailable")?;
    Ok(format!(
        "Bearer {input}.{}",
        URL_SAFE_NO_PAD.encode(signer.sign(input.as_bytes())?)
    ))
}

struct Certificates {
    ca: String,
    chain: String,
    key: String,
}

fn certificates(authority_name: &str) -> TestResult<Certificates> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, authority_name);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate()?;
    let ca = ca_params.self_signed(&ca_key)?;
    let mut leaf_params = CertificateParams::new(vec!["localhost".into()])?;
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    leaf_params.use_authority_key_identifier_extension = true;
    leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_key = KeyPair::generate()?;
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key)?;
    Ok(Certificates {
        ca: ca.pem(),
        chain: format!("{}{}", leaf.pem(), ca.pem()),
        key: leaf_key.serialize_pem(),
    })
}

struct TransportNode<P: StoreProvider> {
    core: Option<Node<P>>,
    listener: Option<JoinHandle<Result<(), NativeAdminError>>>,
    channel: Option<Channel>,
    endpoint: String,
    ca: String,
}

impl<P: StoreProvider> TransportNode<P> {
    async fn start(provider: P, jwt_enabled: bool, old_registration: bool) -> TestResult<Self> {
        let mut core = Node::start(provider)?;
        core.service = core
            .service
            .clone()
            .with_shared_access_policy(sas_policy()?, HOST)?;
        if jwt_enabled {
            core.service = core
                .service
                .clone()
                .with_offline_jwt_policy(jwt_policy()?, HOST)?;
        }
        let certificates = certificates("Switchyard native finite trusted CA")?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://{}", socket.local_addr()?);
        let service = core.service.clone();
        let listener = if old_registration {
            let mut server = Server::builder().tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(
                        certificates.chain.as_bytes(),
                        certificates.key.as_bytes(),
                    ))
                    .timeout(DEADLINE),
            )?;
            tokio::spawn(async move {
                server
                    .add_service(EntityServiceServer::new(service.clone()))
                    .add_service(RuleServiceServer::new(service))
                    .serve_with_incoming(TcpListenerStream::new(socket))
                    .await?;
                Ok(())
            })
        } else {
            let listener = NativeAdminListener::new(service)
                .with_tls(certificates.chain.as_bytes(), certificates.key.as_bytes())?
                .with_handshake_timeout(DEADLINE);
            tokio::spawn(listener.serve(socket))
        };
        Ok(Self {
            core: Some(core),
            listener: Some(listener),
            channel: None,
            endpoint,
            ca: certificates.ca,
        })
    }

    fn core(&self) -> &Node<P> {
        self.core.as_ref().expect("original counted owner")
    }

    async fn connect_with(&self, ca: &str, name: &str) -> TestResult<Channel> {
        Ok(timeout(
            DEADLINE,
            Endpoint::from_shared(self.endpoint.clone())?
                .connect_timeout(DEADLINE)
                .timeout(DEADLINE)
                .tls_config(
                    ClientTlsConfig::new()
                        .ca_certificate(Certificate::from_pem(ca))
                        .domain_name(name),
                )?
                .connect(),
        )
        .await??)
    }

    async fn connect(&mut self) -> TestResult<Channel> {
        let channel = self.connect_with(&self.ca, "localhost").await?;
        self.channel = Some(channel.clone());
        Ok(channel)
    }

    async fn finish(mut self, observed: Observed) -> TestResult {
        drop(self.channel.take());
        let cleanup = if let Some(mut original) = self.listener.take() {
            let id = original.id();
            original.abort();
            let first = timeout(DEADLINE, &mut original).await;
            let expired = first.is_err();
            let joined = match first {
                Ok(joined) => Some(joined),
                Err(_) => timeout(DEADLINE, &mut original).await.ok(),
            };
            match joined {
                Some(Err(error)) if !expired && error.is_cancelled() && error.id() == id => Ok(()),
                Some(Ok(Ok(()))) if !expired => Ok(()),
                joined => Err(Box::new(ListenerFailure {
                    joined,
                    original: if expired { Some(original) } else { None },
                }) as Box<dyn Error>),
            }
        } else {
            Err("original native listener root is missing".into())
        };
        self.core
            .take()
            .expect("original counted owner")
            .finish_with_cleanup(observed, cleanup)
    }
}

impl<P: StoreProvider> Drop for TransportNode<P> {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
    }
}

struct ListenerFailure {
    joined: Option<Result<Result<(), NativeAdminError>, JoinError>>,
    original: Option<JoinHandle<Result<(), NativeAdminError>>>,
}

impl fmt::Debug for ListenerFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("ListenerFailure")
            .field("joined", &self.joined.is_some())
            .field("retained_original", &self.original.is_some())
            .finish()
    }
}

impl fmt::Display for ListenerFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("original native listener join failed or exceeded its deadline")
    }
}

impl Error for ListenerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.joined.as_ref()? {
            Err(error) => Some(error),
            Ok(Err(error)) => Some(error),
            Ok(Ok(())) => None,
        }
    }
}

impl Drop for ListenerFailure {
    fn drop(&mut self) {
        if let Some(original) = &self.original {
            original.abort();
        }
    }
}

#[derive(Clone, Copy)]
enum CertificateRefusal {
    UnknownRoot,
    WrongName,
}

fn certificate_refusal(error: &(dyn Error + 'static), expected: CertificateRefusal) -> bool {
    if let Some(rustls::Error::InvalidCertificate(cause)) = error.downcast_ref::<rustls::Error>() {
        return match expected {
            CertificateRefusal::UnknownRoot => {
                matches!(cause, rustls::CertificateError::UnknownIssuer)
            }
            CertificateRefusal::WrongName => matches!(
                cause,
                rustls::CertificateError::NotValidForName
                    | rustls::CertificateError::NotValidForNameContext { .. }
            ),
        };
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>()
        && error
            .get_ref()
            .is_some_and(|source| certificate_refusal(source, expected))
    {
        return true;
    }
    error
        .source()
        .is_some_and(|source| certificate_refusal(source, expected))
}

pub(super) async fn private_ca_name_checked_finite_rpc_roundtrip_and_tls_refusals<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = TransportNode::start(provider, false, false).await?;
    let observed = observe(async {
        let channel = node.connect().await?;
        let mut client = FiniteQueueServiceClient::new(channel);
        let created = timeout(
            DEADLINE,
            client.create_finite_queue(manage(create("orders"))?),
        )
        .await??
        .into_inner();
        assert_eq!(
            created,
            timeout(DEADLINE, client.get_finite_queue(manage(get("orders"))?))
                .await??
                .into_inner()
        );
        let before = node.core().checkpoint()?;
        let wrong_ca = certificates("Switchyard native finite untrusted CA")?;
        let error = node
            .connect_with(&wrong_ca.ca, "localhost")
            .await
            .expect_err("wrong CA must fail");
        assert!(
            certificate_refusal(error.as_ref(), CertificateRefusal::UnknownRoot),
            "actual unknown-root certificate cause required: {error:?}"
        );
        node.core().untouched(&before)?;
        let error = node
            .connect_with(&node.ca, "wrong.example")
            .await
            .expect_err("wrong name must fail");
        assert!(
            certificate_refusal(error.as_ref(), CertificateRefusal::WrongName),
            "actual name certificate cause required: {error:?}"
        );
        node.core().untouched(&before)?;
        let mut input = definition(&created);
        input.config.as_mut().unwrap().default_time_to_live =
            Some(DefaultTimeToLive::DefaultTtlMillis(60_000));
        input.reservation_limit_bytes = Some(4096);
        let updated = timeout(DEADLINE, client.set_finite_queue_definition(manage(input)?))
            .await??
            .into_inner();
        assert_eq!(updated.generation, created.generation);
        assert_eq!(updated.reservation_limit_bytes, 4096);
        assert_eq!(
            updated.config.unwrap().default_time_to_live,
            Some(DefaultTimeToLive::DefaultTtlMillis(60_000))
        );
        assert_eq!(
            timeout(DEADLINE, client.get_finite_queue(manage(get("orders"))?))
                .await??
                .into_inner(),
            updated
        );
        Ok(())
    })
    .await;
    node.finish(observed).await
}

pub(super) async fn actual_tls_sas_scope_denials_preserve_owner_and_health<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = TransportNode::start(provider, false, false).await?;
    let observed = observe(async {
        let channel = node.connect().await?;
        let mut client = FiniteQueueServiceClient::new(channel);
        let created = timeout(
            DEADLINE,
            client.create_finite_queue(manage(create("orders"))?),
        )
        .await??
        .into_inner();
        let expiry = epoch()? + 120;
        for (rule, scope, expires, expected) in [
            (
                "send",
                format!("amqps://{HOST}"),
                expiry,
                Code::PermissionDenied,
            ),
            (
                "listen",
                format!("amqps://{HOST}"),
                expiry,
                Code::PermissionDenied,
            ),
            (
                "manage",
                format!("amqps://{HOST}/other"),
                expiry,
                Code::PermissionDenied,
            ),
            (
                "manage",
                format!("amqps://{HOST}"),
                1,
                Code::Unauthenticated,
            ),
        ] {
            let token = sas_token(rule, &scope, expires)?;
            let before = node.core().checkpoint()?;
            let mut input = definition(&created);
            input.config = None;
            code(
                timeout(
                    DEADLINE,
                    client.set_finite_queue_definition(credential(input, &token)?),
                )
                .await?,
                expected,
            );
            let mut input = create("refused");
            input.config = None;
            code(
                timeout(
                    DEADLINE,
                    client.create_finite_queue(credential(input, &token)?),
                )
                .await?,
                expected,
            );
            code(
                timeout(
                    DEADLINE,
                    client.get_finite_queue(credential(get("orders"), &token)?),
                )
                .await?,
                expected,
            );
            node.core().untouched(&before)?;
        }
        let before = node.core().checkpoint()?;
        code(
            timeout(DEADLINE, client.get_finite_queue(get("orders"))).await?,
            Code::Unauthenticated,
        );
        let mut foreign = create("foreign");
        foreign.namespace = "other".into();
        code(
            timeout(DEADLINE, client.create_finite_queue(manage(foreign)?)).await?,
            Code::PermissionDenied,
        );
        node.core().untouched(&before)?;
        let scoped = sas_token("manage", &format!("amqps://{HOST}/orders"), expiry)?;
        assert_eq!(
            timeout(
                DEADLINE,
                client.get_finite_queue(credential(get("orders"), &scoped)?)
            )
            .await??
            .into_inner(),
            created
        );
        timeout(
            DEADLINE,
            client.create_finite_queue(manage(create("healthy-after-denials"))?),
        )
        .await??;
        Ok(())
    })
    .await;
    node.finish(observed).await
}

pub(super) async fn actual_tls_offline_jwt_manage_and_denials_keep_sas_independent<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = TransportNode::start(provider, true, false).await?;
    let observed = observe(async {
        let channel = node.connect().await?;
        let mut client = FiniteQueueServiceClient::new(channel);
        let expiry = epoch()? + 120;
        let administrator = jwt("administrator", AUDIENCE, expiry)?;
        let created = timeout(
            DEADLINE,
            client.create_finite_queue(credential(create("orders"), &administrator)?),
        )
        .await??
        .into_inner();
        assert_eq!(created.path, "orders");
        let orders = jwt("orders-manager", AUDIENCE, expiry)?;
        assert_eq!(
            timeout(
                DEADLINE,
                client.get_finite_queue(credential(get("orders"), &orders)?)
            )
            .await??
            .into_inner(),
            created
        );
        for (subject, audience, expires, expected) in [
            ("sender", AUDIENCE, expiry, Code::PermissionDenied),
            ("unknown-subject", AUDIENCE, expiry, Code::Unauthenticated),
            (
                "administrator",
                "wrong-audience",
                expiry,
                Code::Unauthenticated,
            ),
            ("administrator", AUDIENCE, 1, Code::Unauthenticated),
        ] {
            let token = jwt(subject, audience, expires)?;
            let before = node.core().checkpoint()?;
            let mut input = definition(&created);
            input.config = None;
            code(
                timeout(
                    DEADLINE,
                    client.set_finite_queue_definition(credential(input, &token)?),
                )
                .await?,
                expected,
            );
            let mut input = create("jwt-refused");
            input.config = None;
            code(
                timeout(
                    DEADLINE,
                    client.create_finite_queue(credential(input, &token)?),
                )
                .await?,
                expected,
            );
            code(
                timeout(
                    DEADLINE,
                    client.get_finite_queue(credential(get("orders"), &token)?),
                )
                .await?,
                expected,
            );
            node.core().untouched(&before)?;
        }
        let before = node.core().checkpoint()?;
        code(
            timeout(
                DEADLINE,
                client.get_finite_queue(credential(get("other"), &orders)?),
            )
            .await?,
            Code::PermissionDenied,
        );
        node.core().untouched(&before)?;
        let mut input = definition(&created);
        input.reservation_limit_bytes = Some(4096);
        let updated = timeout(
            DEADLINE,
            client.set_finite_queue_definition(credential(input, &administrator)?),
        )
        .await??
        .into_inner();
        assert_eq!(updated.reservation_limit_bytes, 4096);
        let sas = timeout(
            DEADLINE,
            client.create_finite_queue(manage(create("sas-independent"))?),
        )
        .await??
        .into_inner();
        assert_eq!(sas.path, "sas-independent");
        assert_eq!(
            timeout(DEADLINE, client.get_finite_queue(manage(get("orders"))?))
                .await??
                .into_inner(),
            updated
        );
        Ok(())
    })
    .await;
    node.finish(observed).await
}

pub(super) async fn old_service_registration_has_no_finite_method_fallback<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = TransportNode::start(provider, false, true).await?;
    let observed = observe(async {
        let channel = node.connect().await?;
        let mut legacy = EntityServiceClient::new(channel.clone());
        let mut finite = FiniteQueueServiceClient::new(channel);
        let created = timeout(
            DEADLINE,
            legacy.create_entity(manage(legacy_create("legacy"))?),
        )
        .await??
        .into_inner();
        assert_eq!(created.path, "legacy");
        let before = node.core().checkpoint()?;
        code(
            timeout(
                DEADLINE,
                finite.create_finite_queue(manage(create("must-not-fallback"))?),
            )
            .await?,
            Code::Unimplemented,
        );
        code(
            timeout(DEADLINE, finite.get_finite_queue(manage(get("legacy"))?)).await?,
            Code::Unimplemented,
        );
        let input = SetFiniteQueueDefinitionRequest {
            namespace: "tenant".into(),
            path: "legacy".into(),
            expected_generation: Some(1),
            config: Some(full_config()),
            reservation_limit_bytes: Some(4096),
        };
        code(
            timeout(DEADLINE, finite.set_finite_queue_definition(manage(input)?)).await?,
            Code::Unimplemented,
        );
        node.core().untouched(&before)?;
        assert_eq!(
            timeout(
                DEADLINE,
                legacy.get_entity(manage(GetEntityRequest {
                    namespace: "tenant".into(),
                    path: "legacy".into()
                })?)
            )
            .await??
            .into_inner(),
            created
        );
        Ok(())
    })
    .await;
    node.finish(observed).await
}
