//! The configured binary shares its already-loaded offline policy with native TLS administration.
#![cfg(target_os = "linux")]

#[path = "native_admin_offline_jwt_cli/process.rs"]
mod process;

use std::{
    error::Error,
    fs,
    net::TcpListener,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use admin_api::v1::{
    CreateEntityRequest, Entity, EntityKind, GetEntityRequest, ListEntitiesRequest,
    QueueConfiguration, UpdateEntityRequest, entity_service_client::EntityServiceClient,
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use domain::{Command, CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    SignatureScheme,
    crypto::ring,
    pki_types::{PrivateKeyDer, PrivatePkcs1KeyDer},
};
use sha2::Sha256;
use storage::{FjallStore, MemoryStore, StateStore, StoreSnapshot};
use tempfile::TempDir;
use tokio::{
    process::Command as ProcessCommand,
    time::{sleep, timeout},
};
use tonic::{
    Code, Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const ISSUER: &str = "https://issuer.example/";
const AUDIENCE: &str = "urn:switchyard:native-admin";
const SAS_KEY: &str = "native-cli-fixture-key";
const REQUEST_DEADLINE: Duration = Duration::from_secs(3);
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

fn policy() -> String {
    format!(
        r#"{{"version":1,"issuer":"{ISSUER}","audience":"{AUDIENCE}","keys":[{{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"{MODULUS}","e":"AQAB"}}],"bindings":[{{"subject":"administrator","scope":"amqps://{HOST}","permissions":["manage"]}},{{"subject":"sender","scope":"amqps://{HOST}","permissions":["send"]}}]}}"#
    )
}

fn jwt(subject: &str) -> TestResult<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let expires = now.checked_add(120).ok_or("fixture epoch overflow")?;
    let input = format!("{}.{}",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","kid":"key-1","typ":"at+jwt"}"#),
        URL_SAFE_NO_PAD.encode(format!(r#"{{"iss":"{ISSUER}","sub":"{subject}","aud":"{AUDIENCE}","iat":{now},"exp":{expires}}}"#)));
    let provider = ring::default_provider();
    let key = provider
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(
            STANDARD.decode(PRIVATE_DER)?,
        )))?;
    let signer = key
        .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
        .ok_or("public fixture RS256 signing unavailable")?;
    Ok(format!(
        "{input}.{}",
        URL_SAFE_NO_PAD.encode(signer.sign(input.as_bytes())?)
    ))
}

fn authorized<T>(body: T, token: &str) -> TestResult<Request<T>> {
    authorization(body, &format!("Bearer {token}"))
}

fn authorization<T>(body: T, credential: &str) -> TestResult<Request<T>> {
    let mut request = Request::new(body);
    let mut value = process::at(
        "metadata",
        credential.parse::<tonic::metadata::MetadataValue<_>>(),
    )?;
    value.set_sensitive(true);
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

fn sas_token() -> TestResult<String> {
    let resource = byte_serialize(format!("amqps://{HOST}").as_bytes()).collect::<String>();
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(120)
        .ok_or("fixture epoch overflow")?;
    let mut mac = Hmac::<Sha256>::new_from_slice(SAS_KEY.as_bytes())?;
    mac.update(format!("{resource}\n{expires}").as_bytes());
    let signature =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect::<String>();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expires}&skn=fixture-rule"
    ))
}

fn create(path: &str) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: EntityKind::Queue as i32,
        ..Default::default()
    }
}

fn get() -> GetEntityRequest {
    GetEntityRequest {
        namespace: "tenant".into(),
        path: "orders".into(),
    }
}

fn list() -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        page_size: 10,
        ..Default::default()
    }
}

struct Files {
    _directory: TempDir,
    certificate: PathBuf,
    private_key: PathBuf,
    shared_key: PathBuf,
    policy_file: PathBuf,
    store: PathBuf,
    certificate_pem: String,
    private_key_pem: String,
    policy: String,
}

impl Files {
    fn new() -> TestResult<Self> {
        let directory = TempDir::new()?;
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let certificate_pem = cert.pem();
        let private_key_pem = key_pair.serialize_pem();
        let policy = policy();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let shared_key = directory.path().join("shared-key");
        let policy_file = directory.path().join("policy.json");
        let store = directory.path().join("durable-store");
        for (path, bytes) in [
            (&certificate, certificate_pem.as_bytes()),
            (&private_key, private_key_pem.as_bytes()),
            (&shared_key, SAS_KEY.as_bytes()),
            (&policy_file, policy.as_bytes()),
        ] {
            fs::write(path, bytes)?;
        }
        Ok(Self {
            _directory: directory,
            certificate,
            private_key,
            shared_key,
            policy_file,
            store,
            certificate_pem,
            private_key_pem,
            policy,
        })
    }

    fn command(&self) -> TestResult<(ProcessCommand, String)> {
        let amqp = process::at("reserve-amqp-address", TcpListener::bind("127.0.0.1:0"))?;
        let admin = process::at("reserve-admin-address", TcpListener::bind("127.0.0.1:0"))?;
        let amqp_address = process::at("amqp-address", amqp.local_addr())?.to_string();
        let admin_address = process::at("admin-address", admin.local_addr())?.to_string();
        let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_switchyard"));
        command
            .env("TOKIO_WORKER_THREADS", "2")
            .env("RUST_LOG", "warn")
            .arg("--namespace")
            .arg("tenant")
            .arg("--listen")
            .arg(amqp_address)
            .arg("--admin-listen")
            .arg(&admin_address)
            .arg("--storage")
            .arg("fjall")
            .arg("--data-dir")
            .arg(&self.store)
            .arg("--sweep-interval-millis")
            .arg("600000")
            .arg("--tls-certificate")
            .arg(&self.certificate)
            .arg("--tls-private-key")
            .arg(&self.private_key)
            .arg("--shared-access-key-name")
            .arg("fixture-rule")
            .arg("--shared-access-key-file")
            .arg(&self.shared_key)
            .arg("--offline-jwt-policy-file")
            .arg(&self.policy_file);
        drop((amqp, admin));
        Ok((command, format!("https://{admin_address}")))
    }

    async fn connect(&self, endpoint: String) -> TestResult<Channel> {
        let endpoint = process::at("endpoint", Endpoint::from_shared(endpoint))?
            .connect_timeout(Duration::from_millis(250))
            .timeout(REQUEST_DEADLINE);
        let endpoint = process::at(
            "tls-client",
            endpoint.tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(&self.certificate_pem))
                    .domain_name("localhost"),
            ),
        )?;
        process::at(
            "connect-deadline",
            timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(channel) = endpoint.connect().await {
                        return channel;
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await,
        )
    }
}

async fn assert_remote_state(
    client: &mut EntityServiceClient<Channel>,
    administrator: &str,
    expected: &Entity,
) -> TestResult {
    let fetched = process::at(
        "manage-get",
        client.get_entity(authorized(get(), administrator)?).await,
    )?
    .into_inner();
    assert_eq!(&fetched, expected);
    let listed = process::at(
        "manage-list",
        client
            .list_entities(authorized(list(), administrator)?)
            .await,
    )?
    .into_inner();
    assert_eq!(listed.entities, vec![expected.clone()]);
    assert!(listed.next_page_token.is_empty());
    Ok(())
}

fn canonical_baseline(files: &Files) -> TestResult<StoreSnapshot> {
    let store = process::at("reopen-baseline", FjallStore::open(&files.store))?;
    let durable = StateMachine::new(store.clone());
    let namespace = process::at("namespace", NamespaceName::new("tenant"))?;
    let orders = process::at("entity", EntityPath::new("orders"))?;
    let timestamp = process::at("persisted-time", durable.last_applied_time())?;
    let expected = StateMachine::new(MemoryStore::default());
    process::at(
        "canonical-create",
        expected.apply(&Command::new(
            namespace.clone(),
            orders.clone(),
            timestamp,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )),
    )?;
    assert_eq!(
        process::at(
            "persisted-configuration",
            durable.queue_config(&namespace, &orders)
        )?,
        Some(QueueConfig::default())
    );
    let actual = process::at("baseline-snapshot", store.snapshot())?;
    assert_eq!(
        actual,
        process::at("canonical-snapshot", expected.store().snapshot())?
    );
    Ok(actual)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_native_tls_jwt_manage_and_send_denials_reuse_the_loaded_policy() -> TestResult {
    let files = Files::new().map_err(|error| process::retained("files", error))?;
    let administrator =
        jwt("administrator").map_err(|error| process::retained("administrator-token", error))?;
    let sender = jwt("sender").map_err(|error| process::retained("sender-token", error))?;
    let sas = sas_token().map_err(|error| process::retained("sas-token", error))?;
    let sensitive = [
        administrator.as_str(),
        sender.as_str(),
        sas.as_str(),
        SAS_KEY,
        PRIVATE_DER,
        files.policy.as_str(),
        files.private_key_pem.as_str(),
    ];
    let (command, endpoint) = files.command()?;
    let mut created = None;
    process::run(command, &sensitive, async {
        let channel = files.connect(endpoint).await?;
        let mut client = EntityServiceClient::new(channel);
        let entity = process::at(
            "manage-create",
            client
                .create_entity(authorized(create("orders"), &administrator)?)
                .await,
        )?
        .into_inner();
        assert_eq!(entity.path, "orders");
        assert_eq!(entity.kind, EntityKind::Queue as i32);
        assert_remote_state(&mut client, &administrator, &entity).await?;
        created = Some(entity);
        Ok(())
    })
    .await?;
    let baseline = canonical_baseline(&files)?;
    let created = created.expect("the observed positive RPC returned its original entity");

    let (command, endpoint) = files.command()?;
    process::run(command, &sensitive, async {
        let channel = files.connect(endpoint).await?;
        process::at(
            "remove-loaded-policy-file",
            fs::remove_file(&files.policy_file),
        )?;
        let mut client = EntityServiceClient::new(channel);
        let refusal = client
            .create_entity(authorized(create("unauthorized"), &sender)?)
            .await;
        let refused = refusal.expect_err("Send-only cannot create native entities");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert_eq!(refused.message(), "entity management permission required");
        let refusal = client
            .update_entity(authorized(
                UpdateEntityRequest {
                    namespace: "tenant".into(),
                    path: "orders".into(),
                    queue_config: Some(QueueConfiguration {
                        lock_duration_millis: Some(40_000),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                &sender,
            )?)
            .await;
        let refused = refusal.expect_err("Send-only cannot update native entities");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert_eq!(refused.message(), "entity management permission required");
        assert_remote_state(&mut client, &administrator, &created).await?;
        let via_sas = process::at(
            "sas-get",
            client.get_entity(authorization(get(), &sas)?).await,
        )?
        .into_inner();
        assert_eq!(via_sas, created);
        assert!(!files.policy_file.exists());
        Ok(())
    })
    .await?;
    let store = process::at("reopen-after-denials", FjallStore::open(&files.store))?;
    assert_eq!(
        process::at("snapshot-after-denials", store.snapshot())?,
        baseline
    );
    Ok(())
}
