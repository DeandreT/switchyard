use std::{io, net::TcpListener};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use tempfile::TempDir;

use super::*;

// Public RSA geometry fixture from the auth JWT tests, not an operational key.
const POLICY: &str = r#"{"version":1,"issuer":"https://issuer.example/","audience":"urn:switchyard:tenant","keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ","e":"AQAB"}],"bindings":[{"subject":"producer","scope":"amqps://tenant.servicebus.windows.net/orders","permissions":["send"]}]}"#;

struct Credentials {
    _directory: TempDir,
    certificate: PathBuf,
    private_key: PathBuf,
    shared_key: PathBuf,
    jwt_policy: PathBuf,
}

impl Credentials {
    fn new() -> Self {
        let directory = TempDir::new().expect("credential fixture directory");
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec![String::from("localhost")])
                .expect("disposable credential identity");
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let shared_key = directory.path().join("shared-key");
        let jwt_policy = directory.path().join("offline-policy.json");
        fs::write(&certificate, cert.pem()).expect("fixture certificate");
        fs::write(&private_key, key_pair.serialize_pem()).expect("fixture private key");
        fs::write(&shared_key, "preflight-fixture-key\n").expect("fixture shared key");
        fs::write(&jwt_policy, POLICY).expect("fixture offline policy");
        Self {
            _directory: directory,
            certificate,
            private_key,
            shared_key,
            jwt_policy,
        }
    }

    fn authentication(&self) -> SharedAccessAuthentication {
        load_shared_access_authentication(
            DeploymentMode::Development,
            true,
            "tenant",
            Some("fixture-rule"),
            Some(&self.shared_key),
        )
        .expect("fixture SAS policy")
        .expect("configured SAS policy")
    }

    fn arguments(&self, check_config: bool) -> Arguments {
        let mut arguments = Arguments::try_parse_from(["switchyard"]).expect("default arguments");
        arguments.check_config = check_config;
        arguments.namespace = String::from("tenant");
        arguments.tls_certificate = Some(self.certificate.clone());
        arguments.tls_private_key = Some(self.private_key.clone());
        arguments.shared_access_key_name = Some(String::from("fixture-rule"));
        arguments.shared_access_key_file = Some(self.shared_key.clone());
        arguments.offline_jwt_policy_file = Some(self.jwt_policy.clone());
        arguments
    }
}

#[test]
fn offline_jwt_flag_is_opt_in_without_changing_defaults() {
    let arguments = Arguments::try_parse_from(["switchyard"]).expect("ordinary defaults");
    assert!(arguments.offline_jwt_policy_file.is_none());
    let prepared = prepare_configuration(&arguments).expect("ordinary preflight");
    assert!(prepared.shared_access_authentication.is_none());
    assert!(prepared.tls.is_none());
    assert_eq!(prepared.listen.port(), 5672);
    let enabled = Arguments::try_parse_from([
        "switchyard",
        "--offline-jwt-policy-file",
        "policy.json",
        "--check-config",
    ])
    .expect("optional policy-file arguments");
    assert_eq!(
        enabled.offline_jwt_policy_file,
        Some(PathBuf::from("policy.json"))
    );
    assert!(enabled.check_config);
    assert!(
        load_offline_jwt_policy(None, false, None)
            .expect("disabled policy")
            .is_none()
    );
}

#[test]
fn offline_policy_requires_tls_and_sas_before_file_io() {
    let root = TempDir::new().expect("fixture root");
    let missing = root.path().join("must-not-be-opened");
    assert_eq!(
        load_offline_jwt_policy(Some(&missing), false, None).unwrap_err(),
        StartupError::OfflineJwtRequiresTls
    );
    assert_eq!(
        load_offline_jwt_policy(Some(&missing), true, None).unwrap_err(),
        StartupError::OfflineJwtRequiresSharedAccess
    );
    let credentials = Credentials::new();
    assert_eq!(
        load_offline_jwt_policy(Some(&missing), false, Some(credentials.authentication()))
            .unwrap_err(),
        StartupError::OfflineJwtRequiresTls
    );
    let empty = SharedAccessAuthentication::new(
        auth::SharedAccessPolicy::new([]).expect("legal empty shared-access policy"),
        "tenant.servicebus.windows.net",
    )
    .expect("configured shared-access authentication");
    let configured =
        load_offline_jwt_policy(Some(&credentials.jwt_policy), true, Some(empty.clone()))
            .expect("valid policy with TLS and configured empty shared-access policy")
            .expect("enabled authentication");
    assert_eq!(
        configured
            .policy()
            .authenticate_plain("fixture-rule", "preflight-fixture-key"),
        Err(auth::SasError::InvalidCredential)
    );
    assert_eq!(
        load_offline_jwt_policy(Some(&missing), false, Some(empty)).unwrap_err(),
        StartupError::OfflineJwtRequiresTls
    );
    assert_eq!(
        fs::read_dir(root.path())
            .expect("unchanged fixture root")
            .count(),
        0
    );
}

#[test]
fn offline_policy_file_reads_stop_at_64_kib_plus_one() {
    let credentials = Credentials::new();
    let mut maximum = POLICY.as_bytes().to_vec();
    maximum.resize(MAX_OFFLINE_JWT_POLICY_BYTES, b' ');
    fs::write(&credentials.jwt_policy, &maximum).expect("maximum-size policy");
    assert!(
        load_offline_jwt_policy(
            Some(&credentials.jwt_policy),
            true,
            Some(credentials.authentication())
        )
        .is_ok()
    );
    maximum.push(b' ');
    fs::write(&credentials.jwt_policy, &maximum).expect("oversized policy");
    assert_eq!(
        load_offline_jwt_policy(
            Some(&credentials.jwt_policy),
            true,
            Some(credentials.authentication())
        )
        .unwrap_err(),
        StartupError::OfflineJwtPolicyTooLarge
    );
    assert_eq!(
        read_offline_jwt_policy_bytes(&maximum[..MAX_OFFLINE_JWT_POLICY_BYTES])
            .unwrap()
            .len(),
        MAX_OFFLINE_JWT_POLICY_BYTES
    );
    assert_eq!(
        read_offline_jwt_policy_bytes(&maximum[..]),
        Err(StartupError::OfflineJwtPolicyTooLarge)
    );
    assert_eq!(
        read_offline_jwt_policy_bytes(io::repeat(b' ')),
        Err(StartupError::OfflineJwtPolicyTooLarge)
    );
}

#[test]
fn offline_policy_metadata_rejects_nonregular_inputs() {
    let credentials = Credentials::new();
    let directory = TempDir::new().expect("nonregular fixture");
    assert_eq!(
        load_offline_jwt_policy(
            Some(directory.path()),
            true,
            Some(credentials.authentication())
        )
        .unwrap_err(),
        StartupError::OfflineJwtPolicyNotRegularFile
    );
}

#[test]
fn offline_policy_errors_remain_static_and_redacted() {
    let credentials = Credentials::new();
    let missing = credentials._directory.path().join("sentinel-secret-path");
    let error = load_offline_jwt_policy(Some(&missing), true, Some(credentials.authentication()))
        .unwrap_err();
    assert_eq!(error, StartupError::ReadOfflineJwtPolicy);
    assert_eq!(
        error.to_string(),
        "could not read the offline JWT policy file"
    );
    for (bytes, expected) in [
        (vec![0xff], StartupError::OfflineJwtPolicyNotUtf8),
        (
            b"{sentinel-config-content".to_vec(),
            StartupError::OfflineJwtPolicyConfiguration(auth::JwtError::MalformedJson),
        ),
        (
            b"{\"version\":1,\"version\":1,\"secret\":\"sentinel-token-content\"}".to_vec(),
            StartupError::OfflineJwtPolicyConfiguration(auth::JwtError::DuplicateMember),
        ),
    ] {
        fs::write(&credentials.jwt_policy, bytes).expect("invalid fixture policy");
        let error = load_offline_jwt_policy(
            Some(&credentials.jwt_policy),
            true,
            Some(credentials.authentication()),
        )
        .unwrap_err();
        assert_eq!(error, expected);
        for rendered in [error.to_string(), format!("{error:?}")] {
            for sentinel in [
                "sentinel-config-content",
                "sentinel-token-content",
                "sentinel-secret-path",
                "preflight-fixture-key",
            ] {
                assert!(!rendered.contains(sentinel));
            }
        }
    }
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("sentinel-io-detail"))
        }
    }
    let error = read_offline_jwt_policy_bytes(FailedRead).unwrap_err();
    assert_eq!(error, StartupError::ReadOfflineJwtPolicy);
    assert!(!format!("{error:?} {error}").contains("sentinel-io-detail"));
}

#[cfg(unix)]
#[test]
fn offline_policy_uses_the_original_opened_descriptor() {
    let credentials = Credentials::new();
    let opened =
        open_offline_jwt_policy_file(&credentials.jwt_policy).expect("original descriptor");
    fs::remove_file(&credentials.jwt_policy).expect("remove original path");
    fs::create_dir(&credentials.jwt_policy).expect("replacement directory");
    assert_eq!(
        read_offline_jwt_policy_regular_file(opened).unwrap(),
        POLICY.as_bytes()
    );
}

#[cfg(unix)]
#[test]
fn ordinary_policy_symlinks_remain_supported() {
    use std::os::unix::fs::symlink;
    let credentials = Credentials::new();
    let link = credentials._directory.path().join("policy-link");
    symlink(&credentials.jwt_policy, &link).expect("regular-file policy symlink");
    assert!(load_offline_jwt_policy(Some(&link), true, Some(credentials.authentication())).is_ok());
}

#[cfg(unix)]
#[test]
fn fifo_policy_paths_and_symlinks_are_refused() {
    use rustix::fs::{CWD, Mode, OFlags, mkfifoat, open};
    use std::os::unix::fs::symlink;
    let credentials = Credentials::new();
    let fifo = credentials._directory.path().join("policy-fifo");
    mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).expect("fixture FIFO");
    let link = credentials._directory.path().join("fifo-link");
    symlink(&fifo, &link).expect("FIFO symlink");
    for path in [&fifo, &link] {
        // The production open path is covered by the bounded binary probes.
        let descriptor = open(
            path,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .expect("explicit nonblocking FIFO fixture descriptor");
        let opened = fs::File::from(descriptor);
        assert_eq!(
            read_offline_jwt_policy_regular_file(opened).unwrap_err(),
            StartupError::OfflineJwtPolicyNotRegularFile
        );
    }
}

#[test]
fn offline_policy_preflight_reads_credentials_without_storage_or_bind() {
    let credentials = Credentials::new();
    let root = TempDir::new().expect("unopened storage fixture");
    let parent = root.path().join("never-created");
    let store = parent.join("store");
    let occupied = TcpListener::bind("127.0.0.1:0").expect("occupied listener fixture");
    let address = occupied.local_addr().expect("occupied local address");
    let mut arguments = credentials.arguments(true);
    arguments.storage = StorageArgument::Fjall;
    arguments.data_dir = Some(store.clone());
    arguments.listen = Some(address);
    arguments.websocket_listen = Some(address);
    arguments.admin_listen = Some(address);
    arguments.experimental_atomic_messaging_listen = Some(address);
    assert_eq!(run_with_arguments(arguments), Ok(()));
    assert!(!parent.exists());
    assert!(!store.exists());
    assert_eq!(fs::read_dir(root.path()).expect("unopened root").count(), 0);
    let mut normal = credentials.arguments(false);
    normal.storage = StorageArgument::Fjall;
    normal.data_dir = Some(store.clone());
    let prepared = prepare_configuration(&normal).expect("normal shared preparation");
    assert!(prepared.shared_access_authentication.is_some());
    assert!(prepared.tls.is_some());
    assert!(!store.exists());
    drop(occupied);
}

#[test]
fn offline_policy_refusals_are_shared_by_normal_and_check_config() {
    let credentials = Credentials::new();
    let root = TempDir::new().expect("unopened fixture");
    let store = root.path().join("store");
    fs::write(&credentials.jwt_policy, b"{}").expect("invalid policy");
    for check_config in [false, true] {
        let mut arguments = credentials.arguments(check_config);
        arguments.storage = StorageArgument::Fjall;
        arguments.data_dir = Some(store.clone());
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::OfflineJwtPolicyConfiguration(
                auth::JwtError::InvalidConfiguration
            ))
        );
        assert!(!store.exists());
    }
    assert_eq!(
        fs::read_dir(root.path())
            .expect("unopened fixture root")
            .count(),
        0
    );
}

#[test]
fn offline_policy_keeps_existing_startup_error_priorities() {
    let root = TempDir::new().expect("ordering fixture");
    let missing = root.path().join("missing-policy-and-credentials");
    let store = root.path().join("unopened-store");
    for check_config in [false, true] {
        let mut arguments = Arguments::try_parse_from(["switchyard"]).unwrap();
        arguments.check_config = check_config;
        arguments.offline_jwt_policy_file = Some(missing.clone());
        arguments.storage = StorageArgument::Fjall;
        arguments.data_dir = Some(store.clone());
        arguments.sweep_interval_millis = 0;
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::ZeroSweepInterval)
        );
        let mut arguments = Arguments::try_parse_from(["switchyard"]).unwrap();
        arguments.check_config = check_config;
        arguments.mode = ModeArgument::Production;
        arguments.experimental_atomic_messaging_listen =
            Some(SocketAddr::from(([127, 0, 0, 1], 0)));
        arguments.offline_jwt_policy_file = Some(missing.clone());
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::ExperimentalAtomicMessagingInProduction)
        );
        let mut arguments = Arguments::try_parse_from(["switchyard"]).unwrap();
        arguments.check_config = check_config;
        arguments.offline_jwt_policy_file = Some(missing.clone());
        arguments.tls_certificate = Some(missing.clone());
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::IncompleteTlsConfiguration)
        );
        let mut arguments = Arguments::try_parse_from(["switchyard"]).unwrap();
        arguments.check_config = check_config;
        arguments.offline_jwt_policy_file = Some(missing.clone());
        arguments.shared_access_key_name = Some(String::from("partial"));
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::IncompleteSharedAccessPolicy)
        );
    }
    assert!(!store.exists());
    assert_eq!(
        fs::read_dir(root.path())
            .expect("unchanged ordering root")
            .count(),
        0
    );
}

#[test]
fn offline_policy_preserves_the_sas_policy_used_by_native_admin() {
    let credentials = Credentials::new();
    let original = credentials.authentication();
    let original_grant = original
        .policy()
        .authenticate_plain("fixture-rule", "preflight-fixture-key")
        .unwrap();
    let configured =
        load_offline_jwt_policy(Some(&credentials.jwt_policy), true, Some(original.clone()))
            .unwrap()
            .unwrap();
    assert_eq!(configured.audience_host(), original.audience_host());
    let configured_grant = configured
        .policy()
        .authenticate_plain("fixture-rule", "preflight-fixture-key")
        .unwrap();
    assert_eq!(configured_grant, original_grant);
    assert!(configured_grant.allows(
        &auth::ResourceScope::namespace(configured.audience_host()).unwrap(),
        auth::Permission::Manage,
        0,
    ));
    assert!(
        configured
            .policy()
            .authenticate_plain("producer", "preflight-fixture-key")
            .is_err()
    );
    let disabled = load_offline_jwt_policy(None, false, Some(original))
        .unwrap()
        .unwrap();
    assert_eq!(
        disabled
            .policy()
            .authenticate_plain("fixture-rule", "preflight-fixture-key")
            .unwrap(),
        original_grant
    );
}

#[test]
fn offline_policy_does_not_bypass_production_or_quorum_refusals() {
    let credentials = Credentials::new();
    let root = TempDir::new().expect("production fixture root");
    let parent = root.path().join("uncreated-parent");
    let store = parent.join("uncreated-store");
    for check_config in [false, true] {
        for (storage, voters, expected) in [
            (
                StorageArgument::Memory,
                3,
                StartupError::MemoryStorageInProduction,
            ),
            (
                StorageArgument::Fjall,
                3,
                StartupError::ReplicationUnavailableInProduction,
            ),
            (
                StorageArgument::Fjall,
                2,
                StartupError::Cluster(cluster::ClusterConfigError::ProductionRequiresOddQuorum),
            ),
        ] {
            let mut arguments = credentials.arguments(check_config);
            arguments.mode = ModeArgument::Production;
            arguments.voters = voters;
            arguments.storage = storage;
            arguments.data_dir = Some(store.clone());
            assert_eq!(run_with_arguments(arguments), Err(expected));
            assert!(!parent.exists());
            assert!(!store.exists());
        }
    }
    assert_eq!(
        fs::read_dir(root.path())
            .expect("unopened production fixture root")
            .count(),
        0
    );
}
