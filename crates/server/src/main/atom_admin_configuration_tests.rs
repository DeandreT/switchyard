use std::{io, net::TcpListener};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use sha2::Sha256;
use tempfile::TempDir;

use super::atom_admin_configuration::{
    MAX_ATOM_ADMIN_KEY_BYTES, load_atom_admin_configuration, open_atom_admin_key_file,
    read_atom_admin_key_bytes, read_atom_admin_key_regular_file,
};
use super::*;

const ATOM_KEY: &str = "atom-fixture-key";
const LEGACY_KEY: &str = "legacy-fixture-key";

struct Credentials {
    directory: TempDir,
    certificate: PathBuf,
    private_key: PathBuf,
    atom_key: PathBuf,
    legacy_key: PathBuf,
}

impl Credentials {
    fn new() -> Self {
        let directory = TempDir::new().expect("Atom configuration fixture");
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec![String::from("localhost")])
                .expect("disposable TLS identity");
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let atom_key = directory.path().join("atom-key");
        let legacy_key = directory.path().join("legacy-key");
        fs::write(&certificate, cert.pem()).expect("fixture certificate");
        fs::write(&private_key, key_pair.serialize_pem()).expect("fixture private key");
        fs::write(&atom_key, format!("{ATOM_KEY}\r\n")).expect("fixture Atom key");
        fs::write(&legacy_key, format!("{LEGACY_KEY}\n")).expect("fixture legacy key");
        Self {
            directory,
            certificate,
            private_key,
            atom_key,
            legacy_key,
        }
    }

    fn arguments(&self, check_config: bool) -> Arguments {
        let mut arguments = Arguments::try_parse_from(["switchyard"]).unwrap();
        arguments.check_config = check_config;
        arguments.namespace = "localhost".into();
        arguments.tls_certificate = Some(self.certificate.clone());
        arguments.tls_private_key = Some(self.private_key.clone());
        arguments.shared_access_key_name = Some("legacy-rule".into());
        arguments.shared_access_key_file = Some(self.legacy_key.clone());
        arguments.atom_admin_listen = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
        arguments.atom_admin_audience_host = Some("LOCALHOST".into());
        arguments.atom_admin_key_name = Some("atom-rule".into());
        arguments.atom_admin_key_file = Some(self.atom_key.clone());
        arguments
    }

    fn atom(&self) -> atom_admin_configuration::PreparedAtomAdmin {
        load_atom_admin_configuration(&self.arguments(true), true)
            .unwrap()
            .unwrap()
    }
}

fn token(audience: &str, name: &str, key: &str, expiry: u64) -> String {
    let resource = url::form_urlencoded::byte_serialize(audience.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap();
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = url::form_urlencoded::byte_serialize(signature.as_bytes()).collect::<String>();
    let name = url::form_urlencoded::byte_serialize(name.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={name}")
}

#[test]
fn atom_flags_are_opt_in_with_independent_typed_values() {
    let defaults = Arguments::try_parse_from(["switchyard"]).unwrap();
    assert!(defaults.atom_admin_listen.is_none());
    assert!(defaults.atom_admin_audience_host.is_none());
    assert!(defaults.atom_admin_key_name.is_none());
    assert!(defaults.atom_admin_key_file.is_none());
    let prepared = prepare_configuration(&defaults).unwrap();
    assert!(prepared.atom_admin.is_none());
    assert!(prepared.tls.is_none() && prepared.shared_access_authentication.is_none());
    assert_eq!(prepared.listen.port(), 5672);
    let enabled = Arguments::try_parse_from([
        "switchyard",
        "--listen",
        "127.0.0.1:5671",
        "--admin-listen",
        "127.0.0.1:9080",
        "--atom-admin-listen",
        "127.0.0.1:9443",
        "--atom-admin-audience-host",
        "localhost",
        "--atom-admin-key-name",
        "atom-rule",
        "--atom-admin-key-file",
        "atom-key",
    ])
    .unwrap();
    assert_eq!(enabled.listen.unwrap().port(), 5671);
    assert_eq!(enabled.admin_listen.unwrap().port(), 9080);
    assert_eq!(enabled.atom_admin_listen.unwrap().port(), 9443);
    assert_eq!(
        enabled.atom_admin_audience_host.as_deref(),
        Some("localhost")
    );
    assert_eq!(enabled.atom_admin_key_name.as_deref(), Some("atom-rule"));
    assert_eq!(enabled.atom_admin_key_file, Some(PathBuf::from("atom-key")));
    assert!(enabled.shared_access_key_name.is_none());
    assert!(enabled.shared_access_key_file.is_none());
    assert!(enabled.offline_jwt_policy_file.is_none());
    assert!(
        Arguments::try_parse_from(["switchyard", "--atom-admin-listen", "not-a-socket-address",])
            .is_err()
    );
}

#[test]
fn disabled_atom_does_not_infer_tls_or_credentials() {
    let credentials = Credentials::new();
    let mut arguments = credentials.arguments(true);
    arguments.atom_admin_listen = None;
    arguments.atom_admin_audience_host = None;
    arguments.atom_admin_key_name = None;
    arguments.atom_admin_key_file = None;
    assert!(
        load_atom_admin_configuration(&arguments, false)
            .unwrap()
            .is_none()
    );
    let prepared = prepare_configuration(&arguments).unwrap();
    assert!(prepared.atom_admin.is_none());
    assert_eq!(
        prepared
            .shared_access_authentication
            .unwrap()
            .audience_host(),
        "localhost.servicebus.windows.net"
    );
    assert!(prepared.tls.is_some());
}

#[test]
fn atom_configuration_does_not_require_or_supply_legacy_authentication() {
    let credentials = Credentials::new();
    let mut arguments = credentials.arguments(true);
    arguments.shared_access_key_name = None;
    arguments.shared_access_key_file = None;
    let prepared = prepare_configuration(&arguments).unwrap();
    assert!(prepared.shared_access_authentication.is_none());
    assert!(
        prepared
            .atom_admin
            .unwrap()
            .policy
            .authenticate_plain("atom-rule", ATOM_KEY)
            .is_ok()
    );
    assert!(prepared.tls.is_some());
    assert_eq!(run_with_arguments(arguments), Ok(()));
}

#[test]
fn atom_extras_require_their_listener_before_key_io() {
    let root = TempDir::new().unwrap();
    let missing = root.path().join("must-not-be-opened");
    for extra in 0..3 {
        let mut arguments = Arguments::try_parse_from(["switchyard"]).unwrap();
        match extra {
            0 => arguments.atom_admin_audience_host = Some("localhost".into()),
            1 => arguments.atom_admin_key_name = Some("atom-rule".into()),
            _ => arguments.atom_admin_key_file = Some(missing.clone()),
        }
        assert_eq!(
            load_atom_admin_configuration(&arguments, false)
                .err()
                .unwrap(),
            StartupError::AtomAdminRequiresListener
        );
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn partial_atom_tuples_are_refused_before_tls_or_key_io() {
    let root = TempDir::new().unwrap();
    for mask in 0..7 {
        let mut arguments = Arguments::try_parse_from(["switchyard"]).unwrap();
        arguments.atom_admin_listen = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
        if mask & 1 != 0 {
            arguments.atom_admin_audience_host = Some("localhost".into());
        }
        if mask & 2 != 0 {
            arguments.atom_admin_key_name = Some("atom-rule".into());
        }
        if mask & 4 != 0 {
            arguments.atom_admin_key_file = Some(root.path().join("missing"));
        }
        assert_eq!(
            load_atom_admin_configuration(&arguments, false)
                .err()
                .unwrap(),
            StartupError::IncompleteAtomAdminConfiguration
        );
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn atom_always_requires_tls_before_audience_or_key_io() {
    let credentials = Credentials::new();
    let mut arguments = credentials.arguments(true);
    arguments.atom_admin_audience_host = Some("invalid/audience".into());
    arguments.atom_admin_key_name = Some(String::new());
    arguments.atom_admin_key_file = Some(credentials.directory.path().join("missing"));
    assert_eq!(
        load_atom_admin_configuration(&arguments, false)
            .err()
            .unwrap(),
        StartupError::AtomAdminRequiresTls
    );
}

#[test]
fn atom_scope_and_rule_validation_precede_key_io() {
    let credentials = Credentials::new();
    let mut arguments = credentials.arguments(true);
    arguments.atom_admin_key_file = Some(credentials.directory.path().join("missing"));
    for host in [
        "",
        "localhost/orders",
        "localhost:8443",
        "localhost?secret",
        "user@localhost",
    ] {
        arguments.atom_admin_audience_host = Some(host.into());
        arguments.atom_admin_key_name = Some(String::new());
        assert_eq!(
            load_atom_admin_configuration(&arguments, true)
                .err()
                .unwrap(),
            StartupError::AtomAdminInvalidAudience
        );
    }
    arguments.atom_admin_audience_host = Some("LOCALHOST".into());
    assert_eq!(
        load_atom_admin_configuration(&arguments, true)
            .err()
            .unwrap(),
        StartupError::AtomAdminInvalidKeyName
    );
}

#[test]
fn atom_key_reader_bounds_actual_bytes_not_only_metadata() {
    struct CountingReader(usize);
    impl Read for &mut CountingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            buffer.fill(b'k');
            self.0 += buffer.len();
            Ok(buffer.len())
        }
    }
    let mut reader = CountingReader(0);
    assert_eq!(
        read_atom_admin_key_bytes(&mut reader),
        Err(StartupError::AtomAdminKeyTooLarge)
    );
    assert_eq!(reader.0, MAX_ATOM_ADMIN_KEY_BYTES + 1);
    assert_eq!(
        read_atom_admin_key_bytes(&vec![b'k'; MAX_ATOM_ADMIN_KEY_BYTES][..])
            .unwrap()
            .len(),
        MAX_ATOM_ADMIN_KEY_BYTES
    );
    assert_eq!(
        read_atom_admin_key_bytes(&vec![b'k'; MAX_ATOM_ADMIN_KEY_BYTES + 1][..]),
        Err(StartupError::AtomAdminKeyTooLarge)
    );
}

#[test]
fn atom_regular_key_file_cap_includes_terminal_newlines() {
    let credentials = Credentials::new();
    fs::write(&credentials.atom_key, vec![b'k'; MAX_ATOM_ADMIN_KEY_BYTES]).unwrap();
    assert!(load_atom_admin_configuration(&credentials.arguments(true), true).is_ok());
    let mut oversized = vec![b'k'; MAX_ATOM_ADMIN_KEY_BYTES];
    oversized.push(b'\n');
    fs::write(&credentials.atom_key, oversized).unwrap();
    assert_eq!(
        load_atom_admin_configuration(&credentials.arguments(true), true)
            .err()
            .unwrap(),
        StartupError::AtomAdminKeyTooLarge
    );
}

#[test]
fn atom_key_errors_are_static_and_redacted() {
    let credentials = Credentials::new();
    let mut arguments = credentials.arguments(true);
    arguments.atom_admin_key_file = Some(credentials.directory.path().join("sentinel-secret-path"));
    let error = load_atom_admin_configuration(&arguments, true)
        .err()
        .unwrap();
    assert_eq!(error, StartupError::ReadAtomAdminKey);
    assert_eq!(
        error.to_string(),
        "could not read the Atom administration key file"
    );
    for (bytes, expected) in [
        (vec![0xff], StartupError::AtomAdminKeyNotUtf8),
        (Vec::new(), StartupError::AtomAdminInvalidKey),
        (b"\r\n\r\n".to_vec(), StartupError::AtomAdminInvalidKey),
    ] {
        fs::write(&credentials.atom_key, bytes).unwrap();
        let error = load_atom_admin_configuration(&credentials.arguments(true), true)
            .err()
            .unwrap();
        assert_eq!(error, expected);
        assert!(!format!("{error:?} {error}").contains("sentinel"));
        assert!(!format!("{error:?} {error}").contains(ATOM_KEY));
    }
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("sentinel-io-detail"))
        }
    }
    let error = read_atom_admin_key_bytes(FailedRead).unwrap_err();
    assert_eq!(error, StartupError::ReadAtomAdminKey);
    assert!(!format!("{error:?} {error}").contains("sentinel"));
}

#[test]
fn atom_key_retains_utf8_spaces_and_encoded_text() {
    let credentials = Credentials::new();
    let key = " \u{79d8}\u{5bc6}+YWJj= ";
    fs::write(&credentials.atom_key, format!("{key}\r\n\n")).unwrap();
    let atom = credentials.atom();
    let signed = token("https://localhost/orders", "atom-rule", key, 200);
    let grant = atom.policy.authenticate_atom_sas(&signed, 100).unwrap();
    assert_eq!(
        grant.scope(),
        &auth::ResourceScope::entity("localhost", "orders").unwrap()
    );
    assert!(grant.allows(grant.scope(), auth::Permission::Manage, 100));
    assert!(atom.policy.authenticate_plain("atom-rule", key).is_ok());
    assert!(
        atom.policy
            .authenticate_plain("atom-rule", key.trim())
            .is_err()
    );
    assert!(atom.policy.authenticate_plain("atom-rule", "abc").is_err());
}

#[test]
fn atom_regular_file_metadata_refuses_directories() {
    let credentials = Credentials::new();
    let mut arguments = credentials.arguments(true);
    arguments.atom_admin_key_file = Some(credentials.directory.path().to_path_buf());
    assert_eq!(
        load_atom_admin_configuration(&arguments, true)
            .err()
            .unwrap(),
        StartupError::AtomAdminKeyNotRegularFile
    );
}

#[cfg(unix)]
#[test]
fn atom_key_reads_the_original_descriptor_after_path_replacement() {
    let credentials = Credentials::new();
    let opened = open_atom_admin_key_file(&credentials.atom_key).unwrap();
    fs::remove_file(&credentials.atom_key).unwrap();
    fs::create_dir(&credentials.atom_key).unwrap();
    assert_eq!(
        read_atom_admin_key_regular_file(opened).unwrap(),
        format!("{ATOM_KEY}\r\n").as_bytes()
    );
}

#[cfg(unix)]
#[test]
fn atom_key_regular_symlinks_remain_supported() {
    use std::os::unix::fs::symlink;
    let credentials = Credentials::new();
    let link = credentials.directory.path().join("regular-link");
    symlink(&credentials.atom_key, &link).unwrap();
    let mut arguments = credentials.arguments(true);
    arguments.atom_admin_key_file = Some(link);
    assert!(load_atom_admin_configuration(&arguments, true).is_ok());
}

#[cfg(unix)]
#[test]
fn atom_key_fifo_descriptors_and_symlinks_are_not_regular_files() {
    use rustix::fs::{CWD, Mode, OFlags, mkfifoat, open};
    use std::os::unix::fs::symlink;
    let credentials = Credentials::new();
    let fifo = credentials.directory.path().join("key-fifo");
    let link = credentials.directory.path().join("fifo-link");
    mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
    symlink(&fifo, &link).unwrap();
    for path in [&fifo, &link] {
        let descriptor = open(
            path,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .unwrap();
        assert_eq!(
            read_atom_admin_key_regular_file(fs::File::from(descriptor)).unwrap_err(),
            StartupError::AtomAdminKeyNotRegularFile
        );
    }
    let mut arguments = credentials.arguments(true);
    arguments.atom_admin_listen = None;
    arguments.atom_admin_key_file = Some(fifo);
    assert_eq!(
        load_atom_admin_configuration(&arguments, true)
            .err()
            .unwrap(),
        StartupError::AtomAdminRequiresListener
    );
}

#[test]
fn atom_preserves_legacy_startup_validation_order() {
    let credentials = Credentials::new();
    let missing = credentials.directory.path().join("missing-legacy-file");
    for check in [false, true] {
        let mut arguments = credentials.arguments(check);
        arguments.atom_admin_listen = None;
        arguments.sweep_interval_millis = 0;
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::ZeroSweepInterval)
        );
        let mut arguments = credentials.arguments(check);
        arguments.atom_admin_listen = None;
        arguments.tls_private_key = None;
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::IncompleteTlsConfiguration)
        );
        let mut arguments = credentials.arguments(check);
        arguments.atom_admin_listen = None;
        arguments.shared_access_key_file = None;
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::IncompleteSharedAccessPolicy)
        );
        let mut arguments = credentials.arguments(check);
        arguments.atom_admin_listen = None;
        arguments.offline_jwt_policy_file = Some(missing.clone());
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::ReadOfflineJwtPolicy)
        );
        let mut arguments = credentials.arguments(check);
        arguments.atom_admin_listen = None;
        arguments.storage = StorageArgument::Fjall;
        arguments.data_dir = None;
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::MissingDataDirectory)
        );
        let mut arguments = credentials.arguments(check);
        arguments.atom_admin_listen = None;
        arguments.namespace = ".invalid".into();
        arguments.shared_access_key_name = None;
        arguments.shared_access_key_file = None;
        assert!(matches!(
            run_with_arguments(arguments),
            Err(StartupError::Protocol(_))
        ));
    }
}

#[test]
fn atom_check_config_loads_both_policies_without_bind_or_store_open() {
    let credentials = Credentials::new();
    let root = TempDir::new().unwrap();
    let parent = root.path().join("unopened-parent");
    let store = parent.join("store");
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let mut arguments = credentials.arguments(true);
    arguments.listen = Some(address);
    arguments.admin_listen = Some(address);
    arguments.websocket_listen = Some(address);
    arguments.experimental_atomic_messaging_listen = Some(address);
    arguments.atom_admin_listen = Some(address);
    arguments.storage = StorageArgument::Fjall;
    arguments.data_dir = Some(store.clone());
    let prepared = prepare_configuration(&arguments).unwrap();
    assert_eq!(prepared.namespace.as_str(), "localhost");
    assert_eq!(prepared.listen, address);
    assert_eq!(prepared.atom_admin.as_ref().unwrap().address, address);
    assert!(
        prepared
            .shared_access_authentication
            .as_ref()
            .unwrap()
            .policy()
            .authenticate_plain("legacy-rule", LEGACY_KEY)
            .is_ok()
    );
    assert!(
        prepared
            .atom_admin
            .as_ref()
            .unwrap()
            .policy
            .authenticate_plain("atom-rule", ATOM_KEY)
            .is_ok()
    );
    assert_eq!(run_with_arguments(arguments), Ok(()));
    assert!(!parent.exists() && !store.exists());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    drop(occupied);
}

#[test]
fn atom_failures_match_normal_and_check_config_before_storage() {
    let credentials = Credentials::new();
    let root = TempDir::new().unwrap();
    let store = root.path().join("unopened-store");
    for check in [false, true] {
        let mut arguments = credentials.arguments(check);
        arguments.storage = StorageArgument::Fjall;
        arguments.data_dir = Some(store.clone());
        arguments.atom_admin_key_file = Some(root.path().join("missing-atom-key"));
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::ReadAtomAdminKey)
        );
        assert!(!store.exists());
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn atom_keeps_dotless_legacy_audiences_and_profiles_separate() {
    let credentials = Credentials::new();
    let prepared = prepare_configuration(&credentials.arguments(true)).unwrap();
    let legacy = prepared.shared_access_authentication.unwrap();
    let atom = prepared.atom_admin.unwrap();
    assert_eq!(legacy.audience_host(), "localhost.servicebus.windows.net");
    assert_eq!(atom.scope.host(), "localhost");
    assert!(atom.scope.path().next().is_none());
    let signed = token("https://localhost/orders", "atom-rule", ATOM_KEY, 200);
    let grant = atom.policy.authenticate_atom_sas(&signed, 100).unwrap();
    assert!(grant.allows(
        &auth::ResourceScope::entity("localhost", "orders").unwrap(),
        auth::Permission::Manage,
        100
    ));
    assert!(atom.policy.authenticate_sas(&signed, 100).is_err());
    assert!(legacy.policy().authenticate_atom_sas(&signed, 100).is_err());
    for audience in [
        "amqps://localhost/orders",
        "https://localhost.servicebus.windows.net/orders",
        "https://other.example/orders",
        "https://localhost:8443/orders",
    ] {
        assert!(
            atom.policy
                .authenticate_atom_sas(&token(audience, "atom-rule", ATOM_KEY, 200), 100)
                .is_err()
        );
    }
    let legacy_signed = token(
        "amqps://localhost.servicebus.windows.net/orders",
        "legacy-rule",
        LEGACY_KEY,
        200,
    );
    assert!(
        legacy
            .policy()
            .authenticate_sas(&legacy_signed, 100)
            .is_ok()
    );
    assert!(atom.policy.authenticate_sas(&legacy_signed, 100).is_err());
    assert!(
        atom.policy
            .authenticate_plain("legacy-rule", LEGACY_KEY)
            .is_err()
    );
    assert!(
        legacy
            .policy()
            .authenticate_plain("atom-rule", ATOM_KEY)
            .is_err()
    );
}

#[test]
fn prepared_atom_policy_and_tls_clone_do_not_reread_credentials() {
    let credentials = Credentials::new();
    let prepared = prepare_configuration(&credentials.arguments(true)).unwrap();
    let atom = prepared.atom_admin.unwrap();
    let tls = prepared.tls.unwrap();
    let protocols = tls.amqp.alpn_protocols.clone();
    let certificate = tls.certificate_chain.clone();
    let private_key = tls.private_key.clone();
    fs::remove_file(&credentials.atom_key).unwrap();
    fs::create_dir(&credentials.atom_key).unwrap();
    fs::remove_file(&credentials.certificate).unwrap();
    fs::remove_file(&credentials.private_key).unwrap();
    let grant = atom
        .policy
        .authenticate_plain("atom-rule", ATOM_KEY)
        .unwrap();
    assert_eq!(grant.scope(), &atom.scope);
    let broker = Broker::spawn(LocalProposer::new(
        domain::StateMachine::new(storage::MemoryStore::default()),
        SystemClock,
    ));
    let listener = atom
        .listener(broker.handle(), prepared.namespace, &tls)
        .unwrap();
    assert_eq!(tls.amqp.alpn_protocols, protocols);
    assert_eq!(tls.certificate_chain, certificate);
    assert_eq!(tls.private_key, private_key);
    drop(listener);
    drop(broker);
}
