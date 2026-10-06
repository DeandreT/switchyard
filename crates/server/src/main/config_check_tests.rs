use std::net::TcpListener;

use rcgen::{CertifiedKey, generate_simple_self_signed};
use tempfile::TempDir;

use super::*;

fn parse(arguments: &[&str], check_config: bool) -> Arguments {
    let mut arguments = arguments.to_vec();
    if check_config {
        arguments.push("--check-config");
    }
    Arguments::try_parse_from(arguments).expect("startup fixture arguments")
}

struct Credentials {
    _directory: TempDir,
    certificate: PathBuf,
    private_key: PathBuf,
    shared_key: PathBuf,
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
        fs::write(&certificate, cert.pem()).expect("fixture certificate");
        fs::write(&private_key, key_pair.serialize_pem()).expect("fixture private key");
        fs::write(&shared_key, "preflight-fixture-key\n").expect("fixture shared key");
        Self {
            _directory: directory,
            certificate,
            private_key,
            shared_key,
        }
    }

    fn arguments(&self, check_config: bool) -> Arguments {
        parse(
            &[
                "switchyard",
                "--namespace",
                "tenant.servicebus.windows.net",
                "--tls-certificate",
                self.certificate.to_str().expect("fixture certificate path"),
                "--tls-private-key",
                self.private_key.to_str().expect("fixture private-key path"),
                "--shared-access-key-name",
                "fixture-rule",
                "--shared-access-key-file",
                self.shared_key.to_str().expect("fixture shared-key path"),
            ],
            check_config,
        )
    }
}

#[test]
fn check_config_is_opt_in_and_keeps_transport_defaults() {
    let ordinary = parse(&["switchyard"], false);
    assert!(!ordinary.check_config);
    let checked = parse(&["switchyard"], true);
    assert!(checked.check_config);
    let prepared = prepare_configuration(&checked).expect("default preflight");
    assert_eq!(prepared.cluster.mode, DeploymentMode::Development);
    assert_eq!(prepared.cluster.voters, 1);
    assert_eq!(prepared.storage, StorageChoice::Memory);
    assert_eq!(prepared.namespace.as_str(), "development");
    assert_eq!(prepared.listen.port(), 5672);
    assert_eq!(prepared.interval, DEFAULT_SWEEP_INTERVAL);
    assert!(prepared.tls.is_none());
    assert!(prepared.shared_access_authentication.is_none());
    for flag in ["--listen", "--admin-listen", "--websocket-listen"] {
        assert!(
            Arguments::try_parse_from(["switchyard", "--check-config", flag, "not-an-address"])
                .is_err()
        );
    }
}

#[test]
fn check_config_does_not_bind_any_configured_listener() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("occupied fixture address");
    let address = occupied
        .local_addr()
        .expect("occupied local address")
        .to_string();
    let arguments = parse(
        &[
            "switchyard",
            "--listen",
            &address,
            "--admin-listen",
            &address,
            "--websocket-listen",
            &address,
            "--experimental-atomic-messaging-listen",
            &address,
            "--development-maintenance-readiness",
        ],
        true,
    );
    let result = run_with_arguments(arguments);
    drop(occupied);
    assert_eq!(result, Ok(()));
}

#[test]
fn check_config_does_not_create_or_inspect_durable_directories() {
    let root = TempDir::new().expect("storage fixture root");
    let parent = root.path().join("uncreated-parent");
    let unopened = parent.join("uncreated-store");
    let existing = root.path().join("not-a-store");
    fs::create_dir(&existing).expect("existing fixture directory");
    let sentinel = existing.join("unrelated-file");
    fs::write(&sentinel, b"not a database").expect("existing fixture contents");
    for directory in [&unopened, &existing] {
        let arguments = parse(
            &[
                "switchyard",
                "--storage",
                "fjall",
                "--data-dir",
                directory.to_str().expect("fixture storage path"),
            ],
            true,
        );
        assert_eq!(run_with_arguments(arguments), Ok(()));
        assert!(!parent.exists());
        assert!(!unopened.exists());
        assert_eq!(
            fs::read(&sentinel).expect("sentinel unchanged"),
            b"not a database"
        );
        assert_eq!(
            fs::read_dir(&existing).expect("existing directory").count(),
            1
        );
        assert_eq!(fs::read_dir(root.path()).expect("fixture root").count(), 1);
    }
}

#[test]
fn namespace_validation_precedes_store_open_for_both_startup_paths() {
    let root = TempDir::new().expect("namespace fixture root");
    let parent = root.path().join("uncreated-parent");
    let store = parent.join("uncreated-store");
    for check_config in [false, true] {
        let arguments = parse(
            &[
                "switchyard",
                "--namespace",
                ".invalid",
                "--storage",
                "fjall",
                "--data-dir",
                store.to_str().expect("fixture storage path"),
            ],
            check_config,
        );
        assert!(matches!(
            run_with_arguments(arguments),
            Err(StartupError::Protocol(_))
        ));
        assert!(!parent.exists());
        assert!(!store.exists());
        assert_eq!(fs::read_dir(root.path()).expect("fixture root").count(), 0);
    }
}

#[test]
fn zero_sweep_interval_refuses_before_credential_or_store_io() {
    let root = TempDir::new().expect("zero-interval fixture root");
    let parent = root.path().join("uncreated-parent");
    let store = parent.join("uncreated-store");
    let missing = root.path().join("missing-credential");
    for check_config in [false, true] {
        let arguments = parse(
            &[
                "switchyard",
                "--sweep-interval-millis",
                "0",
                "--storage",
                "fjall",
                "--data-dir",
                store.to_str().expect("fixture storage path"),
                "--tls-certificate",
                missing.to_str().expect("missing credential path"),
                "--tls-private-key",
                missing.to_str().expect("missing credential path"),
                "--shared-access-key-name",
                "fixture-rule",
                "--shared-access-key-file",
                missing.to_str().expect("missing credential path"),
            ],
            check_config,
        );
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::ZeroSweepInterval)
        );
        assert!(!parent.exists());
        assert!(!store.exists());
        assert_eq!(fs::read_dir(root.path()).expect("fixture root").count(), 0);
    }
    assert_eq!(
        StartupError::ZeroSweepInterval.to_string(),
        "--sweep-interval-millis must be greater than zero"
    );
}

#[test]
fn positive_sweep_intervals_are_prepared_without_narrowing() {
    for millis in [1, 1_000, u64::MAX] {
        let value = millis.to_string();
        let arguments = parse(&["switchyard", "--sweep-interval-millis", &value], true);
        let prepared = prepare_configuration(&arguments).expect("positive interval");
        assert_eq!(prepared.interval, Duration::from_millis(millis));
    }
}

#[test]
fn storage_preflight_preserves_open_refusal_priority_without_io() {
    let root = TempDir::new().expect("storage policy fixture root");
    let parent = root.path().join("uncreated-parent");
    let store = parent.join("uncreated-store");
    let durable = StorageChoice::Durable {
        directory: store.clone(),
    };
    for (mode, voters, expected) in [
        (
            DeploymentMode::Development,
            2,
            StartupError::Cluster(cluster::ClusterConfigError::DevelopmentRequiresOneVoter),
        ),
        (
            DeploymentMode::Production,
            2,
            StartupError::Cluster(cluster::ClusterConfigError::ProductionRequiresOddQuorum),
        ),
    ] {
        let cluster = ClusterConfig { mode, voters };
        for storage in [StorageChoice::Memory, durable.clone()] {
            assert_eq!(
                server::validate_storage_configuration(cluster, &storage),
                Err(expected.clone())
            );
            assert_eq!(server::open(cluster, storage).err(), Some(expected.clone()));
        }
    }
    let production = ClusterConfig {
        mode: DeploymentMode::Production,
        voters: 3,
    };
    for (storage, expected) in [
        (
            StorageChoice::Memory,
            StartupError::MemoryStorageInProduction,
        ),
        (
            durable.clone(),
            StartupError::ReplicationUnavailableInProduction,
        ),
    ] {
        assert_eq!(
            server::validate_storage_configuration(production, &storage),
            Err(expected.clone())
        );
        assert_eq!(server::open(production, storage).err(), Some(expected));
    }
    assert_eq!(
        server::validate_storage_configuration(
            ClusterConfig {
                mode: DeploymentMode::Development,
                voters: 1
            },
            &durable,
        ),
        Ok(())
    );
    assert!(!parent.exists());
    assert!(!store.exists());
    assert_eq!(fs::read_dir(root.path()).expect("fixture root").count(), 0);
}

#[test]
fn check_config_retains_feature_and_missing_directory_refusals() {
    for check_config in [false, true] {
        for (argv, expected) in [
            (
                vec![
                    "switchyard",
                    "--mode",
                    "production",
                    "--voters",
                    "2",
                    "--sweep-interval-millis",
                    "0",
                    "--experimental-atomic-messaging-listen",
                    "127.0.0.1:0",
                ],
                StartupError::ExperimentalAtomicMessagingInProduction,
            ),
            (
                vec![
                    "switchyard",
                    "--mode",
                    "production",
                    "--sweep-interval-millis",
                    "0",
                    "--development-maintenance-readiness",
                ],
                StartupError::DevelopmentMaintenanceReadinessInProduction,
            ),
            (
                vec![
                    "switchyard",
                    "--sweep-interval-millis",
                    "0",
                    "--development-maintenance-readiness",
                ],
                StartupError::DevelopmentMaintenanceReadinessRequiresAdminListener,
            ),
            (
                vec!["switchyard", "--storage", "fjall"],
                StartupError::MissingDataDirectory,
            ),
        ] {
            assert_eq!(
                run_with_arguments(parse(&argv, check_config)),
                Err(expected)
            );
        }
        assert_eq!(
            run_with_arguments(parse(&["switchyard", "--mode", "production"], check_config)),
            Err(StartupError::TlsRequiredInProduction)
        );
    }
}

#[test]
fn credential_validation_is_shared_without_opening_storage() {
    let root = TempDir::new().expect("credential refusal fixture root");
    let store = root.path().join("uncreated-store");
    let missing = root.path().join("missing-credential");
    for check_config in [false, true] {
        let mut arguments = parse(&["switchyard"], check_config);
        arguments.storage = StorageArgument::Fjall;
        arguments.data_dir = Some(store.clone());
        arguments.tls_certificate = Some(missing.clone());
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::IncompleteTlsConfiguration)
        );
        let mut arguments = parse(&["switchyard"], check_config);
        arguments.data_dir = Some(store.clone());
        arguments.storage = StorageArgument::Fjall;
        arguments.tls_certificate = Some(missing.clone());
        arguments.tls_private_key = Some(missing.clone());
        assert!(matches!(
            run_with_arguments(arguments),
            Err(StartupError::ReadTlsCredentials { .. })
        ));
        let mut arguments = parse(&["switchyard"], check_config);
        arguments.data_dir = Some(store.clone());
        arguments.storage = StorageArgument::Fjall;
        arguments.shared_access_key_name = Some(String::from("fixture-rule"));
        arguments.shared_access_key_file = Some(missing.clone());
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::AuthenticationRequiresTls)
        );
        assert!(!store.exists());
        assert_eq!(fs::read_dir(root.path()).expect("fixture root").count(), 0);
    }
    let credentials = Credentials::new();
    for check_config in [false, true] {
        let arguments = credentials.arguments(check_config);
        let prepared = prepare_configuration(&arguments).expect("same valid credential preflight");
        assert!(prepared.tls.is_some());
        assert!(prepared.shared_access_authentication.is_some());
        assert_eq!(prepared.listen.port(), protocol_amqp::AMQP_TLS_PORT);
        assert_eq!(prepared.namespace.as_str(), "tenant");
    }
    assert_eq!(run_with_arguments(credentials.arguments(true)), Ok(()));
    fs::write(&credentials.shared_key, b"").expect("invalid fixture key");
    for check_config in [false, true] {
        let mut arguments = credentials.arguments(check_config);
        arguments.storage = StorageArgument::Fjall;
        arguments.data_dir = Some(store.clone());
        assert!(matches!(
            run_with_arguments(arguments),
            Err(StartupError::AuthPolicy(_))
        ));
        assert!(!store.exists());
        assert_eq!(fs::read_dir(root.path()).expect("fixture root").count(), 0);
    }
}

#[test]
fn check_config_does_not_bypass_production_storage_refusals() {
    let credentials = Credentials::new();
    let root = TempDir::new().expect("production fixture root");
    let store = root.path().join("uncreated-store");
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
        let mut arguments = credentials.arguments(true);
        arguments.mode = ModeArgument::Production;
        arguments.voters = voters;
        arguments.storage = storage;
        arguments.data_dir = Some(store.clone());
        assert_eq!(run_with_arguments(arguments), Err(expected));
        assert!(!store.exists());
        assert_eq!(fs::read_dir(root.path()).expect("fixture root").count(), 0);
    }
}
