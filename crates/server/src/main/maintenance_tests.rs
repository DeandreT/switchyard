use super::*;

#[test]
fn maintenance_flag_is_opt_in_and_requires_admin_socket() {
    let ordinary = Arguments::try_parse_from(["switchyard"]).expect("ordinary defaults");
    assert!(!ordinary.development_maintenance_readiness);
    assert!(ordinary.admin_listen.is_none());
    let enabled = Arguments::try_parse_from(["switchyard", "--development-maintenance-readiness"])
        .expect("enabled flag");
    assert_eq!(
        run_with_arguments(enabled),
        Err(StartupError::DevelopmentMaintenanceReadinessRequiresAdminListener)
    );
    let enabled = Arguments::try_parse_from([
        "switchyard",
        "--development-maintenance-readiness",
        "--admin-listen",
        "127.0.0.1:0",
    ])
    .expect("separate admin address");
    assert!(enabled.listen.is_none());
    assert!(enabled.development_maintenance_readiness);
    assert!(
        validate_development_maintenance_readiness(
            DeploymentMode::Development,
            true,
            enabled.admin_listen
        )
        .is_ok()
    );
    assert_eq!(
        StartupError::DevelopmentMaintenanceReadinessRequiresAdminListener.to_string(),
        "--development-maintenance-readiness requires --admin-listen"
    );
}

#[test]
fn production_flag_refuses_before_any_startup_io() {
    let directory = tempfile::TempDir::new().expect("startup fixture");
    let store = directory.path().join("never-opened");
    let missing = directory.path().join("missing-credentials");
    for voters in ["2", "3"] {
        for atomic in [false, true] {
            let mut argv = vec![
                "switchyard",
                "--mode",
                "production",
                "--voters",
                voters,
                "--development-maintenance-readiness",
                "--storage",
                "fjall",
                "--data-dir",
                store.to_str().expect("test path"),
                "--tls-certificate",
                missing.to_str().expect("test path"),
                "--tls-private-key",
                missing.to_str().expect("test path"),
                "--shared-access-key-name",
                "rule",
                "--shared-access-key-file",
                missing.to_str().expect("test path"),
            ];
            if atomic {
                argv.extend(["--experimental-atomic-messaging-listen", "127.0.0.1:0"]);
            }
            let arguments = Arguments::try_parse_from(argv).expect("unsupported startup");
            assert_eq!(
                run_with_arguments(arguments),
                Err(if atomic {
                    StartupError::ExperimentalAtomicMessagingInProduction
                } else {
                    StartupError::DevelopmentMaintenanceReadinessInProduction
                })
            );
            assert!(!store.exists());
            assert_eq!(
                std::fs::read_dir(directory.path())
                    .expect("directory")
                    .count(),
                0
            );
        }
    }
    assert_eq!(
        StartupError::DevelopmentMaintenanceReadinessInProduction.to_string(),
        "--development-maintenance-readiness is only available in development mode"
    );
}

#[test]
fn disabled_flag_preserves_existing_startup_refusals() {
    let production = Arguments::try_parse_from(["switchyard", "--mode", "production"])
        .expect("ordinary production");
    assert!(!production.development_maintenance_readiness);
    assert_eq!(
        run_with_arguments(production),
        Err(StartupError::TlsRequiredInProduction)
    );
    assert!(
        validate_development_maintenance_readiness(DeploymentMode::Production, false, None).is_ok()
    );
    assert!(matches!(
        load_shared_access_authentication(DeploymentMode::Production, true, "tenant", None, None),
        Err(StartupError::AuthenticationRequiredInProduction)
    ));
    assert!(
        load_shared_access_authentication(DeploymentMode::Development, false, "tenant", None, None)
            .expect("policy-free development")
            .is_none()
    );
    let invalid = cluster::ClusterConfig {
        mode: DeploymentMode::Production,
        voters: 2,
    };
    assert!(matches!(
        server::open(invalid, StorageChoice::Memory),
        Err(StartupError::Cluster(_))
    ));
    assert_eq!(
        server::open(
            cluster::ClusterConfig {
                mode: DeploymentMode::Production,
                voters: 3
            },
            StorageChoice::Memory
        )
        .err(),
        Some(StartupError::MemoryStorageInProduction)
    );
    assert_eq!(
        server::open(
            cluster::ClusterConfig {
                mode: DeploymentMode::Production,
                voters: 3
            },
            StorageChoice::Durable {
                directory: PathBuf::from("unused-production-store")
            }
        )
        .err(),
        Some(StartupError::ReplicationUnavailableInProduction)
    );
}
