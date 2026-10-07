//! Actual binary probes for the explicit private-CA Atom CLI profile.

use std::{error::Error, ffi::OsString, fs, net::TcpListener};

use domain::{
    EntityPath, FiniteQueueCapacity, NamespaceName, QueueCapacityCommandV1, QueueConfig,
    StateMachine,
};
use hyper::{Method, StatusCode};
use storage::{FjallStore, MemoryStore, StateStore};

use super::atomic_messaging::process;

#[path = "atom_admin_cli/fixture.rs"]
mod fixture;

use fixture::{
    ATOM_KEY, ATOM_RULE, COLLECTION, CREATE, ENTITY, Files, LEGACY_KEY, LEGACY_RULE, token,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn require_success(output: &process::Output) {
    assert!(output.status.success());
    assert!(output.stdout.is_empty() && output.stderr.is_empty());
}

fn require_refusal(output: &process::Output, description: &str) {
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        output.stderr == format!("switchyard: {description}\n"),
        "preflight refusal was not the exact static diagnostic"
    );
    for secret in [ATOM_KEY, LEGACY_KEY, "sentinel-private-path"] {
        assert!(!output.stderr.contains(secret));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_disabled_atom_check_config_preserves_plaintext_and_old_refusals() -> TestResult {
    let files = Files::new()?;
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?.to_string();
    let mut arguments: Vec<OsString> = vec![
        "--storage".into(),
        "fjall".into(),
        "--data-dir".into(),
        files.store.as_os_str().to_owned(),
    ];
    for flag in [
        "--listen",
        "--admin-listen",
        "--websocket-listen",
        "--experimental-atomic-messaging-listen",
    ] {
        arguments.extend([flag.into(), address.clone().into()]);
    }
    require_success(&process::run_switchyard_check_config(&arguments).await?);
    files.unopened_storage();
    require_refusal(
        &process::run_switchyard_check_config(&["--mode".into(), "production".into()]).await?,
        "production mode requires a TLS certificate and private key",
    );
    files.unopened_storage();
    assert_eq!(occupied.local_addr()?.to_string(), address);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_atom_check_config_loads_both_policies_without_bind_or_storage() -> TestResult {
    let files = Files::new()?;
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?;
    let mut arguments = files.arguments(address, &files.atom_key);
    for flag in [
        "--listen",
        "--admin-listen",
        "--websocket-listen",
        "--experimental-atomic-messaging-listen",
    ] {
        arguments.extend([flag.into(), address.to_string().into()]);
    }
    require_success(&process::run_switchyard_check_config(&arguments).await?);
    files.unopened_storage();
    assert_eq!(occupied.local_addr()?, address);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_atom_fifo_and_symlink_are_bounded_regular_file_refusals() -> TestResult {
    use rustix::fs::{CWD, Mode, mkfifoat};
    use std::os::unix::fs::symlink;

    let files = Files::new()?;
    let fifo = files.directory.path().join("sentinel-private-path-fifo");
    let link = files.directory.path().join("sentinel-private-path-link");
    mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR)?;
    symlink(&fifo, &link)?;
    for path in [&fifo, &link] {
        let arguments = files.arguments(([127, 0, 0, 1], 0).into(), path);
        require_refusal(
            &process::run_switchyard_check_config(&arguments).await?,
            "the Atom administration key must be a regular file",
        );
        files.unopened_storage();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_atom_strict_tls_and_credential_refusals_share_one_healthy_child() -> TestResult {
    let files = Files::new()?;
    let (arguments, address) = files.live_arguments()?;
    let sensitive = files.sensitive();
    process::with_atom_cli_child(&arguments, &sensitive, async {
        files.ready(address).await?;
        let manage = token("https://localhost", ATOM_RULE, ATOM_KEY)?;
        let (status, created) = files
            .exchange(address, Method::PUT, ENTITY, &manage, CREATE)
            .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert!(!created.is_empty());
        files.assert_tls_refusals(address).await?;
        // The CLI has only Manage; an unconfigured SEND-labelled rule is a 401, not a permission-grant proof.
        for credential in [
            token("https://localhost", "unconfigured-send", ATOM_KEY)?,
            token("https://localhost", LEGACY_RULE, LEGACY_KEY)?,
            token(
                "amqps://localhost.servicebus.windows.net",
                LEGACY_RULE,
                LEGACY_KEY,
            )?,
            token(
                "https://localhost.servicebus.windows.net",
                ATOM_RULE,
                ATOM_KEY,
            )?,
        ] {
            let (status, body) = files
                .exchange(
                    address,
                    Method::PUT,
                    "/unauthorized?api-version=2024-05",
                    &credential,
                    CREATE,
                )
                .await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            let body = String::from_utf8(body)?;
            assert!(!body.contains(ATOM_KEY) && !body.contains(LEGACY_KEY));
        }
        let (status, fetched) = files
            .exchange(address, Method::GET, ENTITY, &manage, b"")
            .await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "the same strict healthy child remains usable after negatives"
        );
        assert_eq!(fetched, created);
        let (status, listed) = files
            .exchange(address, Method::GET, COLLECTION, &manage, b"")
            .await?;
        assert_eq!(status, StatusCode::OK);
        let listed = String::from_utf8(listed)?;
        assert_eq!(listed.matches("<entry").count(), 1);
        assert!(listed.contains("orders") && !listed.contains("$deadletterqueue"));
        Ok(())
    })
    .await?;
    let store = process::atom_cli_at("atom-durable-reopen", FjallStore::open(&files.store))?;
    let machine = StateMachine::new(store.clone());
    let namespace = NamespaceName::new("localhost")?;
    let entity = EntityPath::new("orders")?;
    let config = QueueConfig {
        lock_duration_millis: 15_000,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(45_000),
        dead_lettering_on_message_expiration: true,
        requires_session: false,
        max_message_bytes: 4 * 1024,
        requires_duplicate_detection: false,
        duplicate_detection_history_time_window_millis: 60_000,
    };
    assert_eq!(machine.queue_config(&namespace, &entity)?, Some(config));
    let expected = StateMachine::new(MemoryStore::default());
    expected.apply_queue_capacity(&QueueCapacityCommandV1::CreateFinite {
        namespace,
        entity,
        issued_at: machine.last_applied_time()?,
        config,
        limit: FiniteQueueCapacity::new(2 * 1024 * 1024)?,
    })?;
    assert_eq!(
        store.snapshot()?,
        expected.store().snapshot()?,
        "after original child/group/pipe cleanup, the entire reopened image is exactly one finite create"
    );
    assert!(fs::metadata(&files.store)?.is_dir());
    Ok(())
}
