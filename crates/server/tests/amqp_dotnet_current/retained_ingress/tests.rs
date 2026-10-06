use std::{future::pending, sync::atomic::Ordering};

use futures_util::FutureExt;

use super::*;

#[tokio::test]
async fn retained_sdk_controller_owns_memory_only_anchors() -> TestResult {
    let mut fixture = fixture::Fixture::start(false).await?;
    let observation = std::panic::AssertUnwindSafe(async {
        let peer = fixture.connect_peer().await?;
        fixture.controller.observe_accept(false).await;
        fixture.controller.install_accepted();
        drop(peer);
        Ok::<_, Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    fixture.finish().await;
    match observation {
        Ok(result) => result?,
        Err(payload) => std::panic::resume_unwind(payload),
    }
    assert_eq!(fixture.controller.lifetime, 1);
    assert_eq!(fixture.controller.report_count(), 1);
    let report = fixture.controller.reports[0]
        .as_ref()
        .expect("original root report");
    assert_eq!(report.anchor().ordinal, 0);
    assert!(report.socket().wrapper().is_some());
    assert_eq!(fixture.controller.permits.available_permits(), 3);
    assert_eq!(fixture.controller.anchor_drops.load(Ordering::SeqCst), 0);
    let same_process_clone = fixture.store.clone();
    assert_eq!(same_process_clone.snapshot()?, fixture.store.snapshot()?);
    let drops = fixture.controller.anchor_drops.clone();
    drop(fixture);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn accepted_socket_ready_survives_observer_cancellation() -> TestResult {
    let mut fixture = fixture::Fixture::start(false).await?;
    let observation = std::panic::AssertUnwindSafe(async {
        let peer = fixture.connect_peer().await?;
        let ready = fixture.controller.accept_ready.clone();
        {
            let observer = fixture.controller.observe_accept(true);
            tokio::pin!(observer);
            tokio::select! {
                () = &mut observer => unreachable!("test Ready hold does not return"),
                () = async {
                    while !ready.load(Ordering::SeqCst) { tokio::task::yield_now().await; }
                } => {},
            }
        }
        let retained_ready = fixture
            .controller
            .accepted
            .as_ref()
            .is_some_and(Result::is_ok);
        fixture.controller.install_accepted();
        drop(peer);
        Ok::<_, Box<dyn Error>>(retained_ready)
    })
    .catch_unwind()
    .await;
    fixture.finish().await;
    let retained_ready = match observation {
        Ok(result) => result?,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    assert!(retained_ready);
    assert!(fixture.controller.accepted.is_none());
    assert_eq!(fixture.controller.report_count(), 1);
    assert!(
        fixture.controller.reports[0]
            .as_ref()
            .expect("original report")
            .socket()
            .wrapper()
            .is_some()
    );
    assert_eq!(fixture.controller.anchor_drops.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn connection_history_exhaustion_retains_prior_roots_and_permits() -> TestResult {
    let mut fixture = fixture::Fixture::start(false).await?;
    let observation = std::panic::AssertUnwindSafe(async {
        for _ in 0..=fixture::CONNECTION_HISTORY {
            let peer = fixture.connect_peer().await?;
            fixture.controller.observe_accept(false).await;
            fixture.controller.install_accepted();
            drop(peer);
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    fixture.finish().await;
    match observation {
        Ok(result) => result?,
        Err(payload) => std::panic::resume_unwind(payload),
    }
    assert_eq!(fixture.controller.lifetime, fixture::CONNECTION_HISTORY);
    assert_eq!(
        fixture.controller.report_count(),
        fixture::CONNECTION_HISTORY
    );
    assert!(!fixture.controller.accepting());
    assert!(fixture.controller.overflow.is_some());
    assert_eq!(fixture.controller.permits.available_permits(), 0);
    assert_eq!(fixture.controller.anchor_drops.load(Ordering::SeqCst), 0);
    for (ordinal, report) in fixture.controller.reports.iter().enumerate() {
        let report = report
            .as_ref()
            .expect("each original holder remains retained");
        assert_eq!(report.anchor().ordinal, ordinal);
        assert!(report.socket().wrapper().is_some());
    }
    let drops = fixture.controller.anchor_drops.clone();
    drop(fixture);
    assert_eq!(drops.load(Ordering::SeqCst), fixture::CONNECTION_HISTORY);
    Ok(())
}

#[tokio::test]
async fn client_failure_is_propagated_after_all_root_reports() -> TestResult {
    let mut fixture = fixture::Fixture::start(false).await?;
    let setup = std::panic::AssertUnwindSafe(async {
        for _ in 0..2 {
            let peer = fixture.connect_peer().await?;
            fixture.controller.observe_accept(false).await;
            fixture.controller.install_accepted();
            drop(peer);
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    let client = async {
        Err::<process::Output, Box<dyn Error>>(Box::new(std::io::Error::other(
            "retained-original-client-failure",
        )))
    };
    tokio::pin!(client);
    let observation = std::panic::AssertUnwindSafe(fixture.drive_client(client.as_mut()))
        .catch_unwind()
        .await;
    fixture.finish_with_client(client.as_mut()).await;
    match setup {
        Ok(result) => result?,
        Err(payload) => std::panic::resume_unwind(payload),
    }
    if let Err(payload) = observation {
        std::panic::resume_unwind(payload);
    }

    let cancelled_task = tokio::spawn(pending::<()>());
    let abort_requested = true;
    cancelled_task.abort();
    let cancelled = cancelled_task
        .await
        .expect_err("original cancelled task result");
    let panicked = tokio::spawn(async {
        panic!("retained-original-test-panic");
    })
    .await
    .expect_err("original panic task result");
    let original_routing_error = std::io::Error::other("retained-original-routing-failure");

    assert_eq!(fixture.controller.report_count(), 2);
    assert_eq!(fixture.controller.anchor_drops.load(Ordering::SeqCst), 0);
    assert!(completed_gate_failed(&fixture));
    let failure = GateFailure {
        fixture: Box::new(fixture),
    };
    assert_eq!(
        failure.source().expect("original client error").to_string(),
        "retained-original-client-failure"
    );
    assert!(
        failure
            .source()
            .expect("typed original client error")
            .downcast_ref::<std::io::Error>()
            .is_some()
    );
    assert!(expected_test_cancellation(
        &cancelled,
        abort_requested,
        RetainedAtomicMessagingDrain::External
    ));
    assert!(!expected_test_cancellation(
        &cancelled,
        false,
        RetainedAtomicMessagingDrain::External
    ));
    assert!(!expected_test_cancellation(
        &cancelled,
        true,
        RetainedAtomicMessagingDrain::Live
    ));
    assert!(!expected_test_cancellation(
        &panicked,
        true,
        RetainedAtomicMessagingDrain::External
    ));
    assert!(unexpected_role_failure(
        Some(&cancelled),
        Some(&original_routing_error),
        true,
        RetainedAtomicMessagingDrain::External,
    ));
    assert_eq!(
        original_routing_error.to_string(),
        "retained-original-routing-failure"
    );
    assert!(
        failure
            .fixture
            .controller
            .reports
            .iter()
            .flatten()
            .all(|report| report.socket().wrapper().is_some())
    );

    let mut panic_fixture = fixture::Fixture::start(false).await?;
    let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let original_polls = polls.clone();
    let panic_client =
        std::future::poll_fn(move |_| -> std::task::Poll<TestResult<process::Output>> {
            original_polls.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(String::from("retained-original-client-panic"));
        });
    tokio::pin!(panic_client);
    let observation = std::panic::AssertUnwindSafe(async {
        let peer = panic_fixture.connect_peer().await?;
        panic_fixture.controller.observe_accept(false).await;
        panic_fixture.controller.install_accepted();
        drop(peer);
        panic_fixture.drive_client(panic_client.as_mut()).await;
        Ok::<_, Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    panic_fixture
        .finish_with_client(panic_client.as_mut())
        .await;
    match observation {
        Ok(result) => result?,
        Err(payload) => std::panic::resume_unwind(payload),
    }
    assert_eq!(panic_fixture.controller.report_count(), 1);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "terminal original future is not repolled"
    );
    assert!(panic_fixture.client.is_none());
    assert_eq!(
        panic_fixture
            .client_panic
            .as_ref()
            .and_then(|payload| payload.downcast_ref::<String>())
            .map(String::as_str),
        Some("retained-original-client-panic")
    );
    Ok(())
}

#[test]
fn success_marker_must_be_exact_completed_line() {
    assert!(success_marker_present(&format!("{SUCCESS}\n")));
    assert!(success_marker_present(&format!(
        "diagnostic\r\n{SUCCESS}\r\n"
    )));
    for stdout in [
        String::new(),
        SUCCESS.to_owned(),
        format!("{SUCCESS}\r"),
        format!("prefix {SUCCESS}\n"),
        format!("{SUCCESS} suffix\n"),
        format!(" {SUCCESS}\n"),
        format!("{SUCCESS}\n{SUCCESS}\n"),
        format!("{}\n", &SUCCESS[..SUCCESS.len() - 1]),
    ] {
        assert!(!success_marker_present(&stdout));
    }
}

#[test]
fn retained_client_arguments_preserve_trust_and_two_cpu_policy() {
    let dll = Path::new("client.dll");
    let ca = Path::new("trusted-ca.pem");
    let directory = Path::new("empty-ca-directory");
    let command =
        process::retained_client_command(dll, "sb://localhost:12345", QUEUE, ca, directory);
    let command = command.as_std();
    let arguments = command.get_args().collect::<Vec<_>>();
    assert_eq!(
        arguments,
        [
            std::ffi::OsStr::new("client.dll"),
            std::ffi::OsStr::new("retained-ingress"),
            std::ffi::OsStr::new(HOST),
            std::ffi::OsStr::new("sb://localhost:12345"),
            std::ffi::OsStr::new(QUEUE),
            std::ffi::OsStr::new(RULE),
            std::ffi::OsStr::new(KEY),
        ]
    );
    let environment = command
        .get_envs()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        environment.get(std::ffi::OsStr::new("DOTNET_PROCESSOR_COUNT")),
        Some(&Some(std::ffi::OsStr::new("2")))
    );
    assert_eq!(
        environment.get(std::ffi::OsStr::new("SSL_CERT_FILE")),
        Some(&Some(ca.as_os_str()))
    );
    assert_eq!(
        environment.get(std::ffi::OsStr::new("SSL_CERT_DIR")),
        Some(&Some(directory.as_os_str()))
    );
    assert_eq!(CURRENT_SDK, "7.21.0");
}
