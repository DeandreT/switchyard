use super::fixture::*;
use super::*;

#[tokio::test]
async fn cancellation_releases_wire_gates_before_original_session_and_socket_joins() {
    let mut fixture = Fixture::new(authorization(auth::PermissionSet::SEND, false)).await;
    fixture.start_loop();
    let observed = caught(async {
        fixture.gate.arm();
        fixture
            .send_attach(&request("held-publication", 17, Role::Sender, "orders"))
            .await;
        fixture.gate.entered().await;
        // shutdown is only a cancellation request. finish below retains and joins
        // the actual session parent, Actor, Reader and acceptance parent results.
        fixture.stop();
    })
    .await;
    fixture.gate.release();
    fixture.finish().await;
    fixture.joined();
    fixture.no_broker_effects();
    rethrow(observed);
}

#[tokio::test]
async fn peer_disconnect_retains_worker_and_socket_results_until_cleanup() {
    let mut fixture = Fixture::new(authorization(auth::PermissionSet::SEND, false)).await;
    let observed = caught(async {
        fixture.cbs_grant().await;
        fixture.authorized_sender(21).await;
        bounded(tokio::io::AsyncWriteExt::shutdown(&mut fixture.peer))
            .await
            .expect("actual peer disconnect");
    })
    .await;
    fixture.finish().await;
    fixture.joined();
    assert_eq!(fixture.worker_results.len(), 3);
    assert!(
        fixture
            .report
            .as_ref()
            .expect("retained report")
            .actor()
            .is_some()
    );
    assert!(
        fixture
            .report
            .as_ref()
            .expect("retained report")
            .reader()
            .is_some()
    );
    rethrow(observed);
}

#[tokio::test]
async fn observation_or_worker_panic_keeps_original_payload_through_cleanup() {
    for worker_panic in [false, true] {
        let mut fixture = Fixture::new(authorization(auth::PermissionSet::SEND, false)).await;
        let observed = caught(async {
            if worker_panic {
                fixture.cbs_grant().await;
                fixture.authorized_sender(21).await;
                fixture
                    .broker
                    .panic_submit
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                fixture.transfer(21, 1, amqp::Message::data("panic")).await;
                bounded(fixture.broker.submitted.notified()).await;
            } else {
                std::panic::panic_any(String::from("original observation panic"));
            }
        })
        .await;
        fixture.finish().await;
        fixture.joined();
        if worker_panic {
            rethrow(observed);
            let result = fixture
                .worker_results
                .pop()
                .expect("original data worker result");
            let error = result.expect_err("original worker panic");
            assert!(error.is_panic());
            let payload = error.into_panic();
            assert_eq!(
                payload.downcast_ref::<String>().map(String::as_str),
                Some("original counted broker panic")
            );
        } else {
            let payload = observed.expect_err("original observation panic");
            assert_eq!(
                payload.downcast_ref::<String>().map(String::as_str),
                Some("original observation panic")
            );
        }
    }
}
