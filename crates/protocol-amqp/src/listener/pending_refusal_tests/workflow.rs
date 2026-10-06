use super::fixture::*;
use super::*;

#[tokio::test]
async fn same_session_cbs_grant_survives_prior_pending_denial() {
    let mut fixture = Fixture::new(authorization(auth::PermissionSet::SEND, false)).await;
    let observed = caught(async {
        fixture
            .deny_direct(&request("before-grant", 17, Role::Sender, "orders"))
            .await;
        fixture.no_broker_effects();
        fixture.cbs_grant().await;
        let local_handle = fixture.authorized_sender(21).await;
        fixture.accepted_send(21, local_handle).await;
        fixture
            .deny_direct(&request("listen-denied", 22, Role::Receiver, "orders"))
            .await;
        assert_eq!(
            fixture
                .broker
                .binds
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let commands = fixture.broker.commands.lock().expect("commands");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].0.namespace(), &namespace());
        assert_eq!(commands[0].0.target().as_str(), "orders");
        assert!(matches!(&commands[0].1, CommandKind::SendEnvelope { .. }));
    })
    .await;
    fixture.finish().await;
    fixture.joined();
    assert_eq!(fixture.worker_results.len(), 3);
    rethrow(observed);
}

#[tokio::test]
async fn listen_grant_keeps_sender_denial_and_authorized_link_isolation() {
    let mut fixture = Fixture::new(authorization(auth::PermissionSet::LISTEN, false)).await;
    let observed = caught(async {
        fixture
            .deny_direct(&request("before-grant", 17, Role::Receiver, "orders"))
            .await;
        fixture.cbs_grant().await;
        let mut sender = fixture.authorized_listen(21).await;
        fixture
            .deny_direct(&request("sender-denied", 22, Role::Sender, "orders"))
            .await;
        fixture
            .write(amqp::Performative::Detach(amqp::Detach {
                handle: 21,
                closed: true,
                error: None,
            }))
            .await;
        bounded(sender.on_detach()).await;
        bounded(sender.close())
            .await
            .expect("authorized link close");
        assert!(matches!(
            fixture.read().await,
            amqp::Frame::Amqp {
                performative: Some(amqp::Performative::Detach(amqp::Detach {
                    closed: true,
                    error: None,
                    ..
                })),
                ..
            }
        ));
        assert_eq!(
            fixture
                .broker
                .binds
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(
            fixture
                .broker
                .commands
                .lock()
                .expect("no fake receive")
                .is_empty()
        );
    })
    .await;
    fixture.finish().await;
    fixture.joined();
    assert_eq!(fixture.worker_results.len(), 2);
    rethrow(observed);
}
